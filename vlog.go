package storage

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"sort"
	"storage/logfile"
	"storage/logger"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrActiveLogFileNil active log file not exists.
	ErrActiveLogFileNil = errors.New("active log file not exists")

	// ErrLogFileNil log file not exists.
	ErrLogFileNil = errors.New("log file %d not exists")
)

const vlogDiscardName = "VLOG_DISCARD"

// valueLog value log is named after the concept in Wisckey paper
// https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf.
// Values will be stored in value log if its size exceed ValueThreshold in options.
type valueLog struct {
	sync.RWMutex
	opt vlogOptions
	// current active log file for writing.
	activeLogFile *logfile.LogFile
	// all log files. Must hold the mutex before modify it.
	logFiles map[uint32]*logfile.LogFile
	cf       *ColumnFamily
	discard  *discard
}

type vlogOptions struct {
	path       string
	blockSize  int64 // size of each vlog's file.
	ioType     logfile.IOType
	gcRatio    float64
	gcInterval time.Duration
}

// valuePos value position.
type valuePos struct {
	Fid    uint32
	Offset int64
}

// openValueLog create a new value log file.
func openValueLog(opt vlogOptions) (*valueLog, error) {
	fileInfos, err := ioutil.ReadDir(opt.path)
	if err != nil {
		return nil, err
	}
	var fids []uint32
	for _, file := range fileInfos {
		if strings.HasSuffix(file.Name(), logfile.VLogSuffixName) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return nil, err
			}
			fids = append(fids, uint32(fid))
		}
	}

	// load in order.
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	if len(fids) == 0 {
		fids = append(fids, logfile.InitialLogFileId)
	}

	// open discard file.
	discard, err := newDiscard(opt.path, vlogDiscardName)
	if err != nil {
		return nil, err
	}

	// open active log file only.
	logFile, err := logfile.OpenLogFile(opt.path, fids[len(fids)-1], opt.blockSize, logfile.ValueLog, opt.ioType)
	// set total size in discard, skip is if already exist.
	discard.setTotal(fids[len(fids)-1], uint32(opt.blockSize))
	if err != nil {
		return nil, err
	}

	vlog := &valueLog{
		opt:           opt,
		activeLogFile: logFile,
		logFiles:      make(map[uint32]*logfile.LogFile),
		discard:       discard,
	}

	// load other log file(don't open).
	for i := 0; i < len(fids)-1; i++ {
		vlog.logFiles[fids[i]] = &logfile.LogFile{Fid: fids[i]}
	}

	if err := vlog.setLogFileState(); err != nil {
		return nil, err
	}

	go vlog.handleCompaction()

	return vlog, nil
}

// Read a VLogEntry from a specified vlog file at offset, returns an error if any.
// If reading from a non-active log file, and the specified file is not open,
// then we will open it and set it into logFiles.
func (vlog *valueLog) Read(fid uint32, offset int64) (*logfile.LogEntry, error) {
	var logFile *logfile.LogFile
	if fid == vlog.activeLogFile.Fid {
		logFile = vlog.activeLogFile
	} else {
		vlog.RLock()
		logFile = vlog.logFiles[fid]
		if logFile != nil && logFile.IOSelector == nil { // haven't initiate(don't open).
			opt := vlog.opt
			lf, err := logfile.OpenLogFile(opt.path, fid, opt.blockSize, logfile.ValueLog, opt.ioType)
			if err != nil {
				vlog.RUnlock()
				return nil, err
			}
			vlog.logFiles[fid] = lf
			logFile = lf
		}
		vlog.RUnlock()
	}
	if logFile == nil {
		return nil, fmt.Errorf(ErrLogFileNil.Error(), fid)
	}

	entry, _, err := logFile.ReadLogEntry(offset)
	if err == logfile.ErrEndOfEntry {
		return &logfile.LogEntry{}, nil
	}
	return entry, err
}

// Write new VLogEntry to value log.
// If the active log file is full, it will be closed and a new active file will be created to replace it.
func (vlog *valueLog) Write(ent *logfile.LogEntry) (*valuePos, int, error) {
	vlog.Lock()
	defer vlog.Unlock()

	buf, eSize := logfile.EncodeEntry(ent)
	// if active Log reach the threshold, close it and open a new one.
	if vlog.activeLogFile.WriteAt+int64(eSize) >= vlog.opt.blockSize {
		if err := vlog.Sync(); err != nil {
			return nil, 0, err
		}
		vlog.logFiles[vlog.activeLogFile.Fid] = vlog.activeLogFile

		logfile, err := vlog.createLogFile()
		if err != nil {
			return nil, 0, err
		}
		vlog.activeLogFile = logfile
	}

	err := vlog.activeLogFile.Write(buf)
	if err != nil {
		return nil, 0, err
	}

	writeAt := atomic.LoadInt64(&vlog.activeLogFile.WriteAt)
	return &valuePos{
		Fid:    vlog.activeLogFile.Fid,
		Offset: writeAt - int64(eSize),
	}, eSize, nil
}

// Sync only for the active log file.
func (vlog *valueLog) Sync() error {
	if vlog.activeLogFile == nil {
		return ErrActiveLogFileNil
	}

	vlog.activeLogFile.Lock()
	defer vlog.activeLogFile.Unlock()

	return vlog.activeLogFile.Sync()
}

// Close only for the active log file.
func (vlog *valueLog) Close() error {
	// close discard channel.
	vlog.discard.closeChan()

	var vlogErr error
	// close active log file
	if err := vlog.activeLogFile.Close(); err != nil {
		vlogErr = err
	}
	// close archived log files.
	for _, lf := range vlog.logFiles {
		if err := lf.Close(); err != nil {
			vlogErr = err
		}
	}
	return vlogErr
}

// createLogFile create a new logfile for active log, and config its total size in discard.
func (vlog *valueLog) createLogFile() (*logfile.LogFile, error) {
	opt := vlog.opt
	fid := vlog.activeLogFile.Fid
	logFile, err := logfile.OpenLogFile(opt.path, fid+1, opt.blockSize, logfile.ValueLog, opt.ioType)
	if err != nil {
		return nil, err
	}
	vlog.discard.setTotal(fid+1, uint32(opt.blockSize))
	return logFile, nil
}

// setLogFileState initiate the writeAt and current active log for given vlog.
func (vlog *valueLog) setLogFileState() error {
	if vlog.activeLogFile == nil {
		return ErrActiveLogFileNil
	}
	var offset int64 = 0
	for {
		if _, size, err := vlog.activeLogFile.ReadLogEntry(offset); err == nil {
			offset += size
			// No need to use atomic updates.
			// This function is only be executed in one goroutine at startup.
			vlog.activeLogFile.WriteAt += size
		} else {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return err
		}
	}
	// if active file's capacity is nearly close to block size, open a new active file.
	if vlog.activeLogFile.WriteAt+logfile.MaxHeaderSize >= vlog.opt.blockSize {
		vlog.logFiles[vlog.activeLogFile.Fid] = vlog.activeLogFile
		logFile, err := vlog.createLogFile()
		if err != nil {
			return err
		}
		vlog.activeLogFile = logFile
	}
	return nil
}

// getActiveFid get the current active log fid.
func (vlog *valueLog) getActiveFid() uint32 {
	vlog.Lock()

	var fid uint32
	if vlog.activeLogFile != nil {
		fid = vlog.activeLogFile.Fid
	}
	vlog.Unlock()
	return fid
}

// getLogFile get the LogFile in vlog.logFiles(used for CLL);
func (vlog *valueLog) getLogFile(fid uint32) *logfile.LogFile {
	vlog.Lock()
	defer vlog.Unlock()
	return vlog.logFiles[fid]
}

func (vlog *valueLog) handleCompaction() {
	if vlog.opt.gcInterval <= 0 {
		return
	}
	// signal channel with buf size equal to 1.
	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	ticker := time.NewTicker(vlog.opt.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// performing cyclic operations
			if err := vlog.compact(); err != nil {
				logger.Errorf("value log compaction err: %+v", err)
			}
		case <-quitSig:
		case <-vlog.cf.closedC:
			return
		}
	}
}
