package storage

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"sort"
	"storage/index"
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

// Close closes all the log files.
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
// Update the vlog.activeLogFile.WriteAt, if active file's capacity is nearly close to block size, open a new active file.
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

/*
handle vlog compaction part.
*/

func (vlog *valueLog) getLogFile(fid uint32) *logfile.LogFile {
	vlog.Lock()
	defer vlog.Unlock()
	return vlog.logFiles[fid]
}

// handleCompaction implements a periodic task that cyclically checks
// and compacts a value log. It sets up a signal channel to receive interrupt
// and other signals from the operating system, and a timer to periodically
// trigger the compaction. It listens to the timer, signal channel,and a closed
// channel, and exits the loop when the closed channel is closed or a signal is received.
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

// compact is used for compacting the log files. In this db, log files are used to record
// data mutation operations so that the correct state can be restored in subsequent queries. When
// log files become too large, they need to be compacted to avoid excessive disk space usage.
func (vlog *valueLog) compact() error {
	// Get the current active log file ID and use the gcRatio to obtain a list of log file IDs to compact.
	activeFid := vlog.getActiveFid()
	ccl, err := vlog.discard.getCCL(activeFid, vlog.opt.gcRatio)
	if err != nil {
		return err
	}

	rewrite := func(file *logfile.LogFile) error {
		vlog.cf.flushLock.Lock()
		defer vlog.cf.flushLock.Unlock()

		var offset int64
		var validEntries []*logfile.LogEntry
		ts := time.Now().Unix()
		for {
			entry, sz, err := file.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF || err == logfile.ErrEndOfEntry {
					break
				}
				return err
			}
			eoff := offset
			offset += sz
			// get the corresponding index of the vlog's entry.
			indexMeta, err := vlog.cf.indexer.Get(entry.Key)
			if err != nil {
				return err
			}
			// If the index metadata is missing or the log entry's
			// value has already been indexed(the value in vlog must be old.), skip it.
			if indexMeta == nil || len(indexMeta.Value) != 0 {
				continue
			}
			if indexMeta.Fid == file.Fid && indexMeta.Offset == eoff {
				validEntries = append(validEntries, entry)
			}
		}
		// Create new index nodes and deleted keys lists as we rewrite valid log entries.
		var nodes []*index.IndexerNode
		var deletedKeys [][]byte

		for _, e := range validEntries {
			if e.ExpiredAt != 0 && e.ExpiredAt <= ts {
				// If the log entry has expired, add its key to the deleted keys list.
				deletedKeys = append(deletedKeys, e.Key)
				continue
			}
			// Write the log entry to the new log file and add the corresponding index metadata to the index nodes list.
			valuePos, esize, err := vlog.Write(e)
			if err != nil {
				return err
			}
			nodes = append(nodes, &index.IndexerNode{
				Key: e.Key,
				Meta: &index.IndexerMeta{
					Fid:       valuePos.Fid,
					Offset:    valuePos.Offset,
					EntrySize: esize,
				},
			})
		}
		// Write the new index nodes and delete the expired log entries' keys.
		writeOpts := index.WriteOptions{SendDiscard: false}
		if _, err := vlog.cf.indexer.PutBatch(nodes, writeOpts); err != nil {
			return err
		}
		if len(deletedKeys) > 0 {
			if err := vlog.cf.indexer.DeleteBatch(deletedKeys, writeOpts); err != nil {
				return err
			}
		}
		if err := vlog.cf.indexer.Sync(); err != nil {
			return err
		}
		return nil
	}

	// For each log file to compact, open the file and iterate over each log entry.
	for _, fid := range ccl {
		lf := vlog.getLogFile(fid)
		if lf == nil || lf.IOSelector == nil {
			// If the file is not open, open it.
			file, err := logfile.OpenLogFile(vlog.opt.path, fid, vlog.opt.blockSize, logfile.ValueLog, vlog.opt.ioType)
			if err != nil {
				return err
			}
			lf = file
		}

		// Rewrite each valid log entry.
		if err = rewrite(lf); err != nil {
			logger.Warnf("compact rewrite err: %+v", err)
			return err
		}

		// Clear the discard state and delete the old log file.
		vlog.discard.clear(fid)
		vlog.Lock()
		if _, ok := vlog.logFiles[fid]; ok {
			delete(vlog.logFiles, fid)
		}
		vlog.Unlock()
		if err = lf.Delete(); err != nil {
			return err
		}
	}
	return nil
}
