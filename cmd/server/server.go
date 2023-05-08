package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"storage"
	"storage/logger"
	"strings"
	"syscall"
	"time"
)

const (
	// PUT put command.
	PUT = "PUT"
	// GET get command.
	GET = "GET"
	// DELETE delete command.
	DELETE = "DELETE"
)

var db *storage.DB

var (
	// ErrArgsNumNotMatch number of args not match.
	ErrArgsNumNotMatch = errors.New("the number of args not match")
	// ErrNotSupportedCmd command not be supported.
	ErrNotSupportedCmd = errors.New("the command not be supported")
)

// init initializes a storage engine instance with default options.
func init() {
	options := storage.DefaultOptions("/tmp/db")
	var err error
	db, err = storage.Open(options)
	if err != nil {
		panic(err)
	}
}

func main() {
	// print banner information.
	banner()

	// listen for incoming connections.
	listener, err := net.Listen("tcp", "127.0.0.1:8888")
	if err != nil {
		panic(err)
	}

	// log server start message.
	logger.Info("db server is running")

	// handle OS signals.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		// handle signal for graceful shutdown.
		<-sig
		os.Exit(-1)
	}()

	for {
		// accept incoming connections.
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		// read command from client.
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			log.Println(err)
			continue
		}

		// split command into parts and execute it.
		cmds := strings.Split(string(buf[:n]), " ")
		var response []byte
		start := time.Now()
		response, err = ExecuteCmds(cmds)
		if err != nil {
			response = []byte("ERROR:" + err.Error())
		} else {
			response = []byte(fmt.Sprintf("%s (%s)", string(response), time.Since(start)))
		}

		// send response to client.
		conn.Write(response)
	}
}

// banner prints banner information.
func banner() {
	b, err := ioutil.ReadFile("cmd/server/banner.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
	fmt.Println("A powerful kv storage engine based on SLM-DB in USENIX FAST '19.")
}

// ExecuteCmds execute command and get result.
func ExecuteCmds(cmds []string) (result []byte, err error) {
	// execute command and return result.
	switch strings.ToUpper(cmds[0]) {
	case PUT:
		if err = check(cmds[1:], 2); err != nil {
			return
		}
		err = db.Put([]byte(cmds[1]), []byte(cmds[2]))
	case GET:
		if err = check(cmds[1:], 1); err != nil {
			return
		}
		result, err = db.Get([]byte(cmds[1]))
		if result == nil {
			result = []byte("empty")
		}
	case DELETE:
		if err = check(cmds[1:], 1); err != nil {
			return
		}
		err = db.Delete([]byte(cmds[1]))
	default:
		err = ErrNotSupportedCmd
	}
	return
}

// check checks if number of arguments is correct.
func check(args []string, num int) error {
	if len(args) != num {
		return ErrArgsNumNotMatch
	}
	return nil
}
