package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/peterh/liner"
)

const cmdHistoryPath = "/tmp/db-cli"

// all supported commands with their syntax and type.
var commandList = [][]string{
	{"PUT", "key value", "STRING"},
	{"GET", "key", "STRING"},
	{"DELETE", "key", "STRING"},
	{"HELP", "cmd", "STRING"},
}

// commands help list
var helpList = map[string]string{
	"PUT":    "PUT key value  summary: Set the string value of a key",
	"GET":    "GET key        summary: Get the value of a key",
	"DELETE": "DELETE key     summary: Delete the key",
	"HELP":   "HELP           summary: To get help about DB client commands",
}

var commandSet map[string]bool

var (
	// ErrCmdNotFound the command not found error.
	ErrCmdNotFound = errors.New("the command not found")
)

// init() initializes the command set with all supported commands.
func init() {
	commandSet = make(map[string]bool)
	for _, cmd := range commandList {
		commandSet[strings.ToLower(cmd[0])] = true
	}
}

func main() {
	// initialize the liner object for command line interaction.
	line := liner.NewLiner()
	defer line.Close()

	// set Ctrl+C as abort signal.
	line.SetCtrlCAborts(true)

	// set command auto completion.
	line.SetCompleter(func(line string) (c []string) {
		for _, cmd := range commandList {
			if strings.HasPrefix(cmd[0], strings.ToUpper(line)) {
				c = append(c, strings.ToUpper(cmd[0]))
			}
		}
		return
	})

	// load command history from file.
	if f, err := os.Open(cmdHistoryPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	// save command history to file when program exits.
	defer func() {
		if f, err := os.Create(cmdHistoryPath); err != nil {
			fmt.Println("Error writing history file: ", err)
		} else {
			line.WriteHistory(f)
			f.Close()
		}
	}()

	prompt := "127.0.0.1:8888>"
	for {
		// read command from user input.
		if cmd, err := line.Prompt(prompt); err == nil {
			// handle empty command.
			if cmd == "" {
				continue
			}
			// handle quit/exit command.
			if cmd == "quit" || cmd == "exit" {
				fmt.Println("bye")
				break
			} else if cmd == "help" {
				// show usage information.
				usage()
			} else {
				cmd = strings.TrimSpace(cmd)
				if err = handleCmd(cmd); err != nil {
					// handle invalid command error.
					fmt.Println(err)
					continue
				}

				// send command to server according to TCP proto and get result.
				var result string
				result, err = sendCmd(cmd)
				if err != nil {
					// handle network error.
					fmt.Println(err)
					continue
				}
				fmt.Println(result)

				// add command to history.
				line.AppendHistory(cmd)
			}
		} else if err == liner.ErrPromptAborted {
			// handle Ctrl+C signal.
			fmt.Println("bye")
			break
		} else {
			// handle input error.
			fmt.Println("Error reading line: ", err)
			break
		}
	}
}

// handleCmd checks if command is valid.
func handleCmd(cmd string) (err error) {
	ars := strings.Split(cmd, " ")
	if !commandSet[strings.ToLower(ars[0])] {
		err = ErrCmdNotFound
	}
	return
}

// sendCmd send command to server according to TCP proto and get result.
func sendCmd(cmd string) (string, error) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:8888", time.Second)
	if err != nil {
		return "", err
	}

	_, err = conn.Write([]byte(cmd))
	if err != nil {
		return "", err
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}

	return string(buf[:n]), nil
}

// usage shows usage information.
func usage() {
	for _, val := range helpList {
		fmt.Println(val)
	}
}
