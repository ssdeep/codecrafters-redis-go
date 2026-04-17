package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type Value struct {
	name   string
	expiry int64
}

var storage = make(map[string]Value)

func cleanup() {
	for {
		for k, v := range storage {
			if v.expiry != -1 {
				if v.expiry > time.Now().UnixMilli() {
					delete(storage, k)
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Go kv store client")
	go cleanup()
	// Uncomment the code below to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	defer l.Close()
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		a, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(a)
	}
}

func handleConnection(a net.Conn) {

	fmt.Println("Connected")
	for {

		word, err := resp.Parse(a)
		if err != nil {
			fmt.Println("Error parsing: ", err.Error())
			return
		}

		cmds := strings.Split(word, " ")
		if word == "PING" {
			a.Write([]byte("+PONG\r\n"))
		} else {
			switch cmds[0] {
			case "SET":
				fmt.Println("Setting ", cmds[1], " to ", cmds[2])
				argSize := len(cmds)
				var exp int64
				if argSize > 3 {
					exp, _ = strconv.ParseInt(cmds[3], 10, 64)
					exp = exp + time.Now().UnixMilli()
				} else {
					exp = -1
				}
				storage[cmds[1]] = Value{cmds[2], exp}
				a.Write([]byte("+OK\r\n"))
			case "GET":
				if _, ok := storage[cmds[1]]; !ok {
					a.Write([]byte("$-1\r\n"))
				} else {
					a.Write(resp.EncodeBulkString(storage[cmds[1]].name))
				}
			case "ECHO":
				a.Write(resp.EncodeBulkString(cmds[1]))
			}
		}

		fmt.Printf("Received: %s\n", word)

	}
}
