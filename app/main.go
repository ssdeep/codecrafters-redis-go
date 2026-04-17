package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
var storage = make(map[string]string)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Go kv store client")

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
				keyVals := strings.Split(cmds[1], " ")
				storage[keyVals[0]] = keyVals[1]
			case "GET":
				if _, ok := storage[cmds[1]]; !ok {
					a.Write([]byte("$-1\r\n"))
				} else {
					a.Write(resp.EncodeBulkString(storage[cmds[1]]))
				}
			case "ECHO":
				a.Write(resp.EncodeBulkString(cmds[1]))
			}
		}

		fmt.Printf("Received: %s\n", word)

	}
}
