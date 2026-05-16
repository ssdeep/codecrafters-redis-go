package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/commands"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

//type Value struct {
//	name   string
//	expiry int64
//}

var store = storage.NewStorage()

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Go kv store client")
	go store.Cleanup()
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
	for {

		word, err := resp.Parse(a)
		if err != nil && err.Error() == "EOF" {
			fmt.Println("Connection closed")
			a.Close()
			return
		} else if err != nil {
			fmt.Println("Error parsing: ", err.Error())
			return
		}

		cmds := strings.Split(word, " ")
		fmt.Println(cmds)
		commands.Execute(cmds, a, store)
		fmt.Printf("Received: %s\n", word)

	}
}
