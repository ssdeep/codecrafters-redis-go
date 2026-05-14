package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/commands"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

//type Value struct {
//	name   string
//	expiry int64
//}

var storage = commands.Storage{
	Singles: sync.Map{},
	Lists:   sync.Map{},
	Streams: sync.Map{},
}

func cleanup() {
	for {
		storage.Singles.Range(func(k, v any) bool {
			if val := v.(resp.Value); val.Expiry != -1 && val.Expiry < time.Now().UnixMilli() {
				storage.Singles.Delete(k)
			}
			return true
		})
		time.Sleep(1 * time.Millisecond)
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
		commands.Execute(cmds, a, &storage)
		fmt.Printf("Received: %s\n", word)

	}
}
