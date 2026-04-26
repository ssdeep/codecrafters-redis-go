package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
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

var storage sync.Map
var listStorage sync.Map

func cleanup() {
	for {
		storage.Range(func(k, v any) bool {
			if val := v.(Value); val.expiry != -1 && val.expiry < time.Now().UnixMilli() {
				storage.Delete(k)
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
		go handleConnection(a, &storage)

	}
}

func handleConnection(a net.Conn, storage *sync.Map) {

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
		if word == "PING" {
			a.Write([]byte("+PONG\r\n"))
		} else {
			fmt.Println("Command: ", cmds)
			switch cmds[0] {
			case "RPUSH":
				fmt.Println("Pushing ", cmds[2], " to ", cmds[1])
				list, _ := listStorage.LoadOrStore(cmds[1], []Value{})
				var items []Value
				for _, item := range cmds[2:] {
					items = append(items, Value{item, -1})
				}
				list = append(list.([]Value), items...)
				listStorage.Store(cmds[1], list)
				length := len(list.([]Value))
				a.Write(resp.IntegersParser{}.Encode(length))
			case "LRANGE":
				fmt.Println("Getting range ", cmds[2], " to ", cmds[3], " from ", cmds[1])
				from, err := strconv.Atoi(cmds[2])
				if err != nil {
					fmt.Println("Error parsing from: ", err.Error())
					return
				}
				to, err := strconv.Atoi(cmds[3])
				if err != nil {
					fmt.Println("Error parsing to: ", err.Error())
					return
				}
				to = to + 1
				list, _ := listStorage.Load(cmds[1])
				arrEncoder := resp.ArraysParser{}
				if list == nil || from >= len(list.([]Value)) || from > to {
					a.Write(arrEncoder.Encode([]string{}))
				} else {
					var items []string
					maxTo := min(to, len(list.([]Value)))
					for _, item := range list.([]Value)[from:maxTo] {
						items = append(items, item.name)
					}
					a.Write(arrEncoder.Encode(items))
				}
			case "SET":
				fmt.Println("Setting ", cmds[1], " to ", cmds[2])
				argSize := len(cmds)
				var exp int64
				var expiryMs int64
				if argSize > 3 {
					fmt.Println("Setting expiry for ", cmds[1], "at", cmds[4])
					exp, err = strconv.ParseInt(strings.Trim(cmds[4], "\r"), 10, 64)
					if err != nil {
						fmt.Println("Error parsing expiry: ", err.Error())
						return
					}

					switch cmds[3] {
					case "PX":
						expiryMs = exp
					case "EX":
						expiryMs = exp * 1000
					default:
						expiryMs = exp
					}

					fmt.Println("Expiring ", cmds[1], " in ", expiryMs, "ms")
					expiryMs = expiryMs + time.Now().UnixMilli()

				} else {
					expiryMs = -1
				}
				storage.Store(cmds[1], Value{cmds[2], expiryMs})
				fmt.Println("Storage: ", storage)
				a.Write([]byte("+OK\r\n"))
			case "GET":
				fmt.Println("Getting ", cmds[1])
				fmt.Println("Storage: ", storage)
				if v, ok := storage.Load(cmds[1]); !ok {
					a.Write([]byte("$-1\r\n"))
				} else {
					a.Write(resp.EncodeBulkString(v.(Value).name))
				}
			case "ECHO":
				a.Write(resp.EncodeBulkString(cmds[1]))
			}
		}

		fmt.Printf("Received: %s\n", word)

	}
}
