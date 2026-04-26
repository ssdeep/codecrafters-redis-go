package commands

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

// var storage sync.Map
// var listStorage sync.Map
type Value struct {
	Name   string
	Expiry int64
}

type Executor interface {
	Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map)
}

var executors = map[string]Executor{
	"PING":   PingExecutor{},
	"SET":    SetExecutor{},
	"GET":    GetExecutor{},
	"ECHO":   EchoExecutor{},
	"RPUSH":  RPushExecutor{},
	"LPUSH":  LPushExecutor{},
	"LRANGE": LRangeExecutor{},
}

func Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	executors[cmds[0]].Execute(cmds, con, storage, listStorage)
}

type PingExecutor struct{}
type SetExecutor struct{}
type GetExecutor struct{}

type EchoExecutor struct{}

type RPushExecutor struct{}

type LPushExecutor struct{}
type LRangeExecutor struct{}

func (p PingExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	con.Write([]byte("+PONG\r\n"))
}

func (s SetExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	fmt.Println("Setting ", cmds[1], " to ", cmds[2])
	argSize := len(cmds)
	var expiryMs int64
	if argSize > 3 {
		fmt.Println("Setting expiry for ", cmds[1], "at", cmds[4])
		exp, err := strconv.ParseInt(strings.Trim(cmds[4], "\r"), 10, 64)
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
	con.Write([]byte("+OK\r\n"))
}

func (g GetExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	fmt.Println("Getting ", cmds[1])
	fmt.Println("Storage: ", storage)
	if v, ok := storage.Load(cmds[1]); !ok {
		con.Write([]byte("$-1\r\n"))
	} else {
		con.Write(resp.EncodeBulkString(v.(Value).Name))
	}
}

func (e EchoExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	con.Write(resp.EncodeBulkString(cmds[1]))
}

func (r RPushExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	response := pushItems("R", cmds, listStorage)
	con.Write(response)
}

func (r LPushExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	response := pushItems("L", cmds, listStorage)
	con.Write(response)
}

func pushItems(from string, cmds []string, listStorage *sync.Map) []byte {
	fmt.Println("Pushing ", cmds[2], " to ", cmds[1])
	list, _ := listStorage.LoadOrStore(cmds[1], []Value{})
	var items []Value
	for _, item := range cmds[2:] {
		items = append(items, Value{item, -1})
	}
	switch from {
	case "R":
		list = append(list.([]Value), items...)
	case "L":
		list = append(items, list.([]Value)...)
	default:
		list = append(list.([]Value), items...)
	}
	//list = append(list.([]Value), items...)
	listStorage.Store(cmds[1], list)
	length := len(list.([]Value))
	return resp.IntegersParser{}.Encode(length)
}

func (l LRangeExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
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

	list, _ := listStorage.Load(cmds[1])
	if from < 0 {
		from = from + len(list.([]Value))
		from = max(0, from)
	}

	if to < 0 {
		to = to + len(list.([]Value))
		to = max(0, to)
	}

	to = to + 1

	arrEncoder := resp.ArraysParser{}
	if list == nil || from >= len(list.([]Value)) || from > to {
		con.Write(arrEncoder.Encode([]string{}))
	} else {
		var items []string
		maxTo := min(to, len(list.([]Value)))
		for _, item := range list.([]Value)[from:maxTo] {
			items = append(items, item.Name)
		}
		con.Write(arrEncoder.Encode(items))
	}
}
