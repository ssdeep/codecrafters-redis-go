package commands

import (
	"container/list"
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
//
//	type Value struct {
//		Name   string
//		Expiry int64
//	}
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
	"LLEN":   LLenExecutor{},
	"LPOP":   LPopExecutor{},
	"BLPOP":  BLPopExecutor{},
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

type LPopExecutor struct{}

type LLenExecutor struct{}

type BLPopExecutor struct{}

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
	storage.Store(cmds[1], resp.Value{cmds[2], expiryMs})
	fmt.Println("Storage: ", storage)
	con.Write([]byte("+OK\r\n"))
}

func (g GetExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	fmt.Println("Getting ", cmds[1])
	fmt.Println("Storage: ", storage)
	if v, ok := storage.Load(cmds[1]); !ok {
		con.Write([]byte("$-1\r\n"))
	} else {
		con.Write(resp.EncodeBulkString(v.(resp.Value).Name))
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
	l, _ := listStorage.LoadOrStore(cmds[1], list.New())
	l2 := l.(*list.List)
	for _, item := range cmds[2:] {

		switch from {
		case "R":
			_ = l2.PushBack(resp.Value{item, -1})
		case "L":
			_ = l2.PushFront(resp.Value{item, -1})
		default:
			_ = l2.PushBack(resp.Value{item, -1})

		}
	}
	return resp.IntegersParser{}.Encode(l2.Len())
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

	l2, _ := listStorage.Load(cmds[1])
	arrEncoder := resp.ArraysParser{}
	if l2 == nil {
		con.Write(arrEncoder.Encode(*list.New()))
		return
	}

	l3 := l2.(*list.List)
	fmt.Printf("Current state %s\n", l3)
	if from < 0 {
		from = from + l3.Len()
		from = max(0, from)
	}

	if to < 0 {
		to = to + l3.Len()
		to = max(0, to)
	}

	to = to + 1

	fmt.Println("final adjusted From: ", from, " to: ", to)

	if l3 == nil || l3.Len() == 0 || from >= l3.Len() || from > to {
		con.Write(arrEncoder.Encode(*list.New()))
	} else {
		maxTo := min(to, l3.Len())
		subList := scan(l3, from, maxTo)
		con.Write(arrEncoder.Encode(*subList))
	}
}

func scan(l *list.List, from, to int) (e *list.List) {
	iter := l.Front()
	for range from {
		//fmt.Println("Skipping ", iter.Value.(resp.Value).Name)
		iter = iter.Next()
	}
	s := to - from
	elems := list.New()
	//fmt.Println("Scanning at ", iter.Value.(resp.Value).Name, " elements")
	for range s {
		elems.PushBack(iter.Value.(resp.Value))
		//elems = append(elems, iter.Value.(resp.Value))
		iter = iter.Next()
	}
	return elems
}

func (l LLenExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	l2, _ := listStorage.Load(cmds[1])
	if l2 == nil {
		l2 = list.New()
	}
	con.Write(resp.IntegersParser{}.Encode(l2.(*list.List).Len()))
}

func (l LPopExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	if l2, ok := listStorage.Load(cmds[1]); ok && l2.(*list.List) != nil && l2.(*list.List).Len() > 0 {
		if len(cmds) == 2 {
			l3 := l2.(*list.List)
			popped := l3.Front().Value.(resp.Value).Name
			l3.Remove(l3.Front())
			con.Write(resp.EncodeBulkString(popped))
		} else if len(cmds) == 3 {
			l3 := l2.(*list.List)
			count, err := strconv.Atoi(cmds[2])
			if err != nil {
				fmt.Println("Error parsing count: ", err.Error())
				return
			}
			arrEncoder := resp.ArraysParser{}
			poppedElements := list.New()
			for range count {
				popped := l3.Front().Value.(resp.Value)
				poppedElements.PushBack(popped)
				l3.Remove(l3.Front())
			}

			con.Write(arrEncoder.Encode(*poppedElements))
		} else if len(cmds) == 4 {
			from, err := strconv.Atoi(cmds[2])
			if err != nil {
				fmt.Println("Error parsing from: ", err.Error())
				return
			}
			if from < 0 {
				from = from + l2.(*list.List).Len()
				from = max(0, from)
			}
			to, err := strconv.Atoi(cmds[3])
			if err != nil {
				fmt.Println("Error parsing to: ", err.Error())
				return
			}
			if to < 0 {
				to = to + l2.(*list.List).Len()
			}

			to = to + 1
			count := to - from
			l3 := l2.(*list.List)
			var poppedElements *list.List
			iter := l3.Front()
			for range from {
				iter = iter.Next()
			}
			for range count {
				poppedElements.PushBack(iter.Value.(resp.Value))
				temp := iter
				l3.Remove(temp)
				iter = iter.Next()
			}
			arrEncoder := resp.ArraysParser{}
			if poppedElements == nil || poppedElements.Len() == 0 {
				con.Write(arrEncoder.Encode(*list.New()))
			} else {
				con.Write(arrEncoder.Encode(*poppedElements))
			}
		}

	} else {
		con.Write([]byte("$-1\r\n"))
	}

}

func (l BLPopExecutor) Execute(cmds []string, con net.Conn, storage *sync.Map, listStorage *sync.Map) {
	if len(cmds) == 3 {
		timeout, err := strconv.ParseFloat(strings.Trim(cmds[2], "\r\n"), 64)
		if err != nil {
			fmt.Println("Error parsing timeout: %w", err)
			return
		}
		timeout = timeout * 1000

		response := list.New()
		response.PushBack(resp.Value{cmds[1], -1})

		arrEncoder := resp.ArraysParser{}
		if timeout == 0 {
			popped, ok := popBlocking(cmds[1], listStorage)
			fmt.Println("Blocking popped: ", popped)
			response.PushBack(popped)
			if ok {
				con.Write(arrEncoder.Encode(*response))
			}
		} else {
			timenow := time.Now()
			tout := time.After(time.Millisecond * time.Duration(timeout))
			popped := make(chan resp.Value, 1)
			go func() {
				poppedValue, ok := popBlocking(cmds[1], listStorage)
				popped <- poppedValue
				if ok {
					close(popped)
				}
			}()

			select {
			case <-tout:
				elapsed := time.Since(timenow).Milliseconds()
				fmt.Printf("Blocking pop timed out after %f milliseconds elapsed %d\n", timeout, elapsed)
				con.Write(resp.EncodeNullArray())
			case poppedValue := <-popped:
				response.PushBack(poppedValue)
				con.Write(arrEncoder.Encode(*response))

			}
		}
	} else {
		fmt.Printf("BLPOP expects 2 arguments found %d\n", len(cmds))
	}
}

func popBlocking(key string, listStorage *sync.Map) (resp.Value, bool) {
	for true {
		if l2, ok := listStorage.Load(key); ok && l2.(*list.List) != nil && l2.(*list.List).Len() > 0 {
			l2 := l2.(*list.List)
			item := l2.Front().Value.(resp.Value).Name
			l2.Remove(l2.Front())
			return resp.Value{item, -1}, true
		}
		time.Sleep(100 * time.Millisecond)
	}

	panic("Should never reach here")
}
