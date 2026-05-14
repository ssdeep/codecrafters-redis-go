package commands

import (
	"container/list"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

type Storage struct {
	Singles sync.Map
	Lists   sync.Map
	Streams sync.Map
}

var watchers = make(map[string]map[resp.ID]chan bool)

type Executor interface {
	Execute(cmds []string, con net.Conn, storage *Storage)
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
	"TYPE":   TypeExecutor{},
	"XADD":   XAddExecutor{},
	"XRANGE": XRangeExecutor{},
	"XREAD":  XReadExecutor{},
	"AUTH":   AuthExecutor{},
	"INCR":   IncrExecutor{},
}

func Execute(cmds []string, con net.Conn, storage *Storage) {
	executors[strings.ToUpper(cmds[0])].Execute(cmds, con, storage)
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

type TypeExecutor struct{}

type XAddExecutor struct{}

type XRangeExecutor struct{}

type XReadExecutor struct{}

type AuthExecutor struct{}

type IncrExecutor struct{}

func (a AuthExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	fmt.Println("Authenticating ", cmds[1])
	con.Write([]byte("+OK\r\n"))
}

func (s Storage) hasGreaterKeyID(key string, id string) bool {
	currentID, err := resp.IdSplits(id)
	if err != nil {
		fmt.Println("Error parsing id: ", err.Error())
		return false
	}

	if _, ok := s.Streams.Load(key); !ok {
		return false
	}

	some, _ := s.Streams.Load(key)
	stream := some.(*list.List)
	if stream == nil {
		return false
	}
	if stream.Len() == 0 {
		return false
	}

	streamAsList := stream.Front()
	//fmt.Printf("Current stream state %t\n", streamAsList)

	if streamAsList == nil || streamAsList.Value == nil {
		fmt.Println("streamAsList is empty")
		return false
	}

	for item, ok := streamAsList.Value.(**resp.Entry); ok; item, ok = streamAsList.Next().Value.(**resp.Entry) {
		id, err := (**item).IdSplits()
		//fmt.Println("Comparing ", id, " to ", currentID)
		if err != nil {
			fmt.Println("Error parsing id: ", err.Error())
			return false
		}
		if id.Gt(currentID) {
			fmt.Println("Found greater key id at ", id)
			return true
		}

		if streamAsList.Next() == nil {
			//fmt.Println("Reached end of stream")
			break
		}
	}

	return false
}

func (p PingExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	con.Write([]byte("+PONG\r\n"))
}

func (s SetExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
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
	storage.Singles.Store(cmds[1], resp.Value{cmds[2], expiryMs})
	fmt.Println("Storage: ", storage)
	con.Write([]byte("+OK\r\n"))
}

func (g GetExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	fmt.Println("Getting ", cmds[1])
	fmt.Println("Storage: ", storage)
	if v, ok := storage.Singles.Load(cmds[1]); !ok {
		con.Write([]byte("$-1\r\n"))
	} else {
		con.Write(resp.EncodeBulkString(v.(resp.Value).Name))
	}
}

func (e EchoExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	con.Write(resp.EncodeBulkString(cmds[1]))
}

func (r RPushExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	response := pushItems("R", cmds, &storage.Lists)
	con.Write(response)
}

func (r LPushExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	response := pushItems("L", cmds, &storage.Lists)
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
	return resp.IntegersParser{}.Encode(int64(l2.Len()))
}

func (l LRangeExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
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

	l2, _ := storage.Lists.Load(cmds[1])
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

func (l LLenExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	l2, _ := storage.Lists.Load(cmds[1])
	if l2 == nil {
		l2 = list.New()
	}
	con.Write(resp.IntegersParser{}.Encode(int64(l2.(*list.List).Len())))
}

func (l LPopExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	if l2, ok := storage.Lists.Load(cmds[1]); ok && l2.(*list.List) != nil && l2.(*list.List).Len() > 0 {
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
			poppedElements := list.New()
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
			con.Write(arrEncoder.Encode(*poppedElements))
		}

	} else {
		con.Write([]byte("$-1\r\n"))
	}

}

func (l BLPopExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
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
			popped, ok := popBlocking(cmds[1], &storage.Lists)
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
				poppedValue, ok := popBlocking(cmds[1], &storage.Lists)
				popped <- poppedValue
				if ok {
					close(popped)
				}
			}()

			select {
			case timeafter := <-tout:
				elapsed := time.Since(timenow).Milliseconds()
				fmt.Printf("Blocking pop timed out after %f milliseconds elapsed %d, started at %s ended at %s \n", timeout,
					elapsed,
					timenow, timeafter)
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

func (t TypeExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	fmt.Println("Getting type of ", cmds[1])
	if _, ok := storage.Singles.Load(cmds[1]); ok {
		con.Write(resp.EncodeSimpleString("string"))
	} else if _, ok := storage.Lists.Load(cmds[1]); ok {
		con.Write(resp.EncodeSimpleString("list"))
	} else if _, ok := storage.Streams.Load(cmds[1]); ok {
		con.Write(resp.EncodeSimpleString("stream"))
	} else {
		con.Write(resp.EncodeSimpleString("none"))
	}
}

func (x XAddExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	fmt.Println("XAdd to streams ", cmds[1])
	stream, _ := storage.Streams.LoadOrStore(cmds[1], list.New())
	streamAsList := stream.(*list.List)

	validatedID, err := validateId(cmds[2], streamAsList)
	if validatedID == "" && err != nil {
		con.Write(resp.EncodeError(err.Error()))
		return
	}
	entry := resp.NewEntry(validatedID)
	streamAsList.PushBack(&entry)
	for i := 3; i < len(cmds)-1; i++ {
		entry.AddValue(cmds[i], cmds[i+1])
		i = i + 1
	}

	con.Write(resp.EncodeBulkString(validatedID))
}

func validateId(id string, l *list.List) (string, error) {
	if id == "*" && l.Len() == 0 {
		return fmt.Sprintf("%d-0", time.Now().UnixMilli()), nil
	} else if id == "*" && l.Len() > 0 {
		entry := l.Back().Value.(resp.Entry)
		prevId, err := entry.IdSplits()

		if err != nil {
			fmt.Println("Error parsing id: ", err.Error())
			return "", err
		}
		nowMs := time.Now().UnixMilli()

		if prevId.Millis == nowMs {
			return fmt.Sprintf("%d-%d", prevId.Millis, prevId.Seq+1), nil
		} else {
			return fmt.Sprintf("%d-0", nowMs), nil
		}
	}

	id_parts := strings.Split(id, "-")
	millis, err := strconv.ParseInt(id_parts[0], 10, 64)
	if err != nil {
		fmt.Println("Error parsing id millisecond part: ", id_parts[0], err.Error())
		return "", err
	}
	var seq int64
	if id_parts[1] == "*" && l.Len() == 0 {
		if millis == 0 {
			seq = 1
		} else {
			seq = 0
		}
	} else if id_parts[1] == "*" && l.Len() > 0 {
		entryRef := l.Back().Value.(**resp.Entry)
		entry := *entryRef
		prevId, err := entry.IdSplits()
		if err != nil {
			fmt.Println("Error parsing id: ", err.Error())
			return "", err
		}
		if prevId.Millis != millis {
			seq = 0
		} else {
			seq = prevId.Seq + 1
		}

	} else {
		seq, err = strconv.ParseInt(id_parts[1], 10, 64)
		if err != nil {
			fmt.Println("Error parsing id sequence part: ", id_parts[1], err.Error())
			return "", err
		}
	}

	if millis <= 0 && seq <= 0 {
		return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	if l.Len() > 0 {
		entry := l.Back().Value.(**resp.Entry)
		prevId, err := (*entry).IdSplits()
		if err != nil {
			fmt.Println("Error parsing id: ", err.Error())
			return "", err
		}
		if prevId.Millis > millis || (prevId.Millis == millis && prevId.Seq >= seq) {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	return fmt.Sprintf("%d-%d", millis, seq), nil
}

func popBlocking(key string, listStorage *sync.Map) (resp.Value, bool) {
	for true {
		if l2, ok := listStorage.Load(key); ok && l2.(*list.List) != nil && l2.(*list.List).Len() > 0 {
			l2 := l2.(*list.List)
			item := l2.Front().Value.(resp.Value).Name
			l2.Remove(l2.Front())
			return resp.Value{item, -1}, true
		}
		time.Sleep(1 * time.Millisecond)
	}

	panic("Should never reach here")
}

func idExtractor(s string) (resp.ID, error) {
	if s == "-" {
		return resp.ID{Millis: 0, Seq: 0}, nil
	} else if s == "+" {
		return resp.ID{Millis: math.MaxInt64, Seq: math.MaxInt64}, nil
	} else {
		return resp.IdSplits(s)
	}

}

func (x XRangeExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	key := cmds[1]
	if values, ok := storage.Streams.Load(key); ok {

		var from, to resp.ID
		var err error
		from, err = idExtractor(cmds[2])

		if err != nil {
			fmt.Println("Error parsing from: ", err.Error())
			return
		}
		to, err = idExtractor(cmds[3])

		if err != nil {
			fmt.Println("Error parsing to: ", err.Error())
			return
		}

		result, err := extractResultsBetweenIds(values.(*list.List), from, to, true)
		if err != nil {
			fmt.Println("Error extracting results between ids: ", err.Error())
			return
		}
		fmt.Println("Added for result ", result.Len())
		arrParser := resp.ArraysParser{}
		_, err = con.Write(arrParser.EncodeEntries(result))
		if err != nil {
			fmt.Println("Error writing to client: ", err.Error())
			return
		}
	} else {
		_, err := con.Write(resp.EncodeNullArray())
		if err != nil {
			fmt.Println("Error writing to client: ", err.Error())
		}
		return
	}

}

func (x XReadExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	fmt.Println("XRead from streams ", cmds[1])
	switch strings.ToUpper(cmds[1]) {
	case "BLOCK":
		timeout, err := strconv.ParseFloat(strings.Trim(cmds[2], "\r\n"), 64)
		if err != nil {
			fmt.Println("Error parsing timeout: %w", err)
			return
		}

		if cmds[5] == "$" {
			if storage.hasGreaterKeyID(cmds[4], "0-0") {
				items, _ := storage.Streams.Load(cmds[4])
				id := items.(*list.List).Back().Value.(**resp.Entry)
				cmds[5] = (*id).ID
			} else {
				cmds[5] = fmt.Sprintf("%d-0", time.Now().UnixMilli())
			}

			fmt.Println("Setting block ID to ", cmds[5])
		}

		fmt.Println("Timeout is ", timeout)
		if timeout == 0 {
			waitchan := make(chan bool)
			go func() {
				for true {
					if storage.hasGreaterKeyID(cmds[4], cmds[5]) {
						waitchan <- true
						break
					}
					time.Sleep(1 * time.Millisecond)
				}
			}()

			until := <-waitchan
			fmt.Println("Indefinite Blocking read key is there until=", until)
			x.ExecuteRead(cmds[2:], con, storage, false)
			close(waitchan)
			return
		}

		var waitchan = make(chan bool)
		timenow := time.Now()
		timeAfter := time.After(time.Millisecond * time.Duration(timeout))
		go func() {
			for true {
				if storage.hasGreaterKeyID(cmds[4], cmds[5]) {
					waitchan <- true
					break
				}
				time.Sleep(1 * time.Millisecond)
			}
		}()

		select {
		case timeafter := <-timeAfter:
			elapsed := time.Since(timenow).Milliseconds()
			fmt.Printf("XRead timed out after %f milliseconds elapsed %d, started at %s ended at %s \n", timeout,
				elapsed,
				timenow, timeafter)
			written, err := con.Write(resp.EncodeNullArray())
			fmt.Println("Error writing to client: ", err.Error(), " written ", written)
		case <-waitchan:
			fmt.Println("Blocking read key is there")
			close(waitchan)
			x.ExecuteRead(cmds[2:], con, storage, false)
			//default:
			//	fmt.Println("No blocking read key is there")
		}
	case "STREAMS":
		x.ExecuteRead(cmds, con, storage, true)
	}
}

func (x XReadExecutor) ExecuteRead(cmds []string, con net.Conn, storage *Storage, includeStart bool) {
	totalKeyCount := len(cmds[2:]) / 2
	backIndex := len(cmds) - 1
	totalKeys := fmt.Sprintf("*%d%s", totalKeyCount, resp.CRLF)
	_, err := con.Write([]byte(totalKeys))
	if err != nil {
		fmt.Println("Error writing to client: ", err.Error())
		return
	}
	for i := range totalKeyCount {
		keyIndex := i + 2
		key := cmds[keyIndex]

		if values, ok := storage.Streams.Load(key); ok {
			var from, to resp.ID
			var err error
			from, err = idExtractor(cmds[backIndex-i])
			if err != nil {
				fmt.Println("Error parsing from: ", err.Error())
				return
			}
			to, err = idExtractor("+")
			if err != nil {
				fmt.Println("Error parsing to: ", err.Error())
				return
			}

			fmt.Println("Getting results for ", key, " from ", from, " to ", to)
			result, err := extractResultsBetweenIds(values.(*list.List), from, to, includeStart)
			if err != nil {
				fmt.Println("Error extracting results between ids: ", err.Error())
				return
			}
			fmt.Println("Added for result ", result.Len())

			arrParser := resp.ArraysParser{}
			startStream := fmt.Sprintf("*%d%s", 2, resp.CRLF)
			byteArr := []byte(startStream)
			con.Write(byteArr)
			con.Write(resp.EncodeBulkString(key))
			_, err = con.Write(arrParser.EncodeEntries(result))
			if err != nil {
				fmt.Println("Error writing to client: ", err.Error())
				return
			}

		} else {
			_, err := con.Write(resp.EncodeNullArray())
			if err != nil {
				fmt.Println("Error writing to client: ", err.Error())
			}
			return
		}
	}

}

func extractResultsBetweenIds(entries *list.List, from, to resp.ID, includeStart bool) (*list.List, error) {
	result := list.New()
	l := entries.Front()
	for item := l; item != nil; item = item.Next() {
		entry := item.Value.(**resp.Entry)
		entryId, err := (*entry).IdSplits()

		if err != nil {
			fmt.Println("Unexpected Error in a saved entry ", err.Error())
			return nil, err
		}
		if entryId.Lt(from) {
			continue
		}

		if (includeStart && entryId.Gte(from) || entryId.Gt(from)) && entryId.Lte(to) {
			fmt.Println("Adding entry ", entryId)
			result.PushBack(&entry)
		}

		if entryId.Gt(to) {
			break
		}
	}
	return result, nil
}

func (x IncrExecutor) Execute(cmds []string, con net.Conn, storage *Storage) {
	fmt.Println("Incr ", cmds[1])
	if value, ok := storage.Singles.Load(cmds[1]); ok {
		intParser := resp.IntegersParser{}
		valueInt, err := strconv.ParseInt(value.(resp.Value).Name, 10, 64)
		if err != nil {
			fmt.Println("Error parsing value: ", err.Error())
			return
		}
		storage.Singles.Store(cmds[1], resp.Value{fmt.Sprintf("%d", valueInt+1), value.(resp.Value).Expiry})
		var newInt = valueInt + 1
		con.Write(intParser.Encode(newInt))

	} else {
		storage.Singles.Store(cmds[1], 1)
	}
}
