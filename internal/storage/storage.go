package storage

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

type Storage struct {
	singles sync.Map
	Lists   sync.Map
	Streams sync.Map
}

var watchers = make(map[string]map[resp.ID]chan bool)

func NewStorage() *Storage {
	return &Storage{
		singles: sync.Map{},
		Lists:   sync.Map{},
		Streams: sync.Map{},
	}
}

func (storage *Storage) SinglesStore(key string, value resp.Value) {
	storage.singles.Store(key, value)
}

func (storage *Storage) SinglesLoad(key string) (any, bool) {
	return storage.singles.Load(key)
}

func (storage *Storage) PushItems(from string, cmds []string) []byte {
	listStorage := &storage.Lists
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

func (storage *Storage) Cleanup() {
	for {
		storage.singles.Range(func(k, v any) bool {
			if val := v.(resp.Value); val.Expiry != -1 && val.Expiry < time.Now().UnixMilli() {
				storage.singles.Delete(k)
			}
			return true
		})
		time.Sleep(1 * time.Millisecond)
	}
}

func (s Storage) HasGreaterKeyID(key string, id string) bool {
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
