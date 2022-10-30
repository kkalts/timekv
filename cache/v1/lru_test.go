package v1

import (
	"container/list"
	"fmt"
	"sync"
	"testing"
)

func TestLRU(t *testing.T) {
	var data = make(map[uint64]*list.Element)
	lru := newWindowLRU(2, data)
	item1 := storeItem{
		key:   1,
		value: 1,
	}
	item2 := storeItem{
		key:   2,
		value: 2,
	}
	item3 := storeItem{
		key:   3,
		value: 3,
	}
	item4 := storeItem{
		key:   4,
		value: 4,
	}
	lru.add(item1)
	lru.add(item2)
	lru.add(item3)
	lru.add(item4)
}

func TestLRUGoroutine(t *testing.T) {
	var n = 5
	var wg sync.WaitGroup
	var data = make(map[uint64]*list.Element)
	lru := newWindowLRU(2, data)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lru.add(storeItem{
				key:   uint64(i),
				value: uint64(i),
			})
		}(i)
	}
	wg.Wait()

	fmt.Println("最终链表长度=", lru.list.Len())
}
