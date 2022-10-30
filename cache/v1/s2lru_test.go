package v1

import (
	"container/list"
	"fmt"
	"testing"
)

func TestS2LRU1(t *testing.T) {
	s1 := &storeItem{key: 1, value: 1}
	s2 := &storeItem{key: 2, value: 2}
	e1 := &list.Element{Value: s1}
	e2 := &list.Element{Value: s2}

	s1_1 := e1.Value.(*storeItem)
	s2_2 := e2.Value.(*storeItem)

	fmt.Println("*s1_1=", *s1_1)
	fmt.Println("*s2_2=", *s2_2)
	var transValue = &storeItem{}
	//a:=*s1_1
	//b:=*s2_2
	*transValue = *s1_1
	*s1_1 = *s2_2
	*s2_2 = *transValue

	//a = b
	//b = transValue
	//fmt.Println("*s1_1=",a)
	//fmt.Println("*s2_2=",b)
	//*s1_1 = *s2_2
	//*s2_2 = *s1_1
	//
	//fmt.Println(s1_1,"   ",s2_2)
}

func TestS2LRU2(t *testing.T) {
	var data = make(map[uint64]*list.Element)
	lru := newSegmentedLRU(1, 1, data)
	s1 := storeItem{1, 1, 0, 1}
	s2 := storeItem{2, 2, 0, 2}
	lru.add(s1)
	lru.add(s2)
	//lru.stageOne.PushFront(&s1)
	//lru.stageOne.PushFront(&s2)
	e1 := lru.data[s1.key]
	e2 := lru.data[s2.key]
	lru.get(e1)
	lru.get(e2)

}
