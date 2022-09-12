package skiplist

import (
	"fmt"
	"testing"
)

func TestSkipListAdd(t *testing.T) {
	list := newSkipList()
	list.AddNode(5)
	list.AddNode(1)
	list.AddNode(7)
	list.AddNode(2)
	list.AddNode(10)
	list.AddNode(0)
	list.AddNode(-10)

	printSkipList(list)
	fmt.Println(list.HasNode(0))
	fmt.Println(list.HasNode(100))
	list.DeleteNode(1)
	printSkipList(list)
}
