package v1

import (
	"container/list"
	"fmt"
)

/*
	Window-LRU实现
		即最基本的LRU实现 链表+Map
*/

type windowLRU struct {
	data map[uint64]*list.Element // map 这里多线程会有并发安全问题
	cap  int                      // 容量
	list *list.List
}

type storeItem struct {
	stage    int // 主缓存的两个阶段/两块区域
	key      uint64
	conflict uint64 // key冲突时辅助判断
	value    interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		cap:  size,
		data: data,
		list: list.New(),
	}
}

/*
	params:
		newItem：新加入的元素
	return:
		storeItem：被淘汰的元素
		bool：是否有被淘汰的元素

	注意这里不考虑并发问题，如果并发，这里可能cap>>list.len
		这里是一个进 一个出


*/
func (wl *windowLRU) add(newItem storeItem) (eitem storeItem, evicted bool) {
	// 判断当前链表长度与容量 如果小于 则直接加入 并返回
	fmt.Printf("开始的链表长度=%d,元素Key=%d \n", wl.list.Len(), newItem.key)
	if wl.list.Len() < wl.cap {
		wl.data[newItem.key] = wl.list.PushFront(&newItem)
		fmt.Printf("放入后的链表长度=%d,元素Key=%d \n", wl.list.Len(), newItem.key)
		return storeItem{}, false
	}
	// 大于 则将链表尾部的淘汰，并删除map中的这个元素
	back := wl.list.Back()
	if back != nil {
		item := back.Value.(*storeItem)
		if item != nil {
			delete(wl.data, item.key)
			//wl.list.Remove(back)
			// 如果不将淘汰数据从链表中删除，可以进行交换，先将要加入的新元素交换到链表的尾部，然后将链表尾部元素移到链表头部

			// 先将要加入的新元素与链表的尾部元素交换，避免另外的内存开支
			eitem, *item = *item, newItem

			// 交换后的元素放入map
			wl.data[item.key] = back
			// 将链表尾部元素移到链表头部
			wl.list.MoveToFront(back)
			return eitem, true
		}
	}
	return storeItem{}, false

}

/*
	这里get不返回数据 只把元素移动到链表头部
		因为get要结合整个流程
*/
func (wl *windowLRU) get(v *list.Element) {
	wl.list.MoveToFront(v)
}
