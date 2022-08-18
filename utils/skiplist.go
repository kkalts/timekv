package utils

import (
	"bytes"
	"math/rand"
)

/*
	2022-08-18
	SkipList V1.0
*/

/*
	跳表中的每个节点
*/
type Element struct {
	data   KVData     // 真正的数据存放
	levels []*Element // 存放每层的节点
}
type KVData struct {
	Key   []byte
	Value []byte
}

type SkipList struct {
	level  int      // 跳表的层数
	header *Element // 头元素 串起整个链表
}

func NewSkipList() *SkipList {
	return &SkipList{
		level:  1,
		header: nil,
	}
}

/*
	层高判断
	概率性的
	随机的
*/
func (sl *SkipList) RandLevel() int {
	for i := 0; i < sl.level; i++ {
		if rand.Intn(2) == 0 {
			return i
		}
	}
	return 0
}

/*
	比较
*/

/*
	插入元素
		在跳表的每一层做插入操作
*/
func (sl *SkipList) Add(data KVData) {
	preElement := sl.header
	var elem *Element
	var insertLocation *Element
	var preInsertLocation *Element
	for i := sl.level - 1; i >= 0; i-- {
		// 首先找到元素应该插入的位置 从最高层开始找

		for next := preElement.levels[i]; next != nil; preElement = next.levels[i] {
			// 当前小于key 则继续
			if bytes.Compare(next.data.Key, data.Key) == -1 {
				continue
			}
			// 大于等于 则在 当前之前增加节点 找到位置
			if comp := bytes.Compare(next.data.Key, data.Key); comp >= 0 {
				// 如果相等 则有相同元素 进行替换 不能立即替换 因为要替换整列的数据
				if comp == 0 {
					elem = next
					elem.data = data
					return
				}
				// 大于 则找到位置 放在当前的前面
				break
			}
			preInsertLocation = preElement
			insertLocation = next

		}
	}
	// 插入元素
	level := sl.RandLevel()
	elem = NewElement(data, level)
	for i := 0; i < level; i++ {
		elem.levels[i] = insertLocation.levels[i]
		preInsertLocation.levels[i] = elem.levels[i]
	}

}

func NewElement(data KVData, level int) *Element {
	return &Element{
		data:   data,
		levels: make([]*Element, level),
	}
}

/*
	删除元素
*/

/*
	查询元素
*/

func (sl *SkipList) Find(key []byte) []byte {
	preElement := sl.header
	// 逐行查找 从最高层开始 每层找不到 则向下
	for i := sl.level - 1; i >= 0; i-- {
		// 找 每层的 头 尾 中间与key比较
		for next := preElement.levels[i]; next != nil; preElement = next.levels[i] {
			// 当前小于key 则继续
			if bytes.Compare(preElement.data.Key, key) == -1 {
				continue
			}
			// 等于 则返回
			if bytes.Compare(preElement.data.Key, key) == 0 {
				return preElement.data.Value
			}
			// 大于 则跳出
			if bytes.Compare(preElement.data.Key, key) == 1 {
				return nil
			}
		}
	}
	return nil
}
