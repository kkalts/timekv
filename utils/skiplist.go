package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"

	//"fmt"
	"math/rand"
	"time"
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
	levels []*Element // 当前节点每层指向的节点
}
type KVData struct {
	Key   []byte
	Value []byte
}

type SkipList struct {
	level  int      // 跳表的层数
	header *Element // 头元素 串起整个链表
}

const MaxLevel = 10

func NewSkipList() *SkipList {
	//headerData:=KVData{
	//	[]byte("0"),
	//	[]byte("0"),
	//}
	header := &Element{levels: make([]*Element, MaxLevel)}

	return &SkipList{
		level:  1,
		header: header,
	}
}

/*
	层高判断
	概率性的
	随机的
*/
func (sl *SkipList) RandLevel() int {
	maxLevel := 10
	i := 1
	for ; i < maxLevel; i++ {
		rand.Seed(time.Now().UnixNano())
		intn := rand.Intn(2)
		//fmt.Println("intn=",intn)
		//fmt.Println("i=",i)
		if intn == 0 {
			return i
		}
	}
	return i
}

/*
	比较
*/

/*
	插入元素
		在跳表的每一层做插入操作
*/
// 可能还有问题
func (sl *SkipList) Add(data KVData) {
	preElement := sl.header
	var elem *Element

	var prevElementHeaders [MaxLevel]*Element
	for i := sl.level - 1; i >= 0; i-- {
		prevElementHeaders[i] = preElement
		// 首先找到元素应该插入的位置 从最高层开始找
		for cur := preElement.levels[i]; cur != nil; cur = preElement.levels[i] {

			// 大于等于 则在 当前之前增加节点 找到位置
			//comp := bytes.Compare(cur.data.Key, data.Key)
			//fmt.Println("comp=",comp)
			if comp := bytes.Compare(cur.data.Key, data.Key); comp >= 0 {
				// 相等 则替换 这里都是指针 一更换 则key对应的值全更换
				if comp == 0 {
					elem = cur
					elem.data = data
					return
				}
				// 大于 则找到位置 放在当前的前面
				break
			}
			// 小于 在层中平行移动
			preElement = cur
			//fmt.Println("preElement=",*preElement)
			// 记录每层的最后一个小于当前data的元素
			prevElementHeaders[i] = preElement
			// 则要插入的节点的下一个就是 prevElementHeaders[i].next 即 elem.next = prevElementHeaders[i].next
		}
	}
	// 插入元素
	level := sl.RandLevel()
	level = 10

	// 如果随机出来很大的层 则只提取一层
	if level > sl.level {
		level = sl.level + 1
		// 只提取出一层 则顶层的最后最小实战是无的 即header
		prevElementHeaders[sl.level] = sl.header
		sl.level = level
	}

	elem = NewElement(data, level)
	// 需要调整 可能每次新增都要增加一层 层数过高
	for i := 0; i < level; i++ {
		// 在找到的位置之后插入
		// 先操作当前插入节点后面的
		elem.levels[i] = prevElementHeaders[i].levels[i]
		// 当前插入节点前面的next 指向当前节点
		prevElementHeaders[i].levels[i] = elem
	}
}

func NewElement(data KVData, level int) *Element {
	return &Element{
		data:   data,
		levels: make([]*Element, level),
	}
}

func (sl *SkipList) Add2(data KVData) *Element {
	update := make([]*Element, MaxLevel)
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		comp := bytes.Compare(x.data.Key, data.Key)
		for x.levels[i] != nil && comp < 0 {
			x = x.levels[i]
		}
		update[i] = x
	}
	x = x.levels[0]

	// Score already presents, replace with new value then return
	if x != nil && bytes.Compare(x.data.Key, data.Key) == 0 {
		x.data.Value = data.Value
		return x
	}

	level := sl.RandLevel()
	level = 10
	if level > sl.level {
		level = sl.level + 1
		update[sl.level] = sl.header
		sl.level = level
	}
	e := NewElement(data, level)
	for i := 0; i < level; i++ {
		e.levels[i] = update[i].levels[i]
		update[i].levels[i] = e
	}
	//sl.len++
	return e
}

/*
	删除元素
*/

/*
	查询元素
*/

func (sl *SkipList) Find(key []byte) []byte {
	preElement := sl.header // header是空的
	// header.levels[0]=第一层第一个 ...
	// 逐行查找 从最高层开始 每层找不到 则向下
	for i := sl.level - 1; i >= 0; i-- {
		// 找 每层的 头 尾 中间与key比较
		for cur := preElement.levels[i]; cur != nil; cur = preElement.levels[i] {

			// 当前小于key 则继续
			if bytes.Compare(cur.data.Key, key) == -1 {
				preElement = cur
				//cur = preElement
				continue
			}
			// 等于 则返回
			if bytes.Compare(cur.data.Key, key) == 0 {
				return cur.data.Value
			}
			// 大于 则跳出
			if bytes.Compare(cur.data.Key, key) == 1 {
				break
			}
		}
	}
	return nil
}

/*
	删除节点
		先找到节点 如果找不到返回nil

	timekv应该是不需要/不能 删除节点 只增
*/

/*
	打印skiplist
*/
func PrintSkipList(sl *SkipList) {
	// 逐行查找 从最高层开始 每层找不到 则向下
	for i := sl.level - 1; i >= 0; i-- {
		preElement := sl.header // 每一层从头开始
		// 找 每层的 头 尾 中间与key比较
		for cur := preElement.levels[i]; cur != nil; cur = preElement.levels[i] {
			fmt.Printf("%s.%s ->", string(cur.data.Key), string(cur.data.Value))
			preElement = cur
			continue
		}
		fmt.Println()
	}
}

func PrintSkipListInt(sl *SkipList) {
	// 逐行查找 从最高层开始 每层找不到 则向下
	for i := sl.level - 1; i >= 0; i-- {
		preElement := sl.header // 每一层从头开始
		// 找 每层的 头 尾 中间与key比较
		for cur := preElement.levels[i]; cur != nil; cur = preElement.levels[i] {
			fmt.Printf("%d.%d ->", BytesToInt(cur.data.Value), BytesToInt(cur.data.Value))
			preElement = cur
			continue
		}
		fmt.Println()
	}
}

/*
	校验每层顺序
*/
func CheckSkipListSort(sl *SkipList) (bool, string, string) {
	for i := sl.level - 1; i >= 0; i-- {
		preElement := sl.header // 每一层从头开始
		// 找 每层的 头 尾 中间与key比较
		for cur := preElement.levels[i]; cur != nil; cur = preElement.levels[i] {
			// 比较pre和cur 当前小于 之前的 则false
			compare := bytes.Compare(cur.data.Key, preElement.data.Value)
			if compare == -1 {
				fmt.Println("compare=", compare)
				fmt.Println("level=", i+1)
				return false, string(cur.data.Key), string(preElement.data.Key)
			}
			preElement = cur
			continue
		}
		fmt.Println()
	}
	return true, "", ""
}

func CheckSkipListSortInt(sl *SkipList) (bool, int, int) {
	for i := sl.level - 1; i >= 0; i-- {
		preElement := sl.header // 每一层从头开始
		// 找 每层的 头 尾 中间与key比较

		for cur := preElement.levels[i]; cur != nil; cur = preElement.levels[i] {
			if preElement.data.Key != nil {
				// 比较pre和cur 当前小于 之前的 则false
				compare := bytes.Compare(cur.data.Key, preElement.data.Value)
				if compare == -1 {
					fmt.Println("compare=", compare)
					fmt.Println("level=", i+1)
					return false, BytesToInt(cur.data.Value), BytesToInt(preElement.data.Value)
				}
			}
			preElement = cur
			continue
		}
		fmt.Println()
	}
	return true, 0, 0
}

func IntToBytes(n int) []byte {
	x := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

//字节转换成整形
func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)

	return int(x)
}
