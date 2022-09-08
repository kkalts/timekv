package v1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

/*
	2022-09-08
	SkipList V2.0
	配合Arena的跳表实现
*/

/*
	跳表中的每个节点
*/
type Element struct {
	Score float64 // 分数 加快比较与查找
	value uint64  // value在arena上的位置（offset） 和 value占据的大小(size) 将offset和size编码在一起 方便cas ？ 有并发操作

	keyOffset uint32 // 不可变 不需要加锁访问？ key只会增加 不会释放 key相同 会更新value 访问不需要加锁
	keySize   uint16 // key的size （在arena上） 不可变 不需要加锁访问

	height uint16 // 节点所在层级 代表这个节点有多少个next指针 （如果是每层的最后一个节点？）

	tower [maxHeight]uint32 // 这个节点 在每层的next节点的地址/指针？
}
type KVData struct {
	Key   []byte
	Value []byte
}

type SkipList struct {
	lock  sync.RWMutex // 读写锁 保证并发安全 读写冲突 写写冲突
	level int          // 跳表当前的层数
	//header *Element // 头元素 串起整个链表
	headOffset uint32 // 跳表头节点 在Arena的offset
	arena      *Arena // 每个跳表 的 内存管理器
	maxLevel   int    // 跳表最大高度

}

const MaxLevel = 10

func NewSkipList() *SkipList {
	sl := &SkipList{}
	sl.maxLevel = maxHeight
	sl.arena = NewArena(1 << 16)
	sl.lock = sync.RWMutex{}
	val := ValueStruct{}
	headerNode := NewNode(nil, val, sl.arena, sl.maxLevel)
	headerOffset := sl.arena.getNodeOffset(headerNode)
	sl.headOffset = headerOffset
	return sl
}

/*
	层高判断
	概率性的
	随机的
	 需要优化
*/
func (sl *SkipList) RandLevel() int {
	maxLevel := 10
	i := 1
	for ; i < maxLevel; i++ {
		rand.Seed(time.Now().UnixNano())
		intn := rand.Intn(2)
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

	score := calScore(data.Key)

	var prevElementHeaders [MaxLevel]*Element
	for i := sl.level - 1; i >= 0; i-- {
		prevElementHeaders[i] = preElement
		// 首先找到元素应该插入的位置 从最高层开始找
		for cur := preElement.levels[i]; cur != nil; cur = preElement.levels[i] {

			// 大于等于 则在 当前之前增加节点 找到位置
			//comp := bytes.Compare(cur.data.Key, data.Key)
			//fmt.Println("comp=",comp)
			if comp := compare(score, data.Key, cur); comp >= 0 {
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
	//level = 1
	// 如果随机出来很大的层 则只提取一层
	if level > sl.level {
		level = sl.level + 1
		// 只提取出一层 则顶层的最后最小实战是无的 即header
		prevElementHeaders[sl.level] = sl.header
		sl.level = level
	}

	elem = NewNode(data, level)
	// 需要调整 可能每次新增都要增加一层 层数过高
	for i := 0; i < level; i++ {
		// 在找到的位置之后插入
		// 先操作当前插入节点后面的
		elem.levels[i] = prevElementHeaders[i].levels[i]
		// 当前插入节点前面的next 指向当前节点
		prevElementHeaders[i].levels[i] = elem
	}
}

func NewNode(key []byte, v ValueStruct, arena *Arena, height int) *node {
	//

	score := calScore(key)

	keyOffset := arena.putKey(key)
	valueOffset := arena.putValue(v)
	valueSize := v.EncodeSize()
	val := encodeValue(valueOffset, valueSize)
	// 给node分配内存
	nodeOffset := arena.putNode(height)

	// 获取node所在
	node := arena.getNode(nodeOffset)

	node.Score = score
	node.value = val
	node.keySize = uint16(len(key))
	node.keyOffset = keyOffset

	return node
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
	score := calScore(key)
	// header.levels[0]=第一层第一个 ...
	// 逐行查找 从最高层开始 每层找不到 则向下
	for i := sl.level - 1; i >= 0; i-- {
		// 找 每层的 头 尾 中间与key比较
		for cur := preElement.levels[i]; cur != nil; cur = preElement.levels[i] {

			// 当前小于key 则继续
			if compare(score, key, cur) == -1 {
				preElement = cur
				//cur = preElement
				continue
			}
			// 等于 则返回
			if compare(score, key, cur) == 0 {
				return cur.data.Value
			}
			// 大于 则跳出
			if compare(score, key, cur) == 1 {
				break
			}
		}
	}
	return nil
}

/*
	删除节点
		先找到节点 如果找不到返回nil 或不返回
		找到 则记录其后面的节点
		在一层中最终都没找到 则需要销毁preE中的这层的节点

	这里只删除 不调整层高

	timekv应该是不需要/不能 删除节点 只增


*/
func (sl *SkipList) Delete(key []byte) {
	preElement := sl.header

	var prevElementHeaders [MaxLevel]*Element
	var nextElementHeaders [MaxLevel]*Element
	for i := sl.level - 1; i >= 0; i-- {
		prevElementHeaders[i] = preElement
		//fmt.Println("第一层循环prevElementHeaders[i] key=",BytesToInt(prevElementHeaders[i].data.Key))
		//nextElementHeaders[i] = preElement.levels[i]
		// 首先找到元素应该插入的位置 从最高层开始找
		for cur := preElement.levels[i]; cur != nil; cur = preElement.levels[i] {

			// 大于等于 则在 当前之前增加节点 找到位置
			//comp := bytes.Compare(cur.data.Key, data.Key)
			//fmt.Println("comp=",comp)
			if comp := compare(calScore(key), key, cur); comp >= 0 {
				// 相等 则找到 如果没有 则可以return
				// 找到 则可break 然后根据prevElementHeaders做处理
				if comp == 0 {
					// 需要删除的 则记录当前之后的
					nextElementHeaders[i] = cur.levels[i]
				} else {
					prevElementHeaders[i] = nil
				}

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

	// 从底层开始删除 如果prevElementHeaders中没有的 则不用管

	for i := 0; i < sl.level; i++ {

		if prevElementHeaders[i] != nil {
			//fmt.Printf("i=%d 每层prevElementHeaders[i] key=%d \n",i,BytesToInt(prevElementHeaders[i].data.Key))
			prevElementHeaders[i].levels[i] = nextElementHeaders[i]
		}
		//if nextElementHeaders[i]!=nil{
		//	fmt.Printf("i=%d 每层的nextElementHeaders[i] key=%d \n",i,BytesToInt(nextElementHeaders[i].data.Key))
		//
		//}
	}
}

/*
	快速比较
		使用分数
		当前节点 cur 与 参数key score比较
		 1: cur > key
		 0: cur = key
		 -1 cur < key
*/
func compare(score float64, key []byte, cur *Element) int {
	if score == cur.Score {
		// 分数相同 则需要全比较
		return bytes.Compare(cur.data.Key, key)
	}

	if cur.Score > score {
		return 1
	} else {
		return -1
	}
}

/*
	计算分数
		对于key 对key的前8位进行计算
这里计算一个分数值，用来加速比较。
	举个例子：aabbccddee和 aabbccdeee，如果用 bytes的 compare，需要比较到第8个字符才能算出大小关系，如果引入 hash，对前8位计算出一个分数值，比较起来就会很快了
*/
func calScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)
	if l > 8 {
		l = 8 // 为什么是8？
	}
	// ???
	for i := 0; i < l; i++ {
		u := uint64(64 - 8 - i*8)
		hash |= uint64(key[i] << u)
	}
	score = float64(hash)
	return
}

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
			fmt.Printf("%d.%d ->", BytesToInt(cur.data.Key), BytesToInt(cur.data.Value))
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
				compare := compare(calScore(preElement.data.Key), preElement.data.Key, cur)
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
