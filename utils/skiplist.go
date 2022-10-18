package utils

import (
	"bytes"
	"fmt"
	"math"

	"sync/atomic"
	"unsafe"

	_ "unsafe"
)

const (
	MaxNodeSize    = 0                  // 跳表节点最大的大小
	maxHeight      = 16                 // 跳表最大层高 （即 MaxLevel）
	heightIncrease = math.MaxUint32 / 3 // 随机层高增长因子
)

/*
	基于Arena支持CAS的跳表实现V1.0
	跳表的基础单元
*/
/*
	跳表节点 不再记录具体的值 记录key value的指针
	指针  具体的值放在arena上
	但是这个跳表的node 还是会记录每层指向的节点等

	node本身在arena上申请
	node节点里面的value key也在arena上申请
*/

/*
	最外层写入的结构体 Entry -> node -> skiplist -> arena
*/
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64 // 做缓存时  有过期时间

	Meta    byte   // ? 后续kv分离的 vlog实现中使用 暂时忽略
	Version uint64 // 版本 ？
	//Offset       uint32 // ?
	//Hlen         int // Length of the header.
	//ValThreshold int64 // ?
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

type node struct {
	value uint64 // value在arena上的位置（offset） 和 value占据的大小(size) 将offset和size编码在一起 方便cas ？ 有并发操作

	keyOffset uint32 // 不可变 不需要加锁访问 key只会增加 不会释放 key相同 会更新value 访问不需要加锁
	keySize   uint16 // key的size （在arena上） 不可变 不需要加锁访问

	height uint16 // 节点所在层级 代表这个节点有多少个next指针 （如果是每层的最后一个节点？）

	tower [maxHeight]uint32 // 这个节点 在每层的next节点的地址/指针 value是节点指针 offset
}

// 获取value的size和offset 这个是对valuestruct编码后的 要获取到真正的 需要继续解 然后在arena上获取
func (n *node) getValueOffset() (valOffset uint32, valSize uint32) {
	// 原子操作
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

// 这里是解码value将其分为size和offset 属于node
func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)     // 低32位
	valSize = uint32(value >> 32) // 将valueSize 移到低32位 然后 使用uint32截断
	return
}

// 获取节点的Key的值 key不可变 所以不用原子操作 或者加锁
func (n *node) key(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

// 更新节点的value 原子操作 这里更新的是node.value 即已经经过两层编码的
func (n *node) setValue(value uint64) {
	atomic.StoreUint64(&n.value, value)
}

// 获取该节点在第h层的next指针（节点指针）
func (n *node) getNextOffset(h int) uint32 {
	return n.tower[h]
}

// 原子操作 更新节点在h层的next指针(用于每层增加/删除节点） ???
func (n *node) casNextOffset(h int, oldNode, newNode uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], oldNode, newNode)
}

type SkipList struct {
	level int32 // 跳表当前的层数
	//header *Element // 头元素 串起整个链表
	headOffset uint32 // 跳表头节点 在Arena的offset
	arena      *Arena // 每个跳表 的 内存管理器
	maxLevel   int    // 跳表最大高度
}

func NewSkipList(arenaSize int64) *SkipList {
	arena := NewArena(arenaSize)
	headNode := newNode(arena, nil, ValueStruct{}, maxHeight)
	headOffset := arena.getNodeOffset(headNode)
	return &SkipList{
		arena:      arena,
		level:      1,
		maxLevel:   maxHeight,
		headOffset: headOffset,
	}
}

/*
	在跳表中找当前key应该插入的地方
	满足 before.Key < key < next.key
	param:
		key
		before 当前key插入的前一个节点的offset
		level 当前所处的层
	return:
		before的offset和next的offset
	用于PUT
*/
func (s *SkipList) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	// 一直循环当前层
	for {
		// 获取到当前层before的next
		beforeNode := s.arena.getNode(before)
		nextNodeOffset := beforeNode.getNextOffset(level)
		nextNode := s.arena.getNode(nextNodeOffset)
		// 如果next==nil则是当前层链表末尾了 则key放在末尾即可
		if nextNode == nil {
			return before, nextNodeOffset
		}
		// 比较next.key和key
		nextNodeKey := nextNode.key(s.arena)
		cmp := compareKey(key, nextNodeKey)
		// ==0 则需要更新
		if cmp == 0 {
			return nextNodeOffset, nextNodeOffset
		}
		// <0 满足before.Key < key < next.key 找到了 返回
		if cmp < 0 {
			return before, nextNodeOffset
		}
		// >0 则key>next.key 继续向后找
		before = nextNodeOffset
	}
}

/*
	target:
		给定key找跳表中距离该值最近的节点（先找到相等的） 用于Get
		找key两边的最近节点
	params:
		key
		less:
			true returnNode.key < key 找key的左边
			false returnNode.key > key 找key的右边
		allowEqual:
			是否允许key = returnNode.key
	return:
		node:找到的节点
		bool: 是否找到相等的

	key > next.key 本层找
	key = next.key
	key < next.key 向下找
*/
func (s *SkipList) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	// 层层遍历
	// 获取一开始的头结点
	x := s.getHead()
	// 获取当前层级
	level := (s.getHeight() - 1)

	// 开始循环
	for {
		// 当前层当前节点的next节点
		next := s.getNext(x, level)
		// 但每次向下 x是从上一层的最后一个节点的这一层的节点的开始 如图
		/*
			   ⇢
			n1————n2
			|     | ⇣
			n1————n2————n3

		*/
		// next是当前层的最后一个 则继续向下层
		if next == nil {
			if level > 0 {
				level--
				continue
			}
			// 最后一层 且链表末尾 到这一步 是找的值大于跳表中最大的
			// 如果less=false 找右边的  即比key更大的 是找不到的
			if !less {
				return nil, false
			}
			if x == s.getHead() {
				return nil, false
			}

			// less=true 最后一层 链表末尾 则找左边 即x
			return x, false
		}

		// 获取next.key
		nextKey := next.key(s.arena)
		// 比较next.key 与 key
		cmp := compareKey(key, nextKey)
		// >0 则 key>next.key>x.key 这层继续遍历 x是每层的当前节点 一开始是从头节点开始的

		if cmp > 0 {
			x = next
			continue
		}
		// 如果 ==0 则key == next.key > x.key
		if cmp == 0 {
			// 判断是否允许相等
			if allowEqual {
				return next, true
			}

			// less = false 找key右边的 即 next.next
			if !less {
				// 这里找最下面一层的原因是 可能当前层的next.next==nil 且当前层 不一定是右边距离最近的
				return s.getNext(next, 0), false
			}
			// less=true 找key左边的 且要是距离最近的 继续向下层循环 因为下层可能还有距离更近的（这里如果有getBefore会更好点 但不是双向链表 是否可以优化成双向链表？）
			if level > 0 {
				level--
				continue
			}
			// less=true 当遍历到最后一层 且x（当前值）是头节点(指当前跳表的总的头节点） 则跳表是空的
			if x == s.getHead() {
				return nil, false
			}
			// 返回最后一层的x 即next的左边最近 less=true
			return x, false
		}

		// 如果 <0 则 key < next.key 但是与x.key的大小不确定 向下层找

		// 继续向下找
		if level > 0 {
			level--
			continue
		}
		// 最后一层 less=false 找右边 则是next
		if !less {
			return next, false
		}
		if x == s.getHead() {
			return nil, false
		}
		// 最后一层 找到左边 less=true 即x
		return x, false
	}

}

/*
	获取跳表头节点
*/
func (s *SkipList) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

/*
	获取跳表当前层，当前节点的下一个节点
*/
func (s *SkipList) getNext(nd *node, level int32) *node {
	return s.arena.getNode(nd.tower[level])
}

/*
	插入节点到跳表中 利用findSpliceForLevel
	找到key应该放的位置 然后把前后节点连接起来
	params:
		待插入的节点

*/
func (s *SkipList) Add(e *Entry) {
	var key []byte
	var v ValueStruct
	key, v = e.Key, ValueStruct{
		Meta:     e.Meta,
		Value:    e.Value,
		ExpireAt: e.ExpiresAt,
		Version:  e.Version,
	}
	// 获取跳表高度
	listHeight := s.getHeight()

	// 定义每层 待插入节点 前后节点数组
	// 这里下面都要层上一层开始 所以+1？
	var prevList [maxHeight + 1]uint32
	var nextList [maxHeight + 1]uint32
	// 从当前层的上一层开始
	prevList[listHeight] = s.headOffset

	// 从最高层开始遍历 使用findSpliceForLevel找相同的
	for i := listHeight - 1; i >= 0; i-- {

		// 如果方法返回的前后节点相同 则是key相同的  需要更新
		// 不是从每一层的头节点开始 这里从每一个上一层的前一个节点开始
		prevList[i], nextList[i] = s.findSpliceForLevel(key, prevList[i+1], int(i))
		if prevList[i] == nextList[i] {
			// 找到相同的key 则需要更新 这里更新不需要CAS吗？ 是因为只更新value 不更新key？ 但是value会更新覆盖吧
			// 放入arena（在arena上新放入）
			valueOffset := s.arena.putValue(v)
			// 编码value (valueoffset和valuesize编码在一起）
			encValue := encodeValue(valueOffset, v.EncodeSize())
			// 编码后的Value放入node中（只是让key指向新value)
			prevNode := s.arena.getNode(prevList[i])
			prevNode.setValue(encValue)
		}
		// 没找到相同的 则需要插入

		// 随机出层高
		randHeight := s.randomHeight()

		// 构造节点
		insertNode := newNode(s.arena, key, v, randHeight)

		// 使用CAS更新跳表的层高参数
		// 再次获取listHeight 获取最新的 可能在这个Add的时候 已经有新的add 改变了height 是否会影响之前的？
		listHeight = s.getHeight()
		for int32(randHeight) > listHeight {
			// 一直更新跳表高度 直到和randHeight一样
			if atomic.CompareAndSwapInt32(&s.level, listHeight, int32(randHeight)) {
				break
			}
			listHeight = s.getHeight()
		}
		// 从第0层开始插入跳表 但是这里注意 整个方法并没有一开始就加锁 则可能造成并发写冲突
		for i := 0; i < randHeight; i++ {
			// 这里需要CAS处理 （不考虑分布式） 如果有并发操作 则需要更新数据（是否会造成更新丢失？）
			// 每层都需要在循环中 一直CAS处理
			for {

				// 正常如下 使用CAS
				insertNode.tower[i] = nextList[i]
				prevNode := s.arena.getNode(prevList[i])
				//prevNode.tower[i] = s.arena.getNodeOffset(insertNode)
				// 将prevNode.tower[i] 由next[i] 更换为insertNode
				// 直到prevNode.tower[i] == next[i] 才将next[i] 更换为s.arena.getNodeOffset(insertNode)
				// 问题：如果这里已经被改变 则永远循环？ 看下面
				if prevNode.casNextOffset(i, nextList[i], s.arena.getNodeOffset(insertNode)) {
					break
				}
				// 如果能够在上面break 最好 如果不能 则可能有并发了 需要再次获取key的插入位置

				prevList[i], nextList[i] = s.findSpliceForLevel(key, prevList[i], i)
				if prevList[i] == nextList[i] {
					// 之前检查过是不等的 这里相等了 说明已经有节点插入了 并发了 则更新
					// 这里检查是？？？
					// 两个A,B协程对于相同key并成冲突写 都是从第0层开始插入 第一个A来的时候不会到这步 后面B来的时候 走到这边 将值更新为B后返回，
					// A协程还在继续插入 随机层高也是A的 但值是B的
					AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
					// 放入arena（在arena上新放入）
					valueOffset := s.arena.putValue(v)
					// 编码value (valueoffset和valuesize编码在一起）
					encValue := encodeValue(valueOffset, v.EncodeSize())
					// 编码后的Value放入node中（只是让key指向新value)
					prevNode := s.arena.getNode(prevList[i])
					prevNode.setValue(encValue)
					// 更新后跳出
					return
				}
				// 同时这里 上面CAS 比较  prevNode.tower[i] != next[i] 到这里 重新计算key应该插入的位置
				// 如果是两个协程1，2 处理不同的key 并发插入 ，1插入后，2的不相等了 要重新找key应该插入的位置
				// 这里就是 重新计算后 没有相同key的情况 不用更新 重新找到新的要插入的地方 继续插入---不同key的并发插入问题
			}
		}

	}
}

/*
	跳表查找
	params:
		key
	return
		value

	是否有读写冲突？获取到最终的？
*/
func (s *SkipList) Get(key []byte) ValueStruct {
	// 找key的下一个或等于key的
	near, _ := s.findNear(key, false, true)
	if near == nil {
		// 没有找到比key大的 也没找到和key相等的
		return ValueStruct{}
	}
	// 找到了大于等于key的
	nextKey := s.arena.getKey(near.keyOffset, near.keySize)
	// 比较key与nextKey
	if !sameKey(key, nextKey) {
		// 不相等 则没找到
		return ValueStruct{}
	}

	// 找到相同的key 则获取value
	valueOffset, valueSize := near.getValueOffset()
	value := s.arena.getValue(valueOffset, valueSize)
	// ???
	//value.Version = ParseTs(nextKey)
	return value
	// 不明白 为什么不直接  near,sameBool := s.findNear(key, false, true)
	// if !sameBool{ValueStruct{}}
}

/*
	判断两个字节数组是否相等
*/
func sameKey(key1, key2 []byte) bool {
	if len(key1) != len(key2) {
		return false
	}
	return bytes.Equal(key1, key2)
}

func newNode(arena *Arena, key []byte, v ValueStruct, randHeight int) *node {

	keyOffset := arena.putKey(key)
	valueOffset := arena.putValue(v)
	valueSize := v.EncodeSize()
	val := encodeValue(valueOffset, valueSize)

	// 给node分配内存
	nodeOffset := arena.putNode(randHeight)

	// 获取node所在
	node := arena.getNode(nodeOffset)
	node.value = val
	node.keySize = uint16(len(key))
	node.keyOffset = keyOffset
	node.height = uint16(randHeight)
	return node
}

/*
	获取当前跳表的高度
*/
func (s *SkipList) getHeight() int32 {
	return atomic.LoadInt32(&s.level)
}

/*
	生成随机层高
*/
func (s *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}
	return h
}

// FastRand is a fast thread local random function.
//go:linkname FastRand runtime.fastrand
func FastRand() uint32

/*
	并发删除key（删除节点）
*/

/*xi
比较两个key

return
	==0 key1 == key2
	> 0 key1 > key2
	< 0 key1 < key2

*/
func compareKey(key1, key2 []byte) int {
	// 这里为什么key长度小于8字节就panic? 是小于8的不会到这步吗？ 不再计算score？
	CondPanic((len(key1) <= 8 || len(key2) <= 8), fmt.Errorf("%s,%s < 8", string(key1), string(key2)))
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}

// Panic 如果err 不为nil 则panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// --------------------------------------------------------------

/*
	实现跳表迭代器 Iterator
		参考Leveldb?
*/
type SkipListIterator struct {
	list *SkipList // 跳表
	n    *node     // 当前节点
}

func (s *SkipList) NewSkipListIterator() *SkipListIterator {
	return &SkipListIterator{
		list: s,
	}
}

/*
	找到第一个
*/
func (si *SkipListIterator) Rewind() {
	si.SeekToFirst()
}

func (si *SkipListIterator) hasNext() bool {
	si.Next()
	return si.Valid()
}

/*
	当前节点的数据
*/
func (si *SkipListIterator) Item() *Entry {
	return &Entry{
		Key:       si.Key(),
		Value:     si.Value().Value,
		ExpiresAt: si.Value().ExpireAt,
		Meta:      si.Value().Meta,
		Version:   si.Value().Version,
	}
}

/*
	关闭迭代器
	释放迭代器占有的资源
	暂不实现
*/
//func (si *SkipListIterator)Close() error {
//
//}

/*
	当前节点是否合法
*/
func (si *SkipListIterator) Valid() bool {
	return si.n != nil
}

/*
	获取当前节点的key
*/
func (si *SkipListIterator) Key() []byte {
	return si.n.key(si.list.arena)
}

/*
	当前节点的值
*/
func (si *SkipListIterator) Value() ValueStruct {
	return si.list.arena.getValue(si.n.getValueOffset())
}

/*
	当前节点的值（valsize+valoffset）
*/
func (si *SkipListIterator) ValueUint64() uint64 {
	return atomic.LoadUint64(&si.n.value)
	//return si.n.value
}

/*
	当前n的后一个 大于的
*/
func (si *SkipListIterator) Next() {
	si.n, _ = si.list.findNear(si.n.key(si.list.arena), false, false)
}

/*
	当前n的前一个 小于的
*/
func (si *SkipListIterator) Prev() {
	si.n, _ = si.list.findNear(si.n.key(si.list.arena), true, false)
}

/*
	找目标值的后一个（大于等于）
*/
func (si *SkipListIterator) Seek(target []byte) {
	si.n, _ = si.list.findNear(target, false, true)
}

/*
	找目标值的前一个（小于等于）
*/
func (si *SkipListIterator) SeekForPrev(target []byte) {
	si.n, _ = si.list.findNear(target, true, true)
}

/*
	找跳表中的第一个节点(除开头结点的）
*/
func (si *SkipListIterator) SeekToFirst() {
	si.n = si.list.getNext(si.list.getHead(), 0)
}

/*
	找跳表中的最后一个节点
*/
func (si *SkipListIterator) SeekToLast() {
	si.n = si.list.findLast()
}

/*
	跳表的最后一个节点
		最后一个节点的含义是？ 一直遍历跳表 直到最后吗？ 是否是第0层的最后一个？ 是
		使用层级遍历 从最高层遍历（最高层的节点最少 向下遍历 则每层遍历的节点都不多，相较于从第0层的开头开始遍历 可能更快

*/
func (s *SkipList) findLast() *node {
	// 空跳表
	if s.getNext(s.getHead(), 0) == nil {
		return nil
	}
	// 获取当前跳表层高
	height := s.getHeight() - 1
	curNode := s.getHead()
	for {
		next := s.getNext(curNode, height)
		if next != nil {
			curNode = next
			continue
		}
		if height == 0 {
			// 这里打印下getHead的值看看
			if curNode == s.getHead() {
				return nil
			}
			return curNode
		}
		height--
	}
}

func (s *SkipList) Draw(align bool) {
	// 逐行查找 从最高层开始 每层找不到 则向下
	for i := s.level - 1; i >= 0; i-- {
		preElementOffset := s.headOffset // 每一层从头开始
		// 获取到node
		preElement := s.arena.getNode(preElementOffset)

		// 找 每层的 头 尾 中间与key比较
		for cur := preElement.tower[i]; cur != 0; cur = preElement.tower[i] {
			curNode := s.arena.getNode(cur)
			key := s.arena.getKey(curNode.keyOffset, curNode.keySize)
			valOffset, valSize := decodeValue(curNode.value)
			value := s.arena.getValue(valOffset, valSize).Value
			fmt.Printf("%s.%s ->", string(key), string(value))
			preElement = curNode
			continue
		}
		fmt.Println()
	}
}
