package utils

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

/*
	实现Arena基本操作
*/
const (
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
)

/*
	 内存分配管理 单元 （只分配 不释放）
	每次先向操作系统申请一大块内存空间，之后跳表节点需要的内存就在arena上分配
	且 一个跳表关联一个arena
	一个跳表中之后就记录 节点在arena上的地址/offset之类的
*/
type Arena struct {
	n   uint32 // Arena中已经分配出去的内存大小，即offset
	buf []byte // Arena申请的内存空间
}

/*
	新建Arena
	一开始要向操作系统申请N大小的内存
*/
func NewArena(n int64) *Arena {
	return &Arena{
		n:   1, // 从1开始？
		buf: make([]byte, n),
	}
}

/*
	在Arena上分配内存
	params:
		size 即要在arena上分配多少内存？
	return:
		返回在arena上的offset 即分配的内存的第一个byte开始的位置

	这个总体应该是不支持并发分配内存
*/
func (s *Arena) allocate(size uint32) uint32 {
	// 当前arena的内存是否够？ （当前+要分配的） 不够需要扩大arena的大小 一般是2倍
	// 假设可以放下 原子的 支持并发申请内存
	offset := atomic.AddUint32(&s.n, size)

	// 上面是假设 但是要真的确定还能放下吗？
	// 且这里要提前预测 还能放下下一个节点吗？ 如果不行 就尽早扩容
	if len(s.buf)-int(offset) < MaxNodeSize {
		// 放不下  则需要扩容arena
		growUnit := uint32(len(s.buf))
		// 但是扩容的大小有上限 小于 2的30次方(1G)
		if growUnit > 1<<30 {
			growUnit = 1 << 30
		}
		// 如果要分配的还大于 growUnit 则 分配size的 （但是 若size大于1G嘞？尽量满足分配）  应该是对arena有做限制 之后如果到达阈值则转为immemtable
		if growUnit < size {
			growUnit = size
		}

		// 非原子 不支持并发扩容
		newBuf := make([]byte, int(growUnit)+len(s.buf))
		// 将原数据拷贝到新buf中 修改arena的指针 让gc来处理原来的byte数组
		copy(newBuf, s.buf)
		// 判断 当前
		s.buf = newBuf
	}

	// 最终返回
	return offset - size
}

// ------------------------接口封装-----------------------------------------------

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}

func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}

/*
	在Arena中放置一个Node节点
	申请一个NODE节点的空间

	params:
		当前节点的层高

	这个方法实质是为 一个node空出一片空间  实质还并没有node放进去
	实质是指针
*/
func (s *Arena) putNode(height int) uint32 {
	// 一个node节点的大小 这里会直接按照maxLevel来计算 nodeSize = 8+4+2+2 + maxLevel*4
	// 但实际并是每个节点都在最高层 不需要分配那么多内存
	nodeSize := int(unsafe.Sizeof(node{}))

	uint32Size := int(unsafe.Sizeof(uint32(0)))

	unusedSize := (maxHeight - height) * uint32Size

	//shouldSize := roundUP(uint32(nodeSize-unusedSize), 8)

	//roundUP(shouldSize,8)
	// 这里要 + nodeAlign是因为 要给足够的size 下面要做内存对齐 最大也只会加nodeAlign 且不会再改变arena的n 则需要这里的size足够大
	n := s.allocate(uint32(nodeSize - unusedSize + nodeAlign)) // offset

	// 进行内存对齐 为了内存操作更快速  这里因为key value不做内存对齐 则每次去分配的时候 不一定是对齐的 则需要分配后做内存对齐
	// 而不是想CSAPP中那样在分配的时候 分配对齐的size 或者 在分配的时候要简单对齐 之后再做对齐

	//finalNodeOffset:=roundUP(n,8)
	// 这里也可以用roundUP
	// 允许在allocate中实质分配到内存 有部分空白
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)

	return m
}

/*
	通过Node的offset获取到node
		返回指针
*/
func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	//b := s.buf[offset] // ?? 这里获取到node开始的指针 这里不能把 s.buf[offset]拿出来 不然就出错 为什么？
	//fmt.Println("b=",b)
	// []byte转node指针
	//return (*node)(unsafe.Pointer(&b))
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

/*
	获取node的offset
*/
func (s *Arena) getNodeOffset(nodeV *node) uint32 {
	if nodeV == nil {
		// 空指针
		return 0
	}
	//获取某个节点,在 arena 当中的偏移量
	//unsafe.Pointer等价于void*,uintptr可以专门把void*的对于地址转化为数值型变量
	//fmt.Println("uintptr(unsafe.Pointer(nodeV))=",uintptr(unsafe.Pointer(nodeV)))
	//fmt.Println("uintptr(unsafe.Pointer(&s.buf[0]))=",uintptr(unsafe.Pointer(&s.buf[0])))
	return uint32(uintptr(unsafe.Pointer(nodeV)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

/*
	内存对齐--- 向上取整
	params:
		x：需要进行内存对齐的地址/或size
		n : 向上取整的字节数
*/

func roundUP(x uint32, n uint32) uint32 {

	return n * ((x + n - 1) / n)
}

/*
	在arena上分配node的key
		不做内存对齐？ 不做内存对齐
*/
func (s *Arena) putKey(key []byte) uint32 {
	keySize := uint32(len(key))
	offset := s.allocate(keySize)
	buf := s.buf[offset : offset+keySize]

	copy(buf, key)
	return offset
}

/*
	获取key 不内存对齐
*/
func (s *Arena) getKey(keyOffset uint32, keySize uint16) []byte {
	return s.buf[keyOffset : keyOffset+uint32(keySize)]
}

/*
	在arena上分配node的value
		在Node的value是将 value的offset和size编码在一起 node中的value应该指ValueStruct 整个

	这里将value扩展成结构体 具体的编解码见下面

	内存对齐？
*/
func (s *Arena) putValue(value ValueStruct) uint32 {
	// 编码数据
	size := value.EncodeSize()
	offset := s.allocate(size)
	value.EncodeValue(s.buf[offset:])
	return offset
}

/*
	获取value
*/
func (s *Arena) getValue(valOffset, valSize uint32) ValueStruct {
	var result ValueStruct
	result.DecodeValue(s.buf[valOffset : valOffset+valSize])
	return result
}

type ValueStruct struct {
	Meta     byte
	Value    []byte
	ExpireAt uint64 // 做缓存时  有过期时间
	Version  uint64
}

/*
	编码ValueStruct Size (value的size + ExpireAt的size)
*/
func (e *ValueStruct) EncodeSize() uint32 {
	sz := len(e.Value)
	// 计算ExpireAt 的size
	enc := sizeVarint(e.ExpireAt)
	//
	return uint32(sz + enc)
}

/*
	计算uint64 占用内存的方法
		因为下文EncodeValue中的PutUvarint就是用varint编码来存储数据的, 因此这里计算了varint编码所占的内存大小, 以保证2者一致.
		varint简单来说就是每字节存7bits, 头上补个1, 好处是如果x是很小的数, 高位都是0, 那么只需存低位的值, 1字节就够了, 相比uint64原来是8字节的, 大大缩减了存储空间
*/

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

/*
	编码 Value和expire的值
		params:
			b：将Value的值和expireAt编码进字节数组中
		return:
			返回value和expireAt的size
*/
func (e *ValueStruct) EncodeValue(b []byte) uint32 {
	// 将uint64值 编码到字节数组中 返回uint64值（即e.ExpireAt)的大小
	// 这样编码 减少uint64占用的内存大小
	sz := binary.PutUvarint(b[:], e.ExpireAt)
	// 将e.Value 放到b数组中 expireAt的后面
	n := copy(b[sz:], e.Value)
	return uint32(n + sz)
}

/*
	解码
		params:
			buf: 放置value的值和expireAt的数组 将数据从数组中解码出来
*/
func (e *ValueStruct) DecodeValue(buf []byte) {
	var sz int
	// 从buf中将Uint64解码出来
	e.ExpireAt, sz = binary.Uvarint(buf)
	e.Value = buf[sz:]
}

/*
	将valueStruct的size和Offset拼接在一起 成为node中的value
*/
func encodeValue(valueOffset uint32, valueSize uint32) uint64 {
	return uint64(valueSize)<<32 | uint64(valueOffset)
}
