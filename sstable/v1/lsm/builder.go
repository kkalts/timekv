package lsm

import (
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/corekv/sstable/v1/file"
	"github.com/hardcore-os/corekv/sstable/v1/pb"
	"github.com/hardcore-os/corekv/sstable/v1/utils"
	"sort"
	"unsafe"
)

/*
	将跳表数据/其他SST文件数据 序列化
	策略模式
		tableBuilder 具体的策略
		这里似乎并没有使用策略模式
	可以提供不同的存储格式

*/
type tableBuilder struct {
	opt        *Options
	blockList  []*Block
	curBlock   *Block
	maxVersion uint64
	keyHashes  []uint32
	keyCount   uint32 // 整个builder 整个sst的key数量
}

type Block struct {
	data           []byte   // 当前block所有相关数据 （kv_data、offsets、offsets_len、checksum、checksum_len)
	entryOffsets   []uint32 // 每个kv数据在data中的offset
	offsetLen      uint16   // offsets数组的长度
	checkSum       []byte   // 对data entryOffsets offsetLen 计算一个校验和
	checkSumLen    uint16   // 校验和的长度
	offset         uint16   // 当前block的offset
	baseKey        []byte   // 当前block的第一个key
	end            int      // 当前block的data的结束位置（data的大小？
	kvDataStartPos int
}
type Header struct {
	overlap uint16 // 与base key相同的部分的长度（base key是当前block的第一个key)
	diff    uint16 //  与base key不同部分的长度
}
type BuildData struct {
	blockList []*Block
	index     []byte
}

/*
	序列化builder（内存数据） 序列化为sst文件 存储到硬盘
*/
func (tb *tableBuilder) flush() {
	// 序列化当前block
	tb.finishCurBlock()

	// 构建布隆过滤器 将当前builder的所有kv数据都放入布隆过滤器中
	filter := utils.NewBloomFilter(len(tb.keyHashes), tb.opt.BloomFalsePositive)
	for i := 0; i < len(tb.keyHashes); i++ {
		filter.Allow(tb.keyHashes[i])
	}
	// 构建索引 index有index_data index_len index的校验和
	// 遍历blocl list

	tableIndex := &pb.TableIndex{}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion

	// index的block_offsets
	var startBlockOffset uint32
	var offsets = make([]*pb.BlockOffset, 0)
	for i := 0; i < len(tb.blockList); i++ {
		offset := &pb.BlockOffset{}
		// 每个block的base key offset len
		block := tb.blockList[i]
		offset.Key = block.baseKey
		offset.Len = uint32(block.end)
		offset.Offset = startBlockOffset
		offsets = append(offsets, offset)
		startBlockOffset += uint32(block.end)
	}
	tableIndex.Offsets = offsets
	tableIndex.BloomFilter = filter.FilterBitMap()

	// 序列化整个index
	index, err := proto.Marshal(tableIndex)
	if err != nil {

	}
	indexLen := len(index)

	// 计算索引的校验和
	indexCheckSum := tb.calCheckSum(index)
	indexCheckSumLen := len(indexCheckSum)
	// 计算sst文件的整体大小

	// 将以上数据都放入一个大[]byte （data index)
	var buf = make([]byte, 0)

	// data拷贝到buf

	// index相关拷贝到buf

	// 创建sstable对象
	ssTable := file.OpenSSTable()

	// 调用sstable方法 将数据放入sstable mmap中data中（通过分配内存 然后拷贝的方式）  刷盘
	// buf拷贝到mmap.Data
	dst, err := ssTable.Bytes(0)
	copy(dst, buf)
}

type Index struct {
}

/*
	在flush时调用 将跳表上数据一个个加到builder的一个个block中
*/
const BlockMaxSize = 0

func (tb *tableBuilder) add(e *utils.Entry) {
	key := e.Key
	val := utils.ValueStruct{
		Meta:     e.Meta,
		Value:    e.Value,
		ExpireAt: e.ExpireAt,
		Version:  e.Version,
	}
	// 当前block大小是否到达限制
	// 需要改变判断是否达到限制的条件
	if len(tb.curBlock.data) >= BlockMaxSize {
		tb.finishCurBlock()
		// 开辟新的block
		tb.curBlock = &Block{
			// 根据参数 创建预估block的大小的字节数组
			data: make([]byte, tb.opt.EveryBlockSize),
		}
	}

	// 否 计算hash(key)
	keyHash := keyHash(key)
	tb.keyHashes = append(tb.keyHashes, keyHash)
	// 计算block 最大版本号（即sst的最大version 不断比较
	if e.Version > tb.maxVersion {
		tb.maxVersion = e.Version
	}
	// 如果是当前block的第一个项 则diffkey为basekey
	// 计算diffkey
	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.curBlock.calDiffKey(key)
	}

	// 计算header
	h := Header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	// 计算当前block的end_offset 写入list（当前entry写入后的 下一个entry的起始位置）
	// 这里end还没改变 则 这里是当kv数据在当前data中的开始offset 放入数组中(在这里是这样 后续end会是data的end位置)
	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	// 将header diffkey（）放入当前block的kv_data字节数组
	tb.append(h)
	tb.append(diffKey)
	// 将val转为[]byte 放入data中

	// 更新offsets offset_len
	tb.allocate(val)

}
func (b *Block) calDiffKey(key []byte) []byte {

}

/*
	序列化当前block
*/
func (tb *tableBuilder) finishCurBlock() {
	// 是 计算当前block的各项信息 序列化(指将各种数据转为[]byte 放入data)当前block（放入block list)
	entryOBytes := U32SliceToBytes(tb.curBlock.entryOffsets)

	entryOffsetsLen := len(tb.curBlock.entryOffsets)
	entryOLenBytes := U32ToBytes(uint32(entryOffsetsLen))
	// 计算校验和
	checkSum := tb.calCheckSum(tb.curBlock.data)

	tb.append(entryOBytes)
	tb.append(entryOLenBytes)
	tb.append(checkSum)
	tb.append(U32ToBytes(uint32(len(checkSum))))
	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil

	return
}

func keyHash(key []byte) uint32 {

}
func U32SliceToBytes(data []uint32) []byte {

}

func U32ToBytes(data uint32) []byte {

}

/*
	将数据放入tb的curBlock的data中
*/
func (tb *tableBuilder) append(data []byte) []byte {

}

/*
	优化点：使用内存分配器

	params:
		need:需要分配的大小
	return:
		在block.data上分配好后将那段位置返回，使用时将数据复制到return的数组中即可 需要测试 可以这样操作吗？

	https://juejin.cn/post/6888117219213967368
	https://blog.csdn.net/weixin_44387482/article/details/119763558
*/
func (tb *tableBuilder) allocate(need int) []byte {

}

func (tb *tableBuilder) calCheckSum(data []byte) []byte {

}

/*
	block迭代器
*/
type blockIterator struct {
	data         []byte // 纯entry数据
	idx          int
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *Block

	it *utils.Item
}

func (bi *blockIterator) Valid() bool      {}
func (bi *blockIterator) Rewind()          {}
func (bi *blockIterator) Item() utils.Item {}
func (bi *blockIterator) Close() error     {}
func (bi *blockIterator) Seek(key []byte) {
	startIndex := 0 // This tells from which index we should start binary search.

	// 二分查找
	foundEntryIdx := sort.Search(len(bi.entryOffsets), func(idx int) bool {
		// If idx is less than start index then just return false.
		if idx < startIndex {
			return false
		}
		bi.setIdx(idx)
		// 当前项的key >=key
		return utils.CompareKeys(bi.key, key) >= 0
	})
	bi.setIdx(foundEntryIdx)
}

// 让当前的迭代器持有当前block当前迭代到的kv数据
func (bi *blockIterator) setIdx(idx int) {
	bi.idx = idx
	// 当前kv项的开始
	startOffset := int(bi.entryOffsets[idx])

	// 设置basekey
	if len(bi.baseKey) == 0 {
		var header Header
		// 解析data中header数据到header结构体
		header.decode(bi.data)
		// basekey是第一项的diffkey
		bi.baseKey = bi.data[headerSize : headerSize+header.diff] // header.diff == 0
	}
	// 找到idx对应的kvdata
	var entryEndOffset int // 当前idx的entry的endoffset  如何计算？？？

	entryData := bi.data[startOffset:entryEndOffset]
	// 解析entry
	var header Header
	// 解析data中header数据到header结构体
	header.decode(entryData)
	// 图中的expired_at 暂时忽略了？
	valueOffset := header.diff + headerSize
	diffKey := entryData[headerSize:valueOffset]
	bi.key = bi.baseKey + diffKey
	val := &utils.ValueStruct{}
	val.DecodeValue(entryData[valueOffset:])
	bi.val = val.Value
	e := &utils.Entry{Key: bi.key}
	e.Key = bi.key
	e.Value = val.Value
	e.ExpireAt = val.ExpireAt
	e.Meta = val.Meta
	bi.it = &utils.Item{E: e}

}
func (bi *blockIterator) setBlock(b *Block) {
	bi.entryOffsets = b.entryOffsets
	bi.data = b.data[:b.kvDataStartPos]

}

const headerSize = uint16(unsafe.Sizeof(Header{}))

// Decode decodes the header.
func (h *Header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}
