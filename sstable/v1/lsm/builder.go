package lsm

import (
	//"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/corekv/sstable/v1/file"
	"github.com/hardcore-os/corekv/sstable/v1/pb"
	"github.com/hardcore-os/corekv/sstable/v1/utils"
	"os"
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
	estimateSz     int64    // 当前block预估数据大小
	data           []byte   // 当前block所有相关数据 （kv_data、offsets、offsets_len、checksum、checksum_len)
	entryOffsets   []uint32 // 每个kv数据在data中的offset
	offsetLen      uint16   // offsets数组的长度 2字节
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
func (tb *tableBuilder) flush(tableName string) (t *Table, err error) {
	// 序列化当前block
	tb.finishCurBlock()
	t = &Table{fid: utils.FID(tableName)}
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

	// data拷贝到buf 遍历blocklist
	for i := 0; i < len(tb.blockList); i++ {
		block := tb.blockList[i]
		buf = append(buf, block.data...)
	}

	// index相关拷贝到buf
	buf = append(buf, index...)
	buf = append(buf, utils.U32ToBytes(uint32(indexLen))...)
	buf = append(buf, indexCheckSum...)
	buf = append(buf, utils.U32ToBytes(uint32(indexCheckSumLen))...)

	// 创建sstable对象
	ssTable := file.OpenSSTable(&file.Options{
		FileName: tableName,
		//Dir:      lm.opt.WorkDir,
		Flag: os.O_CREATE | os.O_RDWR,
		//MaxSz:    int(bd.size)
	})
	t.sst = ssTable

	// 调用sstable方法 将数据放入sstable mmap中data中（通过分配内存 然后拷贝的方式）  刷盘
	// buf拷贝到mmap.Data
	// 同步刷盘还是异步刷盘？
	dst, err := ssTable.Bytes(0, len(buf))
	copy(dst, buf)
	return t, nil
}

type Index struct {
}

/*
	在flush时调用 将跳表上数据一个个加到builder的一个个block中
*/
const BlockMaxSize = 0

func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	entryOffsetsDataLen := (len(tb.curBlock.entryOffsets)+1)*4 + 8 + 4

	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6) + int64(entryOffsetsDataLen) + int64(len(e.Key)) +
		int64(e.EncodedSize())

	return tb.curBlock.estimateSz > BlockMaxSize
}
func (tb *tableBuilder) add(e *utils.Entry) {
	key := e.Key
	val := utils.ValueStruct{
		//Meta:     e.Meta,
		Value:    e.Value,
		ExpireAt: e.ExpireAt,
		//Version:  e.Version,
	}
	// 当前block大小是否到达限制
	// 需要改变判断是否达到限制的条件
	if tb.tryFinishBlock(e) {
		tb.finishCurBlock()
		// 开辟新的block
		tb.curBlock = &Block{
			// 根据参数 创建预估block的大小的字节数组
			data: make([]byte, tb.opt.EveryBlockSize),
		}
	}

	// 否 计算hash(key)
	keyHash := keyHash(utils.ParseKey(key))
	tb.keyHashes = append(tb.keyHashes, keyHash)
	// 计算block 最大版本号（即sst的最大version 不断比较
	// 需要解析key 解析出key的版本号 在Key后面追加时间戳作为版本号（要求在上一次 放入key时就将key处理好）
	// 更新当前table(sst文件 最大的版本号）
	if version := utils.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
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

	// 将header diffkey（）放入当前block的data字节数组
	tb.append(h.encode())
	tb.append(diffKey)
	// 将val转为[]byte 放入data中
	// 计算val需要分配的字节数并进行分配
	dst := tb.allocate(int(val.EncodedSize()))
	// 编码val后放到data中
	val.EncodeValue(dst)

}

/*
 计算当前key与basekey不同的部分（简单遍历整个key 然后）
*/
func (b *Block) calDiffKey(key []byte) []byte {
	var i int
	// 遍历 找到不同的就break 返回
	for i = 0; i < len(key); i++ {
		if key[i] != b.baseKey[i] {
			break
		}
	}
	// 返回当前key与basekey不同的部分（i及之后的）
	return key[i:]
}

/*
	序列化当前block
*/
func (tb *tableBuilder) finishCurBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// 是 计算当前block的各项信息 序列化(指将各种数据转为[]byte 放入data)当前block（放入block list)
	entryOBytes := utils.U32SliceToBytes(tb.curBlock.entryOffsets)

	entryOffsetsLen := len(tb.curBlock.entryOffsets)
	// 按照4字节存
	entryOLenBytes := utils.U32ToBytes(uint32(entryOffsetsLen))

	tb.append(entryOBytes)
	tb.append(entryOLenBytes)
	// 计算校验和 checkSum是8字节 uint64的 -> []byte
	// 确定是对哪些部分计算校验和？ 所有数据（kv+entryoffsets）还是只有entryoffsets数据？ 看着应该是所有数据
	checkSum := tb.calCheckSum(tb.curBlock.data[:tb.curBlock.end])

	tb.append(checkSum)
	tb.append(utils.U32ToBytes(uint32(len(checkSum))))
	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil

	return
}

func keyHash(key []byte) uint32 {
	return utils.Hash(key)
}

/*
	将数据放入tb的curBlock的data中
*/
func (tb *tableBuilder) append(data []byte) {
	dstBuf := tb.allocate(len(data))
	// data拷贝到dstBuf
	copy(dstBuf, data)
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
	curBlock := tb.curBlock
	// 判断当前data还剩的size 是否需要扩容
	if len(curBlock.data[curBlock.end:]) < need {
		// 需要扩容
		newSize := 2 * len(curBlock.data)
		if curBlock.end+need > newSize {
			// 扩容的还不够
			newSize = curBlock.end + need
		}
		// 扩容
		tmp := make([]byte, newSize)
		// curBlock.data -> tmp
		copy(tmp, curBlock.data)
		curBlock.data = tmp
	}
	// 不需要
	curBlock.end += need
	return curBlock.data[curBlock.end-need : curBlock.end]
}

func (tb *tableBuilder) calCheckSum(data []byte) []byte {
	// 为什么不直接返回uint32 然后转[]byte？
	return utils.U64ToBytes(utils.CalCacheSum(data))

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
	prevOverlap  uint16 // 迭代器中上一个key的相对于当前block的覆盖度（与basekey相同的长度）
	it           *utils.Item
}

func (bi *blockIterator) Valid() bool {
	return bi.key != nil
}
func (bi *blockIterator) Rewind() {}
func (bi *blockIterator) Item() utils.Item {
	return *bi.it
}
func (bi *blockIterator) Close() error {
	return nil
}
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
	var entryEndOffset int // 当前idx的entry的endoffset
	// 如果是最后一个entry 则entryEndOffset为data长度
	if bi.idx+1 == len(bi.entryOffsets) {
		entryEndOffset = len(bi.data)
	} else {
		entryEndOffset = int(bi.entryOffsets[bi.idx+1])
	}
	// 其他 entryEndOffset是下一个entry的开始
	entryData := bi.data[startOffset:entryEndOffset]
	// 解析entry
	var header Header
	// 解析data中header数据到header结构体
	header.decode(entryData)
	// 比较当前header的overlap 与 当前迭代器所持的上一个key的prevoverlap
	if header.overlap > bi.prevOverlap {
		// 如果大于 则 当前header的覆盖度更高
		// 比如 basekey=abc  当前key是 abcxxx 则header overlap是3
		// prevOverlap是2 则上一个key是abxxxx  即覆盖 ab
		// 这里bi.key 应该还是上一个的key 还没更新 之后计算diffkey 再更新
		// 则这里 要是大于 则需要取出 当前key 应该是的前缀

		// bi.key[:bi.prevOverlap]先取出ab 再取出c 最终组成的当前key的前缀是 abc 再加diffkey就是完整的key
		bi.key = append(bi.key[:bi.prevOverlap], bi.baseKey[bi.prevOverlap:header.overlap]...)
		// 为什么这里不直接 使用bi.key = bi.basekey？ 因为像上一个key是 abxxx 其并没有完全覆盖basekey 甚至可能与basekey毫无关系
		//bi.key = bi.baseKey
	}
	// 如果小于 则说明当前key 比 上一个key 与basekey覆盖度更小 则在下面更新key时 取当前的overlap + diffkey即可
	// 比如 当前key是 axxxx overlap 是 1 ，上一个是abxxx
	// 则 bi.key = append(bi.key[:header.overlap], diffKey...) 即 append(a,diffkey)

	// 更新prevOverlap
	bi.prevOverlap = header.overlap

	// 图中的expired_at 暂时忽略了？
	valueOffset := header.diff + headerSize
	diffKey := entryData[headerSize:valueOffset]
	// key = basekey + diffkey 是字节数组 则可以直接append
	bi.key = append(bi.key[:header.overlap], diffKey...)
	//bi.key = bi.baseKey + diffKey
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

/*
	编码
*/
func (h Header) encode() []byte {
	var b [4]byte // 这里声明4个字节 计算时预估的header长度是6字节（内存对齐？）
	*(*Header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}
