package lsm

import (
	"bytes"
	_ "fmt"
	_ "github.com/golang/protobuf/proto"
	"github.com/hardcore-os/corekv/sstable/v1/file"
	"github.com/hardcore-os/corekv/sstable/v1/utils"
	"io"
	"os"
	"sort"
)

/*
	sst文件在内存中的句柄对象
*/
type Table struct {
	sst *file.SSTable
	//blocks []*Block
	//index 	*Index
	fid uint64 // sstable的编号 （每个sstable都有自己的编号）
	//lm *LeverManager // 类似上下文 参数反引用
}

/*
	创建一个table
	builder不空 将数据flush （序列化）
	builder为空 从sst文件中加载数据（初始化）
*/
func openTable(opt Options, tableName string, builder *tableBuilder) *Table {

	// 创建sst对象
	// 不管是新文件还是老文件，都与内存建立了映射 数据可以直接使用
	// 老文件 是否意味已经flush过？那么再次flush 是否就不应该从offset=0开始flush？
	// 老文件 用做检索 是否需要限制flush？ 只能在传参时注意？
	// 以及一个sst文件 会多次flush吗 应该不可以
	fid := utils.FID(tableName)
	t := &Table{fid: fid}
	var err error
	//t.sst = ssTable
	if builder != nil {
		// builder不为空 将builder序列化到sst文件 flush
		flushT, err := builder.flush(tableName)
		if err != nil {

		}
		t = flushT

	} else {
		ssTable := file.OpenSSTable(&file.Options{
			FileName: tableName,
			//Dir:      lm.opt.WorkDir,
			Flag: os.O_CREATE | os.O_RDWR,
			//MaxSz:    int(sstSize)
		})
		t.sst = ssTable
	}
	// builder为空 进行初始化 恢复.sst文件 加载Index到内存（sstable?
	// 从mmap的内存空间中 将原sst文件的索引解析到内存中
	err = t.sst.Init()
	if err != nil {

	}

	// 通过迭代器设置ssTable的maxKey
	//tableIterator := NewTableIterator() // 默认降序？为什么 需要在实现时降序实现 实现时根据配置来处理
	//
	//tableIterator.Rewind()
	//t.sst.SetMaxKey(tableIterator.Item().Entry().Key)

	return t
}

// 在openTable后的sst经过初始化 有了sst文件的index等数据 即可用于检索
/*
	检索的原理：检索当前sst文件的每个block
		1. 布隆过滤器判断是否存在（不存在 一定不存在 存在可能不存在）
		2. 索引找到base key 与当前key比较 使用二分查找
		一步步找到所在block
		3. 找到block后 先查找block cache

		sst文件 -> table -> block

		需要的迭代器：tableIterator blockI

		算法使用：二分查找
	params:
		key
		maxVs ?
	return

*/
func (t *Table) Search(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	if maxVs == nil {
		return nil, utils.ErrKeyNotFound
	}
	index := t.sst.Index()
	filterMap := index.BloomFilter
	bloomFilter := utils.NewBloomFilterForTable(filterMap)
	if !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}
	// 创建table的迭代器
	ti := NewTableIterator(&utils.Options{})
	ti.Seek(key)
	// 是否找到
	// 没找到
	if !ti.Valid() {
		return nil, utils.ErrKeyNotFound
	}
	// 找到了
	// 再次判断找到的key与当前key是否相同 以及解析时间戳 版本等
	// 查找的key的版本号 是否大于最大版本号 大于则更新 小于则没找到
	if len(key) != len(ti.Item().Entry().Key) {
		return nil, utils.ErrKeyNotFound
	}
	if bytes.Equal(utils.ParseKey(key), utils.ParseKey(ti.Item().Entry().Key)) {
		// 相等 判断当前key的版本号是否大于最大版本号 如果大于则更新 如果小于则不返回数据
		if curVersion := utils.ParseTs(ti.Item().Entry().Key); curVersion > *maxVs {
			*maxVs = curVersion
			return ti.Item().Entry(), nil
		}

	}
	//
	return nil, utils.ErrKeyNotFound
}

/*
	table迭代器
*/
type TableIterator struct {
	t        *Table
	opt      *utils.Options
	it       utils.Item
	blockPos int
	bi       *blockIterator
	err      error
}

func NewTableIterator(options *utils.Options) *TableIterator {
	return &TableIterator{
		opt: options,
	}
}
func (ti *TableIterator) Next() {

}
func (ti *TableIterator) Valid() bool {
	return ti.err != io.EOF // 如果没有的时候 则是EOF
}
func (ti *TableIterator) Rewind() {}
func (ti *TableIterator) Item() utils.Item {
	return ti.it
}
func (ti *TableIterator) Close() error {
	return nil
}

/*
	找目标值
*/
func (ti *TableIterator) Seek(key []byte) {
	blockOffsets := ti.t.sst.Index().Offsets
	// 进行二分查找 找到key可能在的block

	// 直接使用golang的二分
	idx := sort.Search(len(blockOffsets), func(i int) bool {
		if i == len(blockOffsets) {
			// 当i==len(blockOffsets) 在最后一个？还是找不到 key可能在最后一个block中
			return true
		}
		// GetKey > key 为什么不直接>=0 ?
		// 等于 退出条件是 key <= basekey
		return utils.CompareKeys(blockOffsets[i].GetKey(), key) > 0
	})
	// 找到 basekey > key  的第一个（最左值应该）
	if idx == 0 {
		// key必在第一个block 因为最左值 可以直接去对应的block中使用迭代器继续查找
		// 且 block的basekey<=key
		ti.SeekForBlock(idx, key)
	}
	// 否则 block[idx].basekey > key 则需要找idx-1的block
	// 这个block[idx-1].basekey < key 在idx-1的block中找
	// 在idx-1中没有 则key可能在idx中 key在最后一个block中 则需要在idx中找
	//
	ti.SeekForBlock(idx-1, key)

}

/*
	在block中找key
*/
func (ti *TableIterator) SeekForBlock(idx int, key []byte) {
	// 明确block
	ti.blockPos = idx
	// 在block cache中找block 这里暂时忽略

	blockOffset := ti.t.sst.Index().Offsets[idx]
	var block = &Block{}
	// 通过blockOffset从sst文件中获取到具体的block
	blockData, err := ti.t.read(int(blockOffset.Offset), int(blockOffset.Len))
	if err != nil {
		ti.err = err
		return
	}

	block.data = blockData
	checkSumLen, err := ti.t.read(len(blockData)-4, 4)
	if err != nil {
		ti.err = err
		return
	}
	block.checkSumLen = uint16(utils.BytesToU32(checkSumLen))

	checkSum, err := ti.t.read(len(blockData)-4-int(utils.BytesToU32(checkSumLen)), int(utils.BytesToU32(checkSumLen)))
	if err != nil {
		ti.err = err
		return
	}
	block.checkSum = checkSum
	block.end = len(blockData)
	offsetsLen, err := ti.t.read(len(blockData)-4-int(utils.BytesToU32(checkSumLen))-4, 4)
	if err != nil {
		ti.err = err
		return
	}
	block.offsetLen = uint16(utils.BytesToU32(offsetsLen))
	entryOffsets, err := ti.t.read(len(blockData)-4-int(utils.BytesToU32(checkSumLen))-4-int(block.offsetLen)*4, int(block.offsetLen)*4)
	if err != nil {
		ti.err = err
		return
	}
	block.entryOffsets = utils.BytesToU32Slice(entryOffsets)
	block.kvDataStartPos = len(blockData) - 4 - int(utils.BytesToU32(checkSumLen)) - 4 - int(block.offsetLen)
	//firstEntryOffset:=block.entryOffsets[0]
	// 每个entry的长度是四字节存储 entryoffsets数组 是uint32的数组 即4个字节
	// 对block做二分查找 使用block迭代器
	ti.bi.Seek(key)
	ti.err = ti.bi.err
	ti.it = ti.bi.Item()
}
func (t *Table) read(off, sz int) ([]byte, error) {
	return t.sst.Bytes(off, sz)
}
