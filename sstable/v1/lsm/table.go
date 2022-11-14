package lsm

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/corekv/sstable/v1/file"
	"github.com/hardcore-os/corekv/sstable/v1/utils"
	"sort"
)

type Table struct {
	sst *file.SSTable
	//blocks []*Block
	//index 	*Index
	fid uint64 // sstable的编号 （每个sstable都有自己的编号）
}

/*
	创建一个table
*/
func openTable(opt Options, tableName string, builder *tableBuilder) *Table {
	// 创建sst对象
	ssTable := file.OpenSSTable()
	if builder != nil {
		// builder不为空 将builder序列化到sst文件 flush
		builder.flush()
	}
	// builder为空 进行初始化 恢复.sst文件 加载Index到内存（sstable?
	ssTable.Init()

	return &Table{
		sst: ssTable,
	}
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
	index := t.sst.Index()
	filterMap := index.BloomFilter
	bloomFilter := utils.NewBloomFilterForTable(filterMap)
	if !bloomFilter.MayContainKey(key) {
		return nil, nil
	}
	// 创建table的迭代器
	ti := NewTableIterator()

}

/*
	table迭代器
*/
type TableIterator struct {
	t *Table
	//opt *Options
	it       utils.Item
	blockPos int
	bi       *blockIterator
}

func NewTableIterator() *TableIterator {
	return &TableIterator{}
}
func (ti *TableIterator) Next() {

}
func (ti *TableIterator) Valid() bool      {}
func (ti *TableIterator) Rewind()          {}
func (ti *TableIterator) Item() utils.Item {}
func (ti *TableIterator) Close() error     {}

/*
	找目标值
*/
func (ti *TableIterator) Seek(key []byte) {
	blockOffsets := ti.t.sst.Index().Offsets
	// 进行二分查找 找到key可能在的block

	// 直接使用golang的二分
	idx := sort.Search(len(blockOffsets), func(i int) bool {
		if i == len(blockOffsets) {
			// 当i==len(blockOffsets) 在最后一个？还是找不到
			return true
		}
		// GetKey > key 为什么不直接>=0 ?
		return utils.CompareKeys(blockOffsets[i].GetKey(), key) > 0
	})
	// 找到 GetKey > key  的第一个（最左值应该）
	if idx == 0 {
		// key必在第一个block 因为最左值 可以直接去对应的block中使用迭代器继续查找
		// 且 block的basekey<=key
		ti.SeekForBlock(idx, key)
	}
	// 否则 block[idx].basekey >= key 则需要找idx-1的block
	// 这个block[idx-1].basekey <=key 在idx-1的block中找
	// 在idx-1中没有 则key可能在idx中 为什么？？？？？
	ti.SeekForBlock(idx-1, key)

}

/*
	在block中找
*/
func (ti *TableIterator) SeekForBlock(idx int, key []byte) {

}
