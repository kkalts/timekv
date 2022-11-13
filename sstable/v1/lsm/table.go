package lsm

import "github.com/hardcore-os/corekv/sstable/v1/file"

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
func (t *Table) Search() {

}
