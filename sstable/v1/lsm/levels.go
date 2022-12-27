package lsm

import (
	"errors"
	"fmt"
	v1 "github.com/hardcore-os/corekv/cache/v1"
	"github.com/hardcore-os/corekv/sstable/v1/file"
	"github.com/hardcore-os/corekv/sstable/v1/utils"
	"os"
	"sync"
)

/*
	管理LSM的层级
*/
type LevelManager struct {
	maxFid uint64
	opt    *Options
	cache  *v1.Cache

	manifestFile *file.ManifestFile // manifest文件结构体

	// 形成二维数组 （行-sstable对象） 将ManifestFile解析出的map(manifest-Levels-LevelManifest)最终加载到levels
	// 疑问：为什么还要再加载？ 最终查询时使用二维数组来承载
	// 在增删改时使用map []LevelManifest的形式
	levels []*LevelHandler
}

type LevelHandler struct {
	sync.RWMutex
	levelNum int
	tables   []*Table // 应该是sstable（文件）加载到内存中的句柄（对象）
	lm       *LevelManager
}

/*
  向LevelHandler中增加ssttable（内存中数据结构）
*/
func (lh *LevelHandler) add(table *Table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, table)
}

/*
	levelManager的build
	构建levelmanger(并不是levelmanager的初始化 / new）
		将manifest中数据 加载到Levelhandler
*/
func (lm *LevelManager) build() error {
	lm.levels = make([]*LevelHandler, 0, lm.opt.MaxLevelNum)
	// 填充levels
	// 初始化每层的level
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &LevelHandler{
			levelNum: i,
			tables:   make([]*Table, 0),
			lm:       lm,
		})
	}
	// 从manifest中解析数据 填充lm.levels
	manifest := lm.manifestFile.GetManifest()

	// 校验manifest文件的正确性
	// 校验当前所有sst文件ID是否在manifest.table中都存在
	// manifestfile中记录的所有sst文件
	sstfidMap := utils.LoadSSTFIDMap(lm.opt.WorkDir)

	// 确保manifest中的文件都在当前工作目录中 因为manifest要记录一个正确的数据库状态
	for sstFid, _ := range manifest.Tables {
		if _, ok := sstfidMap[sstFid]; !ok {
			// 不存在 报错
			return errors.New()
		}
	}
	// 删除 与当前所有sst文件ID对比 manifest.Tables中多余的sst文件
	// 当前目录的 >= manifest
	// 所有sst文件以manifest中的为准 以此保持一个正确的数据库状态
	// 如果工作目录有 但manifest中没有则删除工作目录中的文件
	for fid, _ := range sstfidMap {
		if _, ok := manifest.Tables[fid]; !ok {
			// 删除
			filename := utils.GetFileNameSSTable(lm.opt.WorkDir, fid)
			if err := os.Remove(filename); err != nil {
				//return errors.Wrapf(err, "While removing table %d", id)
			}
		}
	}

	// 写入Levels
	for fid, tableLevelInfo := range manifest.Tables {
		fName := utils.GetFileNameSSTable(lm.opt.WorkDir, fid)
		// 内存中sst table文件数据结构
		sstTable := openTable(*lm.opt, fName, nil)
		lm.levels[tableLevelInfo.Level].add(sstTable)
	}

	// 加载sst文件的索引到cache中

	return nil
}

/*
	levelManager的flush
		将一个sst table 变为sst文件 flush到硬盘
		将sst文件的层级关系放入levelhandler - manifest

	参数：immutable MemTable类型
			immutable与memtable实质是同一类型 都是内存中的kv数据（跳表 builder)
*/
func (lm *LevelManager) flush(immutable *memTable) error {
	// 将immutable flush到硬盘
	// 分配一个fid
	var fid uint64
	// 获取到文件名
	sstFName := utils.GetFileNameSSTable(lm.opt.WorkDir, fid)
	// 构建builder 需要将immutable的数据 放入builder中
	builder := NewTableBuilder()

	// 需要将immutable的数据 放入builder中
	// 迭代immutable 使用迭代器
	skipListIterator := immutable.sl.NewSkipListIterator()
	for skipListIterator.Rewind(); skipListIterator.Valid(); skipListIterator.Next() {
		builder.add(skipListIterator.Item())
	}
	// 刷盘到硬盘
	table := openTable(*lm.opt, sstFName, builder)
	// 写入sst文件数据 到 manifest
	lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       fid,
		CheckSum: []byte{'m', 'o', 'c', 'k'},
	})
	// 将sst写入levelhandler 这里为什么写入第一层？ 后续进行压缩处理吗
	lm.levels[0].add(table)

	return nil

}
