package file

import (
	"github.com/golang/protobuf/proto"
	v1 "github.com/hardcore-os/corekv/cache/v1"
	"github.com/hardcore-os/corekv/sstable/v1/lsm"
	"github.com/hardcore-os/corekv/sstable/v1/pb"
	"os"
	"sync"
)

/*
	管理LSM的层级
*/
type LevelManager struct {
	maxFid uint64
	//opt *Options
	cache *v1.Cache

	manifestFile *ManifestFile // manifest文件结构体

	// 形成二维数组 （行-sstable对象） 将ManifestFile解析出的map(manifest-Levels-LevelManifest)最终加载到levels
	// 疑问：为什么还要再加载？ 最终查询时使用二维数组来承载
	// 在增删改时使用map []LevelManifest的形式
	levels []*LevelHandler
}

type LevelHandler struct {
	sync.RWMutex
	levelNum int
	tables   []*lsm.Table // 应该是sstable（文件）加载到内存中的句柄（对象）
}

type ManifestFile struct {
	//opt
	fd                       *os.File // 这里暂时使用普通IO来处理Manifest文件相关 文件句柄
	lock                     sync.Mutex
	deletionRewriteThreshold int       // 进行覆写的阈值
	manifest                 *Manifest // 具体的manifest数据结构体
}

/*
	状态机
	元数据状态维护
*/
type Manifest struct {
	// 各个层的sst struct
	Levels []LevelManifest

	// 快速查找 sst文件在manifest的哪一层
	// key为sst fid value为这个sst所在的层及相关信息
	// 正排 空间换时间？
	Tables map[uint64]TableManifest

	//用于判断检查点的时机
	Creations int //统计SST创建次数
	Deletions int //统计SST删除次数

}

type LevelManifest struct {
	// key为sst文件FID value为具体的sst struct
	Tables map[uint64]struct{}
}

/*
	包含sst基本信息
*/
type TableManifest struct {
	Level uint8
	// ??? 方便后续扩展
	CheckSum []byte
}

/*
	sst的一些元信息
*/
type TableMeta struct {
	ID       uint64
	CheckSum []byte
}

/*
	ManifestChangeSet和 ManifestChange 使用pb序列化
	是每个sst文件的操作记录（日志）
	磁盘中的结构
	见manifest.proto
*/

/*
	接口设计 自顶而下
*/
func NewLevelManger() *LevelManager {

}

/*
	打开/创建manifest文件
*/
func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	manifestFile := &ManifestFile{}
	// 首先打开工作目录下的manifest文件
	fd, err := os.Open(opt.FileName)
	if err != nil {

	}
	// 如果不存在 则创建内存的manifest结构（用于后续Manifest数据处理 最终再刷盘。。。）
	if fd == nil {
		manifest := &Manifest{}

		// 使用覆写的方式进行创建文件 ？ 为什么用覆写的方式
		rewriteManifestFile(opt, manifest)
	}

	// 重放

}

func rewriteManifestFile(opt *Options, m *Manifest) {
	fd, err := os.OpenFile(opt.FileName+"-remanifest", os.O_CREATE|os.O_RDWR, 666)

	// 将m manifest的数据转换成manifestchange 进行序列化 并追加到文件
	changes := m.transManifestToChange()
	set := pb.ManifestChangeSet{Changes: changes}
	marshalChangeSet, err := proto.Marshal(&set)
	if err != nil {

	}
	_, err = fd.Write(marshalChangeSet)
	if err != nil {

	}
	os.Rename(opt.FileName+"-remanifest", opt.FileName+"-manifest")
}

func (m *Manifest) transManifestToChange() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, len(m.Tables))
	for fid, tm := range m.Tables {
		// 为什么都搞成create？
		changes = append(changes, newCreateChange(fid, int(tm.Level), tm.CheckSum))
	}
	return changes

}
func newCreateChange(fid uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       fid,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

/*
	重放manifest文件
		即解析manifest文件的manifestChange 并将数据放入manifest数据结构中
*/
func replayManifestFile(m *Manifest, opt *Options) {
	// 读取manifest文件
	fd, err := os.Open(opt.FileName)
	if err != nil {

	}
	var manifestChangeByte []byte
	read, err := fd.Read(manifestChangeByte)
	var manifestChangeSet = &pb.ManifestChangeSet{}
	err := proto.Unmarshal(manifestChangeByte, manifestChangeSet)

	for i := 0; i < len(manifestChangeSet.Changes); i++ {
		change := manifestChangeSet.Changes[i]

		switch change.Op {
		case pb.ManifestChange_CREATE:
			levelMap := m.Levels[change.Level].Tables
			levelMap[change.Id] = struct{}{}
			table := TableManifest{
				Level:    uint8(change.Level),
				CheckSum: change.Checksum,
			}
			m.Tables[change.Id] = table
			m.Creations++
		case pb.ManifestChange_DELETE:
			delete(m.Tables, change.Id)
			levelManifest := m.Levels[change.Level]
			delete(levelManifest.Tables, change.Id)
			m.Deletions++
		}
	}

}

/*
	添加table元信息到manifest文件
*/
