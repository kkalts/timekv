package file

import (
	"encoding/binary"
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/corekv/sstable/v1/pb"
	"os"
	"sync"
)

type SSTable struct {
	lock *sync.RWMutex
	f    *MmapFile // mmap对象
	fid  uint64

	idxLen    int
	idxStart  int
	idxTables *pb.TableIndex
	minKey    []byte
	maxKey    []byte
}

/*
	打开一个指定文件ID的sst文件 映射到SSTable 结构体
	已经存在的sst文件
	类似与NewSSTable
*/
func OpenSSTable(opt Options) *SSTable {
	// 根据文件ID找到sst文件mmap文件 获取其句柄？
	mmapFile, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {

	}
	return &SSTable{
		lock: &sync.RWMutex{},
		f:    mmapFile,
		fid:  opt.FID,
	}
}
func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

/*
	初始化 是指将一个完整的sst文件所有相关数据 映射到 SSTable结构体
	解析sst文件数据

*/
func (sst *SSTable) Init() error {
	// 从高地址开始读 读4Byte 校验和长度 读f的data
	readLastPos := len(sst.f.Data)
	checkSumLenPos := readLastPos - 4
	checkSumLenBuf := sst.f.Data[checkSumLenPos:readLastPos]
	checkSumLen := int(BytesToU32(checkSumLenBuf))
	checkSumPos := checkSumLenPos - checkSumLen
	checkSum := sst.f.Data[checkSumPos:checkSumLenPos]

	// index_len
	indexLenPos := checkSumPos - 4
	indexLenBuf := sst.f.Data[indexLenPos:checkSumPos]

	indexLen := int(BytesToU32(indexLenBuf))
	sst.idxLen = indexLen
	// index
	indexPos := indexLenPos - indexLen
	indexData := sst.f.Data[indexPos:indexLenPos]

	sst.idxStart = indexPos
	// 计算校验和与checkSum对比 这个校验和是校验index数据

	// 解析index数据 index数据使用PB进行序列化 需要反序列化
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(indexData, indexTable); err != nil {
		return err
	}
	sst.idxTables = indexTable

	if len(indexTable.GetOffsets()) > 0 {
		// 这里需要确定 拿到的是值还是地址？ 这里想要的是值
		minKey := indexTable.GetOffsets()[0].GetKey()
		sst.minKey = minKey
		sst.maxKey = minKey
	}
	return nil
}

func (sst *SSTable) Index() *pb.TableIndex {
	return sst.idxTables
}
func (sst *SSTable) Bytes(offset, size int) ([]byte, error) {
	return sst.f.Bytes(offset, size)
}
