package file

import (
	"github.com/hardcore-os/corekv/sstable/v1/utils/mmap"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
)

type MmapFile struct {
	Data []byte
	Fd   *os.File
}

func (mmap *MmapFile) Bytes(offset, size int) ([]byte, error) {
	return mmap.Data[offset : offset+size], nil
}

func OpenMmapFile(filename string, flag int, maxSz int) (*MmapFile, error) {
	fd, err := os.OpenFile(filename, flag, 666)
	if err != nil {

	}

	// 是否可写的标识
	var writable = true
	if flag == os.O_RDONLY {
		writable = false
	}

	// 将上面的普通文件 与 一块内存空间关联起来 使用mmap 创建出MmapFile
	return OpenMmapFileUsing(fd, maxSz, writable)
}

/*
	将普通文件 与 一块内存空间关联起来 使用mmap 创建出MmapFile

	如果文件是新建的 则maxSz填充文件 并与内存关联
	如果文件是已经存在的 文件的也与内存关联 数据写入到了mmap的buf中

*/
func OpenMmapFileUsing(fd *os.File, maxSz int, writable bool) (*MmapFile, error) {

	// 获取文件元数据
	fileName := fd.Name()
	// 文件统计信息
	fileStatInfo, err := fd.Stat()
	if err != nil {

	}
	fSize := fileStatInfo.Size()

	// 如果文件是新建的 则fSize是0 若文件与内存空间关联 则需要文件的大小与关联内存空间的大小相同 即maxSz
	// 则需要给普通文件填充数据 使其到maxSz大小
	if maxSz > 0 && fSize == 0 {
		err := fd.Truncate(int64(maxSz))
		if err != nil {

		}
		fSize = int64(maxSz)
	}

	// 调用mmap 将文件与内存空间关联起来 将内存空间buf返回
	buf, err := mmap.Mmap(fd, writable, fSize)
	if err != nil {

	}
	if fSize == 0 {
		// 如果为0 则没有走上面 if maxSz > 0 && fSize == 0这一步
		// maxSz为0 则没有真正将mmap与文件关联起来 需要异步刷盘 强制关联
		// fSize 可能是有数据的 则可以触发强制刷盘 （flush时？？？
		dir, _ := filepath.Split(fileName)
		go SyncDir(dir)
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, nil
}

func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}
