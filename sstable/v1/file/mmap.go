package file

import "os"

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

	// 将上面的普通文件 与 一块内存空间关联起来 使用mmap 创建出MmapFile
	OpenMmapFileUsing()

}

/*
	将普通文件 与 一块内存空间关联起来 使用mmap 创建出MmapFile
*/
func OpenMmapFileUsing(fd *os.File, maxSz int, writable bool) {

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

	// 调用mmap 将文件与内存空间关联起来

}
