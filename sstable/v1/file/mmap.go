package file

import "os"

type MmapFile struct {
	Data []byte
	Fd   *os.File
}

func (mmap *MmapFile) Bytes(offset, size int) ([]byte, error) {
	return mmap.Data[offset : offset+size], nil
}
