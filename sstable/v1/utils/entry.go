package utils

import "encoding/binary"

type Entry struct {
	Key   []byte
	Value []byte

	ExpireAt uint64 // 做缓存时  有过期时间
	Version  uint64
	Meta     byte
}

type ValueStruct struct {
	Meta     byte
	Value    []byte
	ExpireAt uint64 // 做缓存时  有过期时间
	Version  uint64
}

func (e *ValueStruct) DecodeValue(buf []byte) {
	var sz int
	// 从buf中将Uint64解码出来
	e.ExpireAt, sz = binary.Uvarint(buf)
	e.Value = buf[sz:]
}

func (e *ValueStruct) EncodeValue(b []byte) uint32 {
	// 将uint64值 编码到字节数组中 返回uint64值（即e.ExpireAt)的大小
	// 这样编码 减少uint64占用的内存大小
	sz := binary.PutUvarint(b[:], e.ExpireAt)
	// 将e.Value 放到b数组中 expireAt的后面
	n := copy(b[sz:], e.Value)
	return uint32(n + sz)
}
