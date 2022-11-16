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

/*
	entry 变长编码
*/
func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	//enc := sizeVarint(uint64(e.Meta)) // 此时还没有涉及meta可忽略
	enc := sizeVarint(e.ExpireAt)
	return uint32(sz + enc)
}
func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func (e *ValueStruct) DecodeValue(buf []byte) {
	var sz int
	// 从buf中将Uint64解码出来
	e.ExpireAt, sz = binary.Uvarint(buf)
	e.Value = buf[sz:]
}

func (e *ValueStruct) EncodeValue(b []byte) uint32 {
	// 其实还需要一字节放meta 此时先忽略
	// 将uint64值 编码到字节数组中 返回uint64值（即e.ExpireAt)的大小
	// 这样编码 减少uint64占用的内存大小
	sz := binary.PutUvarint(b[:], e.ExpireAt)
	// 将e.Value 放到b数组中 expireAt的后面
	n := copy(b[sz:], e.Value)
	return uint32(n + sz)
}

// value只持久化具体的value值和过期时间
func (vs *ValueStruct) EncodedSize() uint32 {
	//sz := len(vs.Value) + 1 // meta 多一个字节 为meta留位置 此时可以忽略
	sz := len(vs.Value) // meta 多一个字节 为meta留位置 此时可以忽略
	enc := sizeVarint(vs.ExpireAt)
	return uint32(sz + enc)
}
