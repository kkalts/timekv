package utils

import (
	"encoding/binary"
	"reflect"
	"unsafe"
)

/*
	对值相关的操作（对数据相关操作）
		比如数据不同类型之间的转换
*/
func BytesToU32(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

/*
	整体与U32SliceToBytes差不多 只是len 变成/4
*/
func BytesToU32Slice(data []byte) []uint32 {
	if len(data) == 0 {
		return nil
	}
	var b []uint32
	// 通过反射 将引用变成指针
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	hdr.Len = len(data) / 4 // uint32是四字节
	hdr.Cap = hdr.Len
	// data切片就是第一个元素的地址

	hdr.Data = uintptr(unsafe.Pointer(&data[0]))
	return b
}

/*
	uint32切片转字节切片
*/
func U32SliceToBytes(data []uint32) []byte {
	if len(data) == 0 {
		return nil
	}
	var b []byte
	// 通过反射 将引用变成指针
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	hdr.Len = len(data) * 4 // uint32是四字节
	hdr.Cap = hdr.Len
	// data切片就是第一个元素的地址

	hdr.Data = uintptr(unsafe.Pointer(&data[0]))
	return b
}

/*
	u32转字节切片
*/
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

// U64ToBytes converts the given Uint64 to bytes
func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

func BytesToU64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}
