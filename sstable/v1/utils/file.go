package utils

import (
	"bytes"
	"fmt"
	"hash/crc32"
)

/*
	文件相关的工具函数
*/
func CompareKeys(key1, key2 []byte) int {
	// 这里为什么key长度小于8字节就panic? 是小于8的不会到这步吗？ 不再计算score？
	CondPanic((len(key1) <= 8 || len(key2) <= 8), fmt.Errorf("%s,%s < 8", string(key1), string(key2)))
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

var CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

func CalCacheSum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}
