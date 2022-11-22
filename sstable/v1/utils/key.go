package utils

import (
	"encoding/binary"
	"math"
)

/*
	key相关的工具函数
*/

// ParseTs parses the timestamp from the key bytes.
// 将key的字节数组中的时间戳解析出来（在上层已经处理好，将数据放在key的后八位） 具体需要看上层处理
func ParseTs(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}
