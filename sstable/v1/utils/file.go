package utils

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"hash/crc32"
	"path"
	"strconv"
	"strings"
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

/*
	校验数据
		对data使用相同的方式计算校验和 与 expected比较
*/
func VerifyChecksum(data []byte, expected []byte) error {
	newCheckSum := CalCacheSum(data)
	expectedU64 := BytesToU64(expected)
	if newCheckSum != newCheckSum {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", newCheckSum, expectedU64)
	}
	return nil
}

/*
	通过sst文件名 获取其序号
*/
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		//Err(err)
		return 0
	}
	return uint64(id)
}

/*
	通过fid获取文件名
*/
func GetFileNameSSTable(dir string, fid uint64) string {

}

/*
	根据目录路径获取其下所有sst文件ID
*/
func LoadSSTFIDMap(dir string) map[uint64]struct{} {

}
