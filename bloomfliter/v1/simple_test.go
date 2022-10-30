package v1

import (
	"fmt"
	"math"
	"testing"
)

func TestBloomF(t *testing.T) {
	var data = []byte("111")
	hash1 := hashFun1(data)
	fmt.Println(hash1)
	hash2 := hashFun2(data)
	fmt.Println(hash2)
	var bloomF = make([]byte, 100) // 一个100的数组
	bloomF[hash1%100] = 1
	bloomF[hash2%100] = 1
	fmt.Println(bloomF)

	filter := MaybeInBloomFilter(data, bloomF)
	fmt.Println(filter)
}

func MaybeInBloomFilter(data []byte, filter []byte) bool {
	fun1 := hashFun1(data)
	fun2 := hashFun2(data)
	if filter[fun1%uint32(len(filter))] == 1 && filter[fun2%uint32(len(filter))] == 1 {
		return true
	}
	return false
}

func hashFun1(data []byte) uint32 {
	var hashResult uint32
	for i := 0; i < len(data); i++ {
		hashResult += uint32(data[i]) / 3
	}
	return hashResult
}

func hashFun2(data []byte) uint32 {
	var hashResult uint32
	for i := 0; i < len(data); i++ {
		hashResult += uint32(data[i])
	}
	return hashResult
}

// 求 在n（数据两）和p（假阳性率）确定的情况下 求m m/n 最终求出k
// m即数组大小
func BitsPerKey(n int, p float64) int {
	m := -1 * float64(n) * math.Log(p) / math.Pow(0.69314718056, 2)
	// locs 即 m / n
	locs := math.Ceil(m / float64(n))
	return int(locs)
}

// 计算K 即 哈希函数的个数
func CalHashNum(bitsPerKey int) uint32 {
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

// 生成布隆过滤器
// bitsPerKey = (m/n)
// keys即元素
func appendFilter(bitsPerKey int, keys []uint32) []int {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := CalHashNum(bitsPerKey)
	// n = m/n * n
	nBits := bitsPerKey * len(keys)
	var filter = make([]int, nBits)
	// 使用一个hash函数 进行K次计算 相当于进行K次哈希
	for _, h := range keys {
		// h是数组中的每一个元素 对每个元素进行（h >> 17 | h<<15） 即hash函数
		delta := h>>17 | h<<15
		// 每一位都进行K次哈希
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			filter[bitPos] = 1
			// 这里元素+delta 即改变上次算出的位置 即计算出新的哈希值
			h += delta
		}
	}
	return filter
}

/*
	优化方向：
		1. 替换哈希函数，对于不同定义的数据用不同的哈希函数
		2. 压缩，节省空间，使用二维数组方式理解并存储bit,比如一个make([]byte,8)就可以存储8*8=64个bit 64位一般就够用了
			举例，现一数据hash后值为13，则将第13位设置为1，则通过 13 / 8 = 1获取到行
					13%8=5 获取到列（这里是第5列 这里考虑哈希值为0的情况）
			即 行号 = bitPos / 8 (即数组中index)
				列号 = bitPos % 8 （1左移列号位）如图
				找到位置后与之前这行的值做与运算，拼接起来

*/
func appendFilter2(bitsPerKey int, keys []uint32) []byte {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := CalHashNum(bitsPerKey)
	// n = m/n * n
	nBits := bitsPerKey * len(keys)
	if nBits < 64 {
		nBits = 64
	}
	// 为了使nBytes为8的倍数
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	// 这里+1 是为了留出一行用于存储哈希函数个数？
	var filter = make([]byte, nBytes+1)
	// 使用一个hash函数 进行K次计算 相当于进行K次哈希
	for _, h := range keys {
		// h是数组中的每一个元素 对每个元素进行（h >> 17 | h<<15） 即hash函数
		delta := h>>17 | h<<15
		// 每一位都进行K次哈希
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			rowIndex := bitPos / 8
			// 这里考虑哈希值为0的情况 哈希值不为负
			colIndex := bitPos % 8
			filter[rowIndex] |= 1 << colIndex
			// 这里元素+delta 即改变上次算出的位置 即计算出新的哈希值
			h += delta
		}
	}
	// 最后一行 存储哈希函数的个数 即 k
	filter[nBytes] = uint8(k)
	return filter
}
