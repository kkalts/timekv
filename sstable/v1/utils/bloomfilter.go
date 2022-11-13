package utils

import "math"

type Filter []byte

type BloomFilter struct {
	bitMap Filter
	k      uint8
}

func NewBloomFilter(numEntries int, falsePositive float64) *BloomFilter {
	bitPerKey := BitsPerKey(numEntries, falsePositive)
	return initFilter(bitPerKey, numEntries)
}

/*
	可能包含key 如果包含返回true
*/
func (b *BloomFilter) MayContainKey(key []byte) bool {
	return b.MayContain(Hash(key))
}

func (b *BloomFilter) FilterBitMap() []byte {
	return b.bitMap
}

func (b *BloomFilter) MayContain(hashed uint32) bool {
	if b.Len() < 2 {
		return false
	}
	//if b.k>30{
	//	return true
	//}
	// 将经过hash后的key
	// 使用一个hash函数 进行K次计算 相当于进行K次哈希
	nBits := uint32(8 * (b.Len() - 1))
	// h是数组中的每一个元素 对每个元素进行（h >> 17 | h<<15） 即hash函数
	delta := hashed>>17 | hashed<<15
	// 每一位都进行K次哈希
	for j := uint8(0); j < b.k; j++ {
		bitPos := hashed % uint32(nBits)
		rowIndex := bitPos / 8 // 行
		// 判断位置上是不是1
		// 这里考虑哈希值为0的情况 哈希值不为负
		colIndex := bitPos % 8 // 列
		if b.bitMap[rowIndex]&1<<colIndex == 0 {
			return false
		}
		// 这里元素+delta 即改变上次算出的位置 即计算出新的哈希值
		hashed += delta
	}
	return true
}
func (b *BloomFilter) Len() int32 {
	return int32(len(b.bitMap))
}
func (b *BloomFilter) InsertKey(key []byte) bool {
	return b.Insert(Hash(key))
}
func (b *BloomFilter) Insert(hashed uint32) bool {
	nBits := uint32(8 * (b.Len() - 1))
	// h是数组中的每一个元素 对每个元素进行（h >> 17 | h<<15） 即hash函数
	delta := hashed>>17 | hashed<<15
	// 每一位都进行K次哈希
	for j := uint8(0); j < b.k; j++ {
		bitPos := hashed % uint32(nBits)
		rowIndex := bitPos / 8
		// 这里考虑哈希值为0的情况 哈希值不为负
		colIndex := bitPos % 8
		b.bitMap[rowIndex] |= 1 << colIndex
		// 这里元素+delta 即改变上次算出的位置 即计算出新的哈希值
		hashed += delta
	}
	return true
}

/*
	布隆过滤器中是否包含Key 如果存在返回true 不存在 则放入过滤器
*/
func (b *BloomFilter) AllowKey(key []byte) bool {
	return b.Allow(Hash(key))
}
func (b *BloomFilter) Allow(hashed uint32) bool {
	if b == nil {
		return true // 这里为什么return true？ 不存在过滤器则当作存在？ 即不做判断
	}
	contain := b.MayContain(hashed)
	if contain {
		return true
	}
	// 不包含 放入
	return b.Insert(hashed)
}

/*
	重置布隆过滤器
*/
func (b *BloomFilter) reset() {
	if b == nil {
		return
	}
	for i := 0; i < len(b.bitMap); i++ {
		b.bitMap[i] = 0
	}
}

// 求 在n（数据量）和p（假阳性率）确定的情况下 求m m/n 最终求出k
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

/*
	numEntries: key的个数
	bitsPerKey：数组大小 即 m
*/
func initFilter(bitsPerKey int, numEntries int) *BloomFilter {
	bf := &BloomFilter{}
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := CalHashNum(bitsPerKey)
	// 哈希函数的个数
	bf.k = uint8(k)
	// n = m/n * n
	nBits := bitsPerKey * numEntries
	if nBits < 64 {
		nBits = 64
	}
	// 为了使nBytes为8的倍数
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	// 这里+1 是为了留出一行用于存储哈希函数个数？
	var filter = make([]byte, nBytes+1)

	//// 使用一个hash函数 进行K次计算 相当于进行K次哈希
	//for _, h := range keys {
	//	// h是数组中的每一个元素 对每个元素进行（h >> 17 | h<<15） 即hash函数
	//	delta :=h >> 17 | h<<15
	//	// 每一位都进行K次哈希
	//	for j := uint32(0); j < k ; j++ {
	//
	//		bitPos:=h % uint32(nBits)
	//		rowIndex:=bitPos/8
	//		// 这里考虑哈希值为0的情况 哈希值不为负
	//		colIndex:=bitPos%8
	//		filter[rowIndex] |= 1<<colIndex
	//		// 这里元素+delta 即改变上次算出的位置 即计算出新的哈希值
	//		h+=delta
	//	}
	//}

	// 最后一行 存储哈希函数的个数 即 k
	filter[nBytes] = uint8(k)
	bf.bitMap = filter
	return bf
}

/*
	hash函数
	Murmur hash
*/
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
