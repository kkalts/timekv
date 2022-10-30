package v1

import (
	"math/rand"
	"time"
)

type cmRow []byte // []byte的每个元素是一个byte 可以放两个数据的访问频次 即两个counter

func newCmRow(numCounters int64) cmRow {
	return make([]byte, numCounters/2)
}

/*
	获取n的统计频次
*/
func (r cmRow) get(n uint64) byte {
	if int(n) > len(r)*2-1 {
		panic("index out of cmRow")
		return 0
	}
	indexBig := n / 2    // 数组的index
	indexLittle := n % 2 // 数组元素中的index 0是第一个 即前4位 1是第二个即后四位

	// 在哪个counter上加1
	// 两个值都要小于15 不然溢出
	littleBit := r[indexBig] << 4 & 0xff >> 4
	bigBit := r[indexBig] >> 4

	if indexLittle == 0 {
		return littleBit
	} else if indexLittle == 1 {
		return bigBit
	}
	return 0
}

func (r cmRow) get2(n uint64) byte {
	return byte(r[n/2]>>((n&1)*4)) & 0x0f
}

/*
	对n的统计频次+1
		n是经过hash后的位置
*/
func (r cmRow) increment(n uint64) {
	if int(n) > len(r)*2-1 {
		panic("index out of cmRow")
		return
	}
	// 找到n属于哪个counter
	indexBig := n / 2    // 数组的index
	indexLittle := n % 2 // 数组元素中的index 0是第一个 即前4位 1是第二个即后四位

	// 在哪个counter上加1
	// 两个值都要小于15 不然溢出
	littleBit := r[indexBig] << 4 & 0xff >> 4
	bigBit := r[indexBig] >> 4

	if indexLittle == 0 {
		if littleBit < 15 {
			littleBit++
		}
	} else if indexLittle == 1 {
		if bigBit < 15 {
			bigBit++
		}
	}

	r[indexBig] = bigBit<<4 | littleBit

}

func (r cmRow) increment2(n uint64) {
	i := n / 2
	s := (n & 1) * 4
	v := (r[i] >> s) & 0x0f
	if v < 15 {
		r[i] += 1 << s
	}
}

/*
	保鲜机制
		将所有的数据的访问频次减半
*/
func (r cmRow) reset() {
	for k, _ := range r {
		// 每个byte减半
		r[k] = r[k] >> 1 & 0x77 // 0x77是  0111 0111 每个四位取后三位
	}
}

/*
	清空
		清空数据的访问频次
*/
func (r cmRow) clear() {
	for k, _ := range r {
		r[k] = 0
	}
}

// 快速计算大于 X，且最接近 X 的二次幂
func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

const cmDepth = 4 // 4个hash函数
/*
	封装cmSketch
		最终要有N个Hash函数 对N行cmRow 进行hash后 获取key在每一行cmRow的频次 找最小的
*/
type cmSketch struct {
	rows [cmDepth]cmRow  // 二维数组 4行cmRow
	seed [cmDepth]uint64 // 四个随机数 hash的作用？
	mask uint64          // numCounters（next2Power） - 1 = next2Power-1  用来保留后N位 为了用来将数据固定在numCounter之内 防止溢出 但会冲突
	// 比如numCounter=16 若hash后的值是17 则 17 & mask 变位1
}

func NewCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid numCounters")
	}
	numCounters = next2Power(numCounters)
	mask := numCounters - 1
	var s = &cmSketch{}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < cmDepth; i++ {
		s.rows[i] = newCmRow(numCounters)
		s.seed[i] = source.Uint64()
	}
	s.mask = uint64(mask)

	return s
}

/*
	hashed：hash后的值 hash还要之前的步骤做
*/
func (s *cmSketch) Increment(hashed uint64) {
	rows := s.rows
	for i, _ := range rows {
		rows[i].increment((hashed ^ s.seed[i]) & s.mask)
	}
}

/*
	找hashed在四个cmRow中最小的计数值
*/
func (s *cmSketch) Estimate(hashed uint64) int64 {
	var min = byte(255)
	for i := 0; i < len(s.rows); i++ {
		record := s.rows[i].get((hashed ^ s.seed[i]) & s.mask)
		if record < min {
			min = record
		}
	}
	return int64(min)
}

func (s *cmSketch) Clear() {
	for i := 0; i < len(s.rows); i++ {
		s.rows[i].clear()
	}
}
func (s *cmSketch) Reset() {
	for i := 0; i < len(s.rows); i++ {
		s.rows[i].reset()
	}
}
