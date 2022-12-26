package lsm

type Options struct {
	EveryBlockSize     int     // 每个block的大小 序列化后
	BloomFalsePositive float64 // 布隆过滤器假阳性
	BloomFilterK       uint8
	MaxLevelNum        int    // lsm最大层级数
	WorkDir            string // 工作目录
}
