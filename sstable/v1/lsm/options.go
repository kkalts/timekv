package lsm

type Options struct {
	EveryBlockSize     int     // 每个block的大小 序列化后
	BloomFalsePositive float64 // 布隆过滤器假阳性
}
