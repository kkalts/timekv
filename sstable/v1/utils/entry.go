package utils

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
