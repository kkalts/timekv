package lsm

import (
	v12 "github.com/hardcore-os/corekv/casSkipList/v1"
)

/*
	内存中的kv结构 更抽象化的上层
		在timekv中是跳表
*/

type memTable struct {
	sl *v12.SkipList
}
