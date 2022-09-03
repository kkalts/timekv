package v1

import (
	"sync/atomic"
)

/*
	实现Arena基本操作
*/

const (
	MaxNodeSize = 0 // 跳表节点最大的大小
)

/*
	 内存分配管理 单元 （只分配 不释放）
	每次先向操作系统申请一大块内存空间，之后跳表节点需要的内存就在arena上分配
	且 一个跳表关联一个arena
	一个跳表中之后就记录 节点在arena上的地址/offset之类的
*/
type Arena struct {
	n   uint32 // Arena中已经分配出去的内存大小，即offset
	buf []byte // Arena申请的内存空间
}

/*
	新建Arena
	一开始要向操作系统申请N大小的内存
*/
func NewArena(n int64) *Arena {
	return &Arena{
		n:   1, // 从1开始？
		buf: make([]byte, n),
	}
}

/*
	在Arena上分配内存
	params:
		size 即要在arena上分配多少内存？
	return:
		返回在arena上的offset 即分配的内存的第一个byte开始的位置

	这个总体应该是不支持并发分配内存
*/
func (s *Arena) allocate(size uint32) uint32 {
	// 当前arena的内存是否够？ （当前+要分配的） 不够需要扩大arena的大小 一般是2倍
	// 假设可以放下 原子的 支持并发申请内存
	offset := atomic.AddUint32(&s.n, size)

	// 上面是假设 但是要真的确定还能放下吗？
	// 且这里要提前预测 还能放下下一个节点吗？ 如果不行 就尽早扩容
	if len(s.buf)-int(offset) < MaxNodeSize {
		// 放不下  则需要扩容arena
		growUnit := uint32(len(s.buf))
		// 但是扩容的大小有上限 小于 2的30次方(1G)
		if growUnit > 1<<30 {
			growUnit = 1 << 30
		}
		// 如果要分配的还大于 growUnit 则 分配size的 （但是 若size大于1G嘞？尽量满足分配）  应该是对arena有做限制 之后如果到达阈值则转为immemtable
		if growUnit < size {
			growUnit = size
		}

		// 非原子 不支持并发扩容
		newBuf := make([]byte, int(growUnit)+len(s.buf))
		// 将原数据拷贝到新buf中 修改arena的指针 让gc来处理原来的byte数组
		copy(newBuf, s.buf)
		// 判断 当前
		s.buf = newBuf
	}

	// 最终返回
	return offset - size
}

// ------------------------接口封装-----------------------------------------------

/*

 */
