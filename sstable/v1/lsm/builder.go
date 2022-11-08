package lsm

import "github.com/hardcore-os/corekv/sstable/v1/utils"

/*
	将跳表数据/其他SST文件数据 序列化
	策略模式
		tableBuilder 具体的策略
		这里似乎并没有使用策略模式
	可以提供不同的存储格式

*/
type tableBuilder struct {
	blockList  []*Block
	curBlock   *Block
	maxVersion uint64
	keyHashes  []uint32
}

type Block struct {
	data         []byte   // kv_data （PB序列化）
	entryOffsets []uint32 // 每个kv数据在data中的offset
	offsetLen    uint16   // offsets数组的长度
	checkSum     []byte   // 对data entryOffsets offsetLen 计算一个校验和
	checkSumLen  uint16   // 校验和的长度
	offset       uint16   // 当前block的offset
	baseKey      []byte   // 当前block的第一个key
	end          int      // 当前block的data的结束位置（data的大小？
}
type Header struct {
	overlap uint16 // 与base key相同的部分的长度（base key是当前block的第一个key)
	diff    uint16 //  与base key不同部分的长度
}
type BuildData struct {
	blockList []*Block
}

func (tb *tableBuilder) flush() {

}

/*
	在flush时调用 将跳表上数据一个个加到builder的一个个block中
*/
const BlockMaxSize = 0

func (tb *tableBuilder) add(e *utils.Entry) {
	key := e.Key
	val := utils.ValueStruct{
		Meta:     e.Meta,
		Value:    e.Value,
		ExpireAt: e.ExpireAt,
		Version:  e.Version,
	}
	// 当前block大小是否到达限制
	if len(tb.curBlock.data) >= BlockMaxSize {
		// 是 计算当前block的各项信息 序列化(指将各种数据转为[]byte 放入data)当前block（放入block list)

		// 开辟新的block
	}

	// 否 计算hash(key)
	keyHash := keyHash(key)
	tb.keyHashes = append(tb.keyHashes, keyHash)
	// 计算block 最大版本号（即sst的最大version 不断比较
	if e.Version > tb.maxVersion {
		tb.maxVersion = e.Version
	}
	// 计算diffkey
	diffKey := tb.curBlock.calDiffKey(key)

	// 计算header
	h := Header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	// 计算当前block的end_offset 写入list（当前entry写入后的 下一个entry的起始位置）
	// 这里end还没改变 则 这里是当kv数据在当前data中的开始offset 放入数组中
	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	// 将header diffkey（）放入当前block的kv_data字节数组
	tb.append(h)
	tb.append(diffKey)
	// 将val转为[]byte 放入data中

	// 更新offsets offset_len
	tb.allocate(val)

}
func (b *Block) calDiffKey(key []byte) []byte {

}
func keyHash(key []byte) uint32 {

}

/*
	将数据放入tb的curBlock的data中
*/
func (tb *tableBuilder) append(data []byte) []byte {

}

/*
	优化点：使用内存分配器

	params:
		need:需要分配的大小
	return:
		在block.data上分配好后将那段位置返回，使用时将数据复制到return的数组中即可 需要测试 可以这样操作吗？

	https://juejin.cn/post/6888117219213967368
	https://blog.csdn.net/weixin_44387482/article/details/119763558
*/
func (tb *tableBuilder) allocate(need int) []byte {

}
