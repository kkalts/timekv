package v1

import "container/list"

/*
	分段的LRU实现
		两个阶段
			阶段一：待淘汰区 20%
			阶段二：保护区  80%
	两个阶段的数据会互相跑

	主要在get的时候互相转换，以及其他保鲜等机制
	set即将数据set到阶段一的lru中，一般的lru逻辑
*/

const (
	STAGE_ONE = 1
	STAGE_TWO = 2
)

type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne, stageTwo       *list.List
}

func newSegmentedLRU(size1, size2 int, data map[uint64]*list.Element) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: size1,
		stageTwoCap: size2,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

/*
	数据放入
		数据先进入stageOne 即 probation区
	当数据在probation区被再次访问，则放到protected区
	protected区满了 则需要淘汰最后一个数据
*/
func (sl *segmentedLRU) add(newItem storeItem) {
	newItem.stage = 1
	// 先进入stageOne 如果有stageOne空间 或者整体有空间
	//  整体有空间 如果stageOne没有空间 是先放在stageOne 后续再放在stageTwo?
	if sl.stageOne.Len() < sl.stageOneCap || sl.Len() < sl.stageOneCap+sl.stageTwoCap {
		sl.data[newItem.key] = sl.stageOne.PushFront(&newItem)
		return
	}
	// stageOne没空间了 淘汰stageOne的数据 新数据必须放在stageOne
	back := sl.stageOne.Back()
	backItem := back.Value.(*storeItem)
	delete(sl.data, backItem.key)
	*backItem = newItem
	sl.data[newItem.key] = back
	sl.stageOne.MoveToFront(back)
	// 比较stageOne要淘汰的访问频次 与 新数据的访问频次 --- 这里没有比较？
	// 在cache中做？
}

func (sl *segmentedLRU) Len() int {
	return sl.stageOne.Len() + sl.stageTwo.Len()
}

// 将对应的元素 在stageone 和 stageTwo之间
func (sl *segmentedLRU) get(v *list.Element) {
	// 如果v在链表中没有？
	item := v.Value.(*storeItem)

	// 如果已经在stageTwo 则将元素移到stageTwo的头部
	if item.stage == STAGE_TWO {
		sl.stageTwo.MoveToFront(v)
		return
	}
	// 如果元素在stageOne 再次访问了 可以放在stageTwo
	// 如果stageTwo满了 则要淘汰数据 stageOne?
	if sl.stageTwo.Len() < sl.stageTwoCap {
		// stageTWO不满 放到stageTwo

		// 从stageOne移出
		sl.stageOne.Remove(v)
		item.stage = STAGE_TWO
		// 放到stageTWO
		// 重新放入map 因为这里是插入链表2 会有新的地址
		sl.data[item.key] = sl.stageTwo.PushFront(item)
		return
	}

	// stageTwo满了 则需要淘汰数据 淘汰stageTwo的数据
	back := sl.stageTwo.Back()
	backItem := back.Value.(*storeItem)
	// 将stageTwo的back 换到 stageOne 的头部
	// back的元素换成新元素
	// 这里没有第三变量 如何转换成功过？
	// 这里不能 分成两段交换 两段交换 会都指向 item = backitem = item
	//*backItem = *item
	//*item = *backItem
	*backItem, *item = *item, *backItem // ????? https://juejin.cn/post/6844904131530866695
	backItem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	// map重新放入
	sl.data[item.key] = v
	sl.data[backItem.key] = back

	// 当前元素数据 换到stageTwo 并放在stageTwo头部
	sl.stageOne.MoveToFront(v)
	sl.stageTwo.MoveToFront(back)
}

/*
	如果sl满了 找到要被驱逐的 驱逐stageOne的最后一个
		但是暂时不驱逐 要与windowLRU的访问频次判断
*/
func (sl *segmentedLRU) victim() *storeItem {
	if sl.Len() < sl.stageTwoCap+sl.stageOneCap {
		return nil
	}
	back := sl.stageOne.Back()
	backItem := back.Value.(*storeItem)
	//delete(sl.data,backItem.key)
	//sl.stageOne.Remove(back)
	return backItem
}
