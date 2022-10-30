package v1

import (
	"container/list"
	"sync"
)

/*
	将 lru - bloomfilter - s2lru组合在一起
*/
/*
	这里做并发控制
*/
type Cache struct {
	lru         *windowLRU
	slru        *segmentedLRU
	cmSketch    *cmSketch
	bloomFilter *BloomFilter
	m           sync.RWMutex
	t           int32                    // 当前总共操作get的次数
	threshold   int32                    // 重置布隆过滤器和保鲜的阈值
	data        map[uint64]*list.Element // 大map 管理lru 和 slru的所有数据
}

func (c *Cache) Set(key, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

// 返回是否成功设置进key value
func (c *Cache) set(key, value interface{}) bool {
	if key == nil {
		return false
	}
	keyHash, conflictHash := c.keyToHash(key)
	// 放入lru
	item := storeItem{key: keyHash, value: value, stage: 0, conflict: conflictHash}
	evictedItem, evicted := c.lru.add(item)

	// 没有驱逐的 不满 返回
	if !evicted {
		return true
	}
	// 有被驱逐的

	// 存在 则从主内存中获取要淘汰的
	victim := c.slru.victim()
	if victim == nil {
		// 主内存没满 lru淘汰的可以直接进主内存
		c.slru.add(evictedItem)
		return true
	}
	// 满了
	// 布隆过滤器判断是否存在
	allowKey := c.bloomFilter.Allow(uint32(evictedItem.key))
	if !allowKey {
		return true
	}
	// 判断访问频次
	slruVC := c.cmSketch.Estimate(victim.key)
	lruVC := c.cmSketch.Estimate(evictedItem.key)
	if slruVC > lruVC {
		// 主内存的高 则淘汰lru的 即不把lru的放入主内存
		return true
	}

	// 把lru的放入主内存 淘汰slru的
	// 上面slru.victim() 没有真正把元素驱逐 这里进入 也会去置换主内存 stageOne的尾部
	c.slru.add(evictedItem)
	return true

}

// 读锁
/*
	返回 获取到的值 和 是否获取成功
*/
func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.get(key)
}

// 布隆过滤器重置 + slru保鲜
func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.t++
	if c.t == c.threshold {
		// 重置布隆过滤器和执行保鲜
		// 这里重置过滤器可以使用 AB 过滤器 防止在重置时出错
		c.cmSketch.Reset()
		c.t = 0
	}
	// 从lru中获取  增加访问频次

	// 没有 看布隆过滤器中有没有
	// 没有 则就是没有

	// 有

	// 从slru中获取  增加访问频次

	// 没有 则 没有
}

func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {

}
