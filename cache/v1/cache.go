package v1

import (
	"container/list"
	"github.com/cespare/xxhash/v2"
	"sync"
	"unsafe"
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
// 返回 获取到的值 及 是否获取到key对应的值
func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.t++
	if c.t == c.threshold {
		// 重置布隆过滤器和执行保鲜
		// 这里重置过滤器可以使用 AB 过滤器 防止在重置时出错
		c.cmSketch.Reset()
		c.bloomFilter.reset()
		c.t = 0
	}

	// 直接从map中获取
	keyHash, conflictHash := c.keyToHash(key)
	// 放入布隆过滤器
	c.bloomFilter.Allow(uint32(keyHash))
	// 增加访问频次 表明key被访问过 即使没有
	c.cmSketch.Increment(keyHash)

	item, ok := c.data[keyHash]
	if !ok {
		// 没拿到

		return nil, false
	}
	// 有
	sItem := item.Value.(*storeItem)

	// 判断是否冲突 防止不同key但keyHash相同，hash冲突，如果不同 则冲突了
	if sItem.conflict != conflictHash {
		// 冲突
		return nil, false
	}

	// 判断数据所在阶段 将其在其lru中移到头部
	if sItem.stage == 0 {
		// 在lru中
		c.lru.get(item)
	} else {
		// 在slru
		c.slru.get(item)
	}

	return sItem.value, true
}

/*
	返回删除的值的冲突值 及 是否删除成功
*/
func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := c.keyToHash(key)
	// 判断key是否存在
	item, ok := c.data[keyHash]
	if !ok {
		// 没拿到
		return 0, false
	}

	// 存在
	sItem := item.Value.(*storeItem)

	// 判断是否冲突 哈希冲突
	// 防止keyHash冲突
	if conflictHash != 0 && (sItem.conflict != conflictHash) {
		// 冲突
		return 0, false
	}

	// 删除map中的key
	delete(c.data, keyHash)
	return sItem.conflict, true
	// 这里为什么不删除lru中的？ 如果依靠淘汰的方式 要很久才能淘汰出去
	// 不删除是为了 如果是高频的key 如果再次访问/add 可以继承之前的访问频次？
}

/*
	对各种类型的key使用两种不同的哈希函数做哈希计算
	返回值：
		return1: 用作对key的哈希后值
		return2:用作key的冲突值，防止return1哈希冲突的辅助值 解决哈希冲突
*/
func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

// 缺少更新操作
