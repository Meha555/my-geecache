package geecache

import (
	"geecache/lru"
	"geecache/util"
	"sync"
)

type Cache struct {
	mtx        sync.Mutex
	lru        *lru.LRUCache
	cacheBytes int64
}

func (c *Cache) Get(key string) (value util.ByteView, ok bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	// 如果等于 nil 再创建实例。这种方法称之为延迟初始化(Lazy Initialization)，一个对象的延迟初始化意味着该对象的创建将会延迟至第一次使用该对象时。主要用于提高性能，并减少程序内存要求。
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}
	if val, ok := c.lru.Get(key); ok {
		return val.(util.ByteView), true
	}
	return
}

func (c *Cache) Put(key string, value util.ByteView) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}
	c.lru.Put(key, value)
}
