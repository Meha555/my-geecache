package geecache

import (
	"fmt"
	pb "geecache/proto"
	"geecache/singleflight"
	"geecache/util"
	"log"
	"sync"
)

// Group 相当于redis里的db。
type Group struct {
	// db名称
	name string
	// 直接从数据源取数据，不走缓存
	srcGetter Getter
	// 本地缓存
	localCache *Cache
	// 远程缓存
	peerPicker PeerPicker
	// use singleflight.Batch to make sure that
	// each key is only fetched once
	loader *singleflight.Batch
}

var (
	mtx    sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, srcGetter Getter) *Group {
	if srcGetter == nil {
		panic("nil Getter")
	}
	g := &Group{
		name:       name,
		srcGetter:  srcGetter,
		localCache: &Cache{cacheBytes: cacheBytes},
		loader:     singleflight.NewBatch(),
	}
	mtx.Lock()
	defer mtx.Unlock()
	groups[name] = g
	return g
}

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mtx.RLock()
	g := groups[name]
	mtx.RUnlock()
	return g
}

func (g *Group) RegisterPeerPicker(picker PeerPicker) {
	if g.peerPicker != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peerPicker = picker
}

func (g *Group) Get(key string) (util.ByteView, error) {
	// 先从本地缓存中取值
	if val, ok := g.localCache.Get(key); ok {
		log.Printf("%s 命中本地缓存!", key)
		return val, nil
	}
	// 本地缓存未命中，继续尝试远程缓存
	// each key is only fetched once (either locally 打到数据库 or remotely 打到对端)
	// regardless of the number of concurrent callers.
	// 这个 CallOnce 只有在缓存没有命中的时候才会执行，并不会影响缓存的本身的性能，缓存没命中的时候必然要等待获取数据，要么等其他节点返回，要么等锁。
	val, err := g.loader.Call(key, func() (interface{}, error) {
		if g.peerPicker != nil {
			if val, err := g.getFromPeer(key); err == nil {
				log.Printf("%s 命中远程缓存!", key)
				return val, nil
			}
		}
		log.Printf("%s 未命中任何缓存,直接读数据源!", key)
		// 远程缓存也没命中，则直接从数据源取
		return g.getFromSouce(key)
	})
	if err == nil {
		return val.(util.ByteView), err
	}
	return util.ByteView{}, err
}

func (g *Group) getFromSouce(key string) (util.ByteView, error) {
	bytes, err := g.srcGetter.Get(key)
	if err != nil {
		return util.ByteView{}, err
	}
	value := util.ByteView{B: util.CloneBytes(bytes)} // 这里bytes是切片，所以不会深拷贝，所以这里手动深拷贝来防止底层数据源修改了数据导致util.ByteView中持有的数据也被修改
	g.populateCache(key, value)
	return value, err
}

func (g *Group) getFromPeer(key string) (util.ByteView, error) {
	peer := g.peerPicker.PickPeer(key)
	if peer == nil {
		return util.ByteView{}, fmt.Errorf("no peer for key: %s", key)
	}
	var resp pb.Response
	err := peer.Get(&pb.Request{
		Group: g.name,
		Key:   key,
	}, &resp)
	if err != nil {
		return util.ByteView{}, err
	}
	// 对于远程节点，不应该更新其远程缓存。因为分布式缓存的目的是不同key缓存在不同的节点上，增加总的吞吐量。如果大家转发请求后，都再备份一次，每台机器上都缓存了相同的数据，就失去意义了。每个节点缓存1G数据，理论上10个节点总共可以缓存10G不同的数据。
	// 当然对于热点数据，每个节点拿到值后，本机备份一次是有价值的，增加热点数据的吞吐量。groupcache 的原生实现中，有1/10的概率会在本机存一次。这样10个节点，理论上可以缓存9G不同的数据，算是一种取舍。
	return util.ByteView{B: util.CloneBytes(resp.Value)}, err // 这里bytes是切片，所以不会深拷贝，所以这里手动深拷贝来防止底层数据源修改了数据导致util.ByteView中持有的数据也被修改
}

// 更新本地缓存
func (g *Group) populateCache(key string, value util.ByteView) {
	g.localCache.Put(key, value)
}
