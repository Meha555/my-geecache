package consistenthash

import (
	"hash/crc32"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Hash func(data []byte) uint32

type NodeID string

// NodeMap contains all hashed keys
type NodeMap struct {
	sync.Mutex
	// 哈希函数
	hasher Hash
	// 虚拟节点倍数（一个真实节点对应replicas个虚拟节点）
	replicas int
	// 虚拟节点哈希环（有序）
	ring []uint32
	// 虚拟节点和真实节点的映射表 <虚拟节点哈希值, 真实节点名称>
	nodeMap map[uint32]NodeID
}

func New(replicas int, fn Hash) *NodeMap {
	m := &NodeMap{
		hasher:   fn,
		replicas: replicas,
		ring:     make([]uint32, 0), // REVIEW 切片也是引用类型，不需要初始化吗
		nodeMap:  make(map[uint32]NodeID),
	}
	if m.hasher == nil {
		m.hasher = crc32.ChecksumIEEE
	}
	return m
}

// AddNodes adds some nodes to the hash.
// NOTE 可能会造成部分key的哈希值变化，导致哈希值变化的这部分数据会缓存不命中。这需要分布式一致性算法来解决。
func (m *NodeMap) AddNodes(nodes ...NodeID) {
	if len(nodes) == 0 {
		return
	}
	m.Lock()
	defer m.Unlock()
	var builder strings.Builder
	for _, node := range nodes {
		for i := range m.replicas {
			builder.WriteString(string(node))
			builder.WriteString("-")
			builder.WriteString(strconv.Itoa(i))
			hash := m.hasher([]byte(builder.String()))
			m.nodeMap[hash] = node
			m.ring = append(m.ring, hash)
			builder.Reset()
		}
	}
	slices.Sort(m.ring)
}

// DelNode removes a node from the hash. It will remove all the virtual nodes of the node.
func (m *NodeMap) DelNode(node NodeID) {
	m.Lock()
	defer m.Unlock()
	virtualNodes := make(map[uint32]struct{}, m.replicas)
	var builder strings.Builder
	for i := range m.replicas {
		builder.WriteString(string(node))
		builder.WriteString("-")
		builder.WriteString(strconv.Itoa(i))
		hash := m.hasher([]byte(builder.String()))
		delete(m.nodeMap, hash)
		virtualNodes[hash] = struct{}{}
	}
	m.ring = slices.DeleteFunc(m.ring, func(i uint32) bool {
		_, ok := virtualNodes[i]
		return ok
	})
}

// Get gets the closest node in the hash to the provided key.
func (m *NodeMap) GetNode(key string) (node NodeID) {
	m.Lock()
	defer m.Unlock()
	if len(m.ring) == 0 {
		return
	}

	hash := m.hasher([]byte(key))
	// 找到环上最近的虚拟节点
	idx := sort.Search(len(m.ring), func(i int) bool {
		return m.ring[i] >= hash
	})
	// 如果 idx == len(m.keys)，说明应选择 m.keys[0]，因为 m.keys 是一个环状结构，所以用取余数的方式来处理这种情况。
	// 比较大的值会全塞到第一个节点，此时应该增大replicas的值
	node = m.nodeMap[m.ring[idx%len(m.ring)]]
	return
}

/*
采用写时复制的版本：这里只有修改values即Add或者Remove的时候需要加锁，但是读取values如Get是不需要加锁的，保证了只会有一个并发单位正在修改。同时修改的过程并不是直接对values进行修改，而是复制了一份副本，修改完成后再原子store。同样的，Get是原子拷贝了values的副本，可以有多个并发单位操作自己看到的副本，但是这样会带来一定的数据一致性问题（比如某时刻有并发单位执行了remove或者add操作，修改了values，但是其它get操作不知道，拿到的是过时的副本）。这样的好处就是提高了并发量，同一时刻只能有一个并发单位执行修改操作，但是可以有多个单位执行读操作。为了保证一致性问题，最简单的操作就是不用写时复制，改成读写锁，但是这样并发性能会有所损失，更高级的可能就是采用一些一致性算法了吧。

type Hash func(data []byte)uint32

type Map struct {
	sync.Mutex
	// 哈希函数
	hash Hash
	// 虚拟节点倍数
	replicas int

	// 原子地存取 keys 和 hashMap
	values atomic.Value // values
}

type values struct {
	// 哈希环
	keys []int
	// 虚拟节点与真实节点的映射
	hashMap map[int]string
}

func NewMap(replicas int, hashFunc Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash: hashFunc,
	}
	m.values.Store(&values{
		hashMap: make(map[int]string),
	})
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// 添加节点
func (m *Map) Add(keys ...string) {
	m.Lock()
	defer m.Unlock()
	newValues := m.loadValues()
	for _, key := range keys {
		// 对每个 key(节点) 创建 m.replicas 个虚拟节点
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			newValues.keys = append(newValues.keys, hash)
			newValues.hashMap[hash] = key
		}
	}
	sort.Ints(newValues.keys)
	m.values.Store(newValues)
}

func (m *Map) Get(key string) string {
    values := m.loadValues()
	if len(values.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(values.keys), func(i int) bool {
		return values.keys[i] >= hash
	})
	// 如果 idx == len(m.keys)，说明应选择 m.keys[0]，
	// 因为 m.keys 是一个环状结构，用取余数的方式来处理这种情况
	return values.hashMap[values.keys[idx % len(values.keys)]]
}

func (m *Map) Remove(key string) {
	m.Lock()
	defer m.Unlock()
	newValues := m.loadValues()

	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
		idx := sort.SearchInts(newValues.keys, hash)
		if newValues.keys[idx] != hash {
			return
		}
		newValues.keys = append(newValues.keys[:idx], newValues.keys[idx+1:]...)
		delete(newValues.hashMap, hash)
	}

	m.values.Store(newValues)
}

func (m *Map) loadValues() *values {
    return m.values.Load().(*values)
}

func (m *Map) copyValues() *values {
	oldValues := m.loadValues()
	newValues := &values{
		keys:    make([]int, len(oldValues.keys)),
		hashMap: make(map[int]string),
	}
	copy(newValues.keys, oldValues.keys)
	for k, v := range oldValues.hashMap {
		newValues.hashMap[k] = v
	}
	return newValues
}
*/
