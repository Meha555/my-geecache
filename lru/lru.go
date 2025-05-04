package lru

import (
	"container/list"
)

type Value interface {
	Size() int
}

type Cache struct {
	maxBytes  int64
	nbytes    int64
	ll        *list.List // 队头最新,队尾最旧
	cache     map[string]*list.Element
	OnEvicted func(key string, value Value)
}

type Entry struct {
	Key   string
	Value Value
}

func (e *Entry) Size() int64 {
	return int64(len(e.Key)) + int64(e.Value.Size())
}

func New(maxBytes int64, onEvicted func(key string, value Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		nbytes:    0,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

func (l *Cache) touch(ele *list.Element) {
	l.ll.MoveToFront(ele)
}

func (l *Cache) evict() {
	val := l.ll.Remove(l.ll.Back())
	kv := val.(*Entry)
	delete(l.cache, kv.Key)
	l.nbytes -= int64(len(kv.Key)) + int64(kv.Value.Size())
	if l.OnEvicted != nil {
		l.OnEvicted(kv.Key, kv.Value)
	}
}

func (l *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := l.cache[key]; ok {
		l.touch(ele)
		return ele.Value.(*Entry).Value, true
	}
	return nil, false
}

func (l *Cache) Put(key string, value Value) {
	if ele, ok := l.cache[key]; ok {
		kv := ele.Value.(*Entry)
		kv.Value = value
		l.nbytes += int64(value.Size()) - int64(kv.Value.Size())
		l.touch(ele)
	} else {
		l.cache[key] = l.ll.PushFront(&Entry{key, value})
		l.nbytes += int64(len(key)) + int64(value.Size())
	}
	for l.nbytes > l.maxBytes {
		l.evict()
	}
}

func (l *Cache) Len() int {
	return l.ll.Len()
}
