package lru

import (
	"reflect"
	"testing"
)

type String string

func (d String) Size() int {
	return len(d)
}

func TestGet(t *testing.T) {
	ele := Entry{
		"key1", String("1234"),
	}
	lru := New(ele.Size(), nil)
	lru.Put("key1", ele.Value)
	if v, ok := lru.Get("key1"); !ok || string(v.(String)) != "1234" {
		t.Fatalf("cache hit key1=1234 failed")
	}
	if _, ok := lru.Get("key2"); ok {
		t.Fatalf("cache miss key2 failed")
	}
}

func TestRemoveoldest(t *testing.T) {
	k1, k2, k3 := "key1", "key2", "k3"
	v1, v2, v3 := "value1", "value2", "v3"
	cap := len(k1 + k2 + v1 + v2)
	lru := New(int64(cap), nil)
	lru.Put(k1, String(v1))
	lru.Put(k2, String(v2))
	lru.Put(k3, String(v3))

	if _, ok := lru.Get("key1"); ok || lru.Len() != 2 {
		t.Fatalf("Removeoldest key1 failed")
	}
}

func TestOnEvicted(t *testing.T) {
	var keys []string
	callback := func(key string, value Value) {
		keys = append(keys, key)
	}
	lru := New(int64(10), callback)
	lru.Put("key1", String("123456"))
	lru.Put("k2", String("k2"))
	lru.Put("k3", String("k3"))
	lru.Put("k4", String("k4"))

	expect := []string{"key1", "k2"}

	if !reflect.DeepEqual(expect, keys) {
		t.Fatalf("Call OnEvicted failed, expect keys equals to %s", expect)
	}
	if lru.Len() != 2 {
		t.Fatalf("LRUCache Size() failed")
	}
}
