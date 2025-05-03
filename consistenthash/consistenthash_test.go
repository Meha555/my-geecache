package consistenthash

import (
	"strconv"
	"strings"
	"testing"
)

func TestHashing(t *testing.T) {
	hash := New(3, func(key []byte) uint32 {
		nums := strings.SplitN(string(key), "-", 2)
		var code = 0
		for _, num := range nums {
			i, _ := strconv.Atoi(num)
			code = code*10 + i
		}
		return uint32(code)
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2-0, 4-0, 6-0, 2-1, 4-1, 6-1, 2-2, 4-2, 6-2
	hash.AddNodes("6", "4", "2")

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"50": "6",
	}

	for k, v := range testCases {
		if hash.GetNode(k) != NodeID(v) {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// AddNodess 5-0, 5-1, 5-2
	hash.AddNodes("5")

	// 57 should now map to 5.
	testCases["50"] = "5"

	for k, v := range testCases {
		if hash.GetNode(k) != NodeID(v) {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}
}

func TestConsistency(t *testing.T) {
	hash1 := New(1, nil)
	hash2 := New(1, nil)

	hash1.AddNodes("Bill", "Bob", "Bonny")
	hash2.AddNodes("Bob", "Bonny", "Bill")

	if hash1.GetNode("Ben") != hash2.GetNode("Ben") {
		t.Errorf("Fetching 'Ben' from both hashes should be the same")
	}

	hash2.AddNodes("Becky", "Ben", "Bobby")

	if hash1.GetNode("Ben") != hash2.GetNode("Ben") ||
		hash1.GetNode("Bob") != hash2.GetNode("Bob") ||
		hash1.GetNode("Bonny") != hash2.GetNode("Bonny") {
		t.Errorf("Direct matches should always return the same entry")
	}

	hash2.DelNode("Bob")

	if hash1.GetNode("Ben") != hash2.GetNode("Ben") ||
		hash1.GetNode("Becky") != hash2.GetNode("Becky") ||
		hash1.GetNode("Bonny") != hash2.GetNode("Bonny") {
		t.Errorf("Direct matches should always return the same entry")
	}
}
