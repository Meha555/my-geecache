package main

import (
	"flag"
	"fmt"
	"geecache"
	"geecache/consistenthash"
	"geecache/network"
	"log"
	"net/http"
)

var mockDB = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func createGroup() *geecache.Group {
	return geecache.NewGroup("scores", 2<<10, geecache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Printf("[SlowDB] search key: %v", key)
			if v, ok := mockDB[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s does not exist", key)
		}))
}

// 来启动缓存服务器：创建 HTTPPool，添加节点信息，注册到 group 中，启动 HTTP 服务（共3个端口，8001/8002/8003），用户不感知。
func startCacheServer(addr string, addrs []consistenthash.NodeID, gee *geecache.Group) {
	cacheServer := network.NewCacheServer(addr)
	cacheServer.AddPeers(addrs...)
	gee.RegisterPeerPicker(cacheServer)
	log.Println("geecache is running at", addr)
	log.Fatal(http.ListenAndServe(addr[7:], cacheServer))
}

// 用来启动一个 API 服务（端口 9999），与用户进行交互，用户感知
func startAPIServer(apiAddr string, group *geecache.Group) {
	http.Handle("/api", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			view, err := group.Get(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(view.ByteSlice())
		}))
	log.Println("fontend server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], nil))
}

func main() {
	var port int
	var api bool
	flag.IntVar(&port, "port", 8001, "Geecache server port")
	flag.BoolVar(&api, "api", false, "Start a api server")
	flag.Parse()

	apiAddr := "http://localhost:9999"
	// 3个缓存服务器
	addrMap := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}

	var addrs []consistenthash.NodeID
	for _, v := range addrMap {
		addrs = append(addrs, consistenthash.NodeID(v))
	}
	// 创建缓存服务器中的db
	group := createGroup()
	if api {
		go startAPIServer(apiAddr, group)
	}
	startCacheServer(addrMap[port], addrs, group)
}
