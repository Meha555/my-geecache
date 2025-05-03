package network

import (
	"fmt"
	"geecache"
	"geecache/consistenthash"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	// 节点间通讯地址的前缀，默认是 /_geecache/，那么 http://example.com/_geecache/ 开头的请求，就用于节点间的访问。因为一个主机上还可能承载其他的服务，加一段 Path 是一个好习惯。比如，大部分网站的 API 接口，一般以 /api 作为前缀。
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50
)

// HTTPPool，作为承载节点间 HTTP 通信的核心数据结构
type HTTPPool struct {
	sync.RWMutex
	// this peer's base URL, e.g. "https://example.net:8000"
	selfURL  string
	basePath string

	// 一致性哈希根据具体的 key 选择节点来实现负载均衡
	peers *consistenthash.NodeMap
	// 映射远程节点与对应的 httpGetter。每一个远程节点对应一个 httpGetter，因为 httpGetter 与远程节点的地址 baseURL 有关。
	getters map[consistenthash.NodeID]*httpGetter
}

func NewHTTPPool(addr string) *HTTPPool {
	return &HTTPPool{
		selfURL:  addr,
		basePath: defaultBasePath,
		peers:    consistenthash.New(defaultReplicas, nil),
		getters:  make(map[consistenthash.NodeID]*httpGetter),
	}
}

// ServeHTTP 负责处理所有HTTP请求 /<basepath>/<groupname>/<key>
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s : %s", r.Method, r.URL.Path)
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) < 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	key := parts[1]

	group := geecache.GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	value, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream") // 表明是二进制流
	w.Write(value.ByteSlice())                                 // 这里可以用 w.Write(view.b) 代替，写入 http body 不会影响 cache 的值
}

func (p *HTTPPool) AddPeers(peers ...consistenthash.NodeID) {
	p.Lock()
	defer p.Unlock()
	for _, peer := range peers {
		url, err := url.JoinPath(string(peer), defaultBasePath)
		if err != nil {
			panic(err)
		}
		p.peers.AddNodes(peer)
		p.getters[peer] = &httpGetter{remoteURL: url}
	}
}

func (p *HTTPPool) DelPeeker(peer consistenthash.NodeID) {
	p.Lock()
	defer p.Unlock()
	if _, ok := p.getters[peer]; ok {
		p.peers.DelNode(peer)
		delete(p.getters, peer)
	}
}

func (p *HTTPPool) PickPeer(key string) geecache.PeerGetter {
	p.RLock()
	defer p.RUnlock()
	nodeId := p.peers.GetNode(key)
	if nodeId != consistenthash.NodeID(p.selfURL) {
		log.Printf("Pick peer %v", nodeId)
		return p.getters[nodeId]
	}
	return nil
}

func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.selfURL, fmt.Sprintf(format, v...))
}

type httpGetter struct {
	// 将要访问的远程节点的地址，例如 http://example.com/_geecache/
	remoteURL string
}

func (g *httpGetter) Get(group string, key string) ([]byte, error) {
	url, err := url.JoinPath(g.remoteURL, url.QueryEscape(group), url.QueryEscape(key))
	if err != nil {
		return nil, err
	}
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %v", resp.Status)
	}

	var nodeId []byte
	_, err = resp.Body.Read(nodeId)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}
	return nodeId, nil
}
