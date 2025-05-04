package network

import (
	"fmt"
	"geecache"
	"geecache/consistenthash"
	pb "geecache/proto"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
)

const (
	// 节点间通讯地址的前缀，默认是 /_geecache/，那么 http://example.com/_geecache/ 开头的请求，就用于节点间的访问。因为一个主机上还可能承载其他的服务，加一段 Path 是一个好习惯。比如，大部分网站的 API 接口，一般以 /api 作为前缀。
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50
)

// CacheServer，作为承载节点间 HTTP 通信的核心数据结构
type CacheServer struct {
	sync.RWMutex
	// 当前节点的自身地址, e.g. "https://example.net:8000"
	selfURL string
	// 当前节点的API前缀
	basePath string

	// 一致性哈希根据具体的 key 选择节点来实现负载均衡
	peers *consistenthash.NodeMap
	// 映射远程节点与对应的 httpGetter。每一个远程节点对应一个 httpGetter，因为 httpGetter 与远程节点的地址 baseURL 有关。
	getters map[consistenthash.NodeID]*httpGetter
}

func NewCacheServer(addr string) *CacheServer {
	return &CacheServer{
		selfURL:  addr,
		basePath: defaultBasePath,
		peers:    consistenthash.New(defaultReplicas, nil),
		getters:  make(map[consistenthash.NodeID]*httpGetter),
	}
}

// ServeHTTP 负责处理所有HTTP请求 selfURL/<basepath>/<groupname>/<key>
func (p *CacheServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	log.Printf("[Server %s] %s : %s", p.selfURL, r.Method, r.URL.Path)
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
	// 用 protobuf 的目的非常简单，为了获得更高的性能。传输前使用 protobuf 编码，接收方再进行解码，可以显著地降低二进制传输的大小。另外一方面，protobuf 可非常适合传输结构化数据，便于通信字段的扩展。
	body, err := proto.Marshal(&pb.Response{Value: value.ByteSlice()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(body) // 这里可以用 w.Write(view.b) 代替，写入 http body 不会影响 cache 的值
}

// AddPeers 添加一个对端节点到本地节点注册表
func (p *CacheServer) AddPeers(peers ...consistenthash.NodeID) {
	p.Lock()
	defer p.Unlock()
	for _, peer := range peers {
		url, err := url.JoinPath(string(peer), p.basePath)
		if err != nil {
			panic(err)
		}
		p.peers.AddNodes(peer)
		p.getters[peer] = &httpGetter{remoteURL: url}
	}
}

// DelPeeker 从本地节点注册表中删除一个对端节点
func (p *CacheServer) DelPeeker(peer consistenthash.NodeID) {
	p.Lock()
	defer p.Unlock()
	if _, ok := p.getters[peer]; ok {
		p.peers.DelNode(peer)
		delete(p.getters, peer)
	}
}

func (p *CacheServer) PickPeer(key string) geecache.PeerGetter {
	p.RLock()
	defer p.RUnlock()
	nodeId := p.peers.GetNode(key)
	// 不要选到自己了,否则会自己请求自己导致无限递归
	// 哈希到自己也说明了这个key确实缓存未命中，因为能走到PickPeer就是本地缓存未命中
	if nodeId != consistenthash.NodeID(p.selfURL) {
		log.Printf("[Server %s] Pick peer %v", p.selfURL, nodeId)
		return p.getters[nodeId]
	}
	return nil
}

type httpGetter struct {
	// 将要访问的远程节点的地址，例如 http://example.com/_geecache/
	remoteURL string
}

func (g *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	url, err := url.JoinPath(g.remoteURL, url.QueryEscape(in.GetGroup()), url.QueryEscape(in.GetKey()))
	if err != nil {
		return err
	}
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", resp.Status)
	}

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	if err := proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}
