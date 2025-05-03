package geecache

import pb "geecache/proto"

// PeerPicker is the interface that must be implemented to locate
// the peer that owns a specific key.
type PeerPicker interface {
	PickPeer(key string) PeerGetter
}

// PeerGetter is the interface that must be implemented by a peer.
// PeerGetter 就对应于流程中的 HTTP 客户端
type PeerGetter interface {
	// 用于从对应 group 查找缓存值（远程版本的Get）
	Get(in *pb.Request, out *pb.Response) error
}
