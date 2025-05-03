package singleflight

import "sync"

// call 代表正在进行中，或已经结束的请求。
type call struct {
	wg sync.WaitGroup
	// 调用的返回值
	retval interface{}
	// 调用的错误
	err error
}

// Batch 管理不同 key 的请求(call)。确保对于同一个 key 的多个请求，实际只执行一次。(注意不是boost.asio的AsyncAgent)
// 官方有golang.org/x/sync/singleflight的实现
type Batch struct {
	mtx sync.Mutex
	m   map[string]*call
}

// Call 的作用就是，针对相同的 key，无论 Do 被调用多少次，函数 fn 都只会被调用一次，等待 fn 调用结束了，返回返回值或错误。
func (g *Batch) CallOnce(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mtx.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	// 如果有正在进行的调用,则等待
	if c, ok := g.m[key]; ok {
		g.mtx.Unlock()
		c.wg.Wait()
		return c.retval, c.err
	}
	// 否则发起调用
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mtx.Unlock()

	c.retval, c.err = fn()
	c.wg.Done()
	g.mtx.Lock()
	delete(g.m, key)
	g.mtx.Unlock()

	return c.retval, c.err
}

/*
type result struct {
	val interface{}
	err error
}

type entry struct {
	res   result
	ready chan struct{}
}

type request struct {
	key      string
	fn       func() (interface{}, error)
	response chan result
}

type Batch struct{ requests chan request }

func New() *Batch {
	g := &Batch{make(chan request)}
	go g.serve()
	return g
}

func (g *Batch) Close() { close(g.requests) }

func (g *Batch) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// Create request for each key
	req := request{key, fn, make(chan result)}
	// Send to g.serve handle
	g.requests <- req
	// Wait for response
	ret := <-req.response
	return ret.val, ret.err
}

func (g *Batch) serve() {
	// Cache the results of each key
	cache := make(map[string]*entry)
	// handle each request
	for r := range g.requests {
		if e, ok := cache[r.key]; !ok {
			e := &entry{
				ready: make(chan struct{}),
			}
			cache[r.key] = e
			go e.call(r)
		} else {
			go e.deliver(r.response)
		}
		//I didn't implement a good way to delete the cache
	}
}

func (e *entry) call(req request) {
	e.res.val, e.res.err = req.fn()
	req.response <- e.res
	close(e.ready)
}

func (e *entry) deliver(resp chan<- result) {
	<-e.ready
	resp <- e.res
}
*/
