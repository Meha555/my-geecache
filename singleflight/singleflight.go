package singleflight

// // call 代表正在进行中，或已经结束的请求。
// type call struct {
// 	wg sync.WaitGroup
// 	// 调用的返回值
// 	retval interface{}
// 	// 调用的错误
// 	err error
// }

// // Batch 管理不同 key 的请求(call)。确保对于同一个 key 的多个请求，实际只执行一次。(注意不是boost.asio的AsyncAgent)
// // 官方有golang.org/x/sync/singleflight的实现
// type Batch struct {
// 	m sync.Map
// }

// // Call 的作用就是，针对相同的 key，无论 Do 被调用多少次，函数 fn 都只会被调用一次，等待 fn 调用结束了，返回返回值或错误。
// // 这里使用 DCL 和 sync.Map 的 LoadOrStore 方法确保并发安全
// // REVIEW 有疑问，假设线程A下一步就要执行到c.wg.Add(1)，但此时线程B执行了LoadOrStore发现是pending == true于是 执行c.wg.Wait()。但是因为A还没有执行c.wg.Add(1)，导致B的Wait会立即结束，导致返回两个空值【这个问题其实是call的构造不完整，真正完整的构造是希望fn的返回值能和retval与err进行关联，而这无法在构造call对象时实现，所以这个问题无解，除非有std::async+std::future的功能，在go中可以通过channel实现】
// func (g *Batch) Call(key string, fn func() (interface{}, error)) (interface{}, error) {
// 	// 使用 LoadOrStore 确保并发安全
// 	cl, pending := g.m.LoadOrStore(key, &call{})
// 	c := cl.(*call)

// 	// 如果已经有正在进行的调用，则等待
// 	if pending {
// 		c.wg.Wait()
// 		return c.retval, c.err
// 	}

// 	// 发起调用
// 	c.wg.Add(1)
// 	c.retval, c.err = fn()
// 	c.wg.Done()
// 	g.m.Delete(key)

// 	return c.retval, c.err
// }

// result 就是future
type result struct {
	val interface{}
	err error
}

type call struct {
	res   result
	done chan struct{}
}
// request就是promise
type request struct {
	key   string
	fn    func() (interface{}, error)
	resCh chan result
}

type Batch struct{ reqCh chan request }

func NewBatch() *Batch {
	g := &Batch{make(chan request)}
	go g.serve()
	return g
}

func (g *Batch) Close() { close(g.reqCh) }

func (g *Batch) Call(key string, fn func() (interface{}, error)) (interface{}, error) {
	// Create request for each key
	req := request{key, fn, make(chan result)}
	// Send to g.serve handle
	g.reqCh <- req
	// Wait for response
	ret := <-req.resCh
	return ret.val, ret.err
}

func (g *Batch) serve() {
	// 只有一个serve协程，所以无需加锁
	// Cache the results of each key
	pendings := make(map[string]*call)
	// handle each request
	for req := range g.reqCh {
		// 如果是当前没有处理过该key的请求，则执行
		if c, ok := pendings[req.key]; !ok {
			e := &call{
				done: make(chan struct{}),
			}
			pendings[req.key] = e
			go e.exec(req)
			<-e.done
			delete(pendings, req.key)
		} else { // 否则等待结果
			go c.wait(req.resCh)
		}
	}
}

func (c *call) exec(req request) {
	c.res.val, c.res.err = req.fn()
	req.resCh <- c.res
	close(c.done)
}

func (c *call) wait(resCh chan<- result) {
	resCh <- c.res
}
