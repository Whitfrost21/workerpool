package workerpool

import (
	"github.com/Whitfrost21/workerpool/lru"
)

// claim that same job is already in-flight
type claimRequest[k comparable] struct {
	key  k
	resp chan bool
}

// request to retrive result from cache
type cacheGetreq[k comparable, v any] struct {
	key  k
	resp chan cacheGetresp[v]
}

// response of request
type cacheGetresp[v any] struct {
	value v
	found bool
}

// request to store a job result in cache
type cacheSetreq[k comparable, v any] struct {
	key   k
	value v
}

// CacheActor operates on cache (share memory by communicating instead mutex)
type CacheActor[k comparable, v any] struct {
	getreq     chan cacheGetreq[k, v]
	setreq     chan cacheSetreq[k, v]
	cache      *lru.LRUmap[k, v]
	claimReqs  chan claimRequest[k]
	releasereq chan k
	inflight   map[k]struct{}
}

func NewCacheActor[k comparable, v any](cache *lru.LRUmap[k, v]) *CacheActor[k, v] {
	ca := &CacheActor[k, v]{
		getreq:     make(chan cacheGetreq[k, v]),
		setreq:     make(chan cacheSetreq[k, v]),
		cache:      cache,
		claimReqs:  make(chan claimRequest[k]),
		releasereq: make(chan k),
		inflight:   make(map[k]struct{}),
	}
	go ca.run()
	return ca
}

// run handles the communication requests (store or retrive offcourse)
func (c *CacheActor[k, v]) run() {
	for {
		select {
		case req := <-c.getreq: //retrives (key,response) from channel
			val, ok := c.cache.Get(req.key)                    //retrive job result from cache
			req.resp <- cacheGetresp[v]{value: val, found: ok} //send back response(value,bool)

		case req := <-c.setreq: //retrives (key,value)from channel
			c.cache.Put(req.key, req.value) //store (key,value) in cache

		case req := <-c.claimReqs:
			_, cached := c.cache.Get(req.key)
			_, inflight := c.inflight[req.key]
			if cached || inflight {
				req.resp <- false
				continue
			}
			c.inflight[req.key] = struct{}{}
			req.resp <- true

		case key := <-c.releasereq:
			delete(c.inflight, key)
		}
	}
}

// get the value from cache
func (c *CacheActor[k, v]) Get(key k) (v, bool) {
	resp := make(chan cacheGetresp[v]) //empty channel for communication with CacheActor
	c.getreq <- cacheGetreq[k, v]{key: key, resp: resp}
	r := <-resp
	return r.value, r.found
}

// add result in cache
func (c *CacheActor[k, v]) Set(key k, value v) {
	c.setreq <- cacheSetreq[k, v]{key: key, value: value} //push key and value in channel(fire and forget)
}

func (c *CacheActor[k, v]) Tryclaim(key k) bool {
	resp := make(chan bool)
	c.claimReqs <- claimRequest[k]{key: key, resp: resp}
	return <-resp
}

func (c *CacheActor[k, v]) Release(key k) {
	c.releasereq <- key
}
