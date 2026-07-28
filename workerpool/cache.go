package workerpool

import "github.com/Whitfrost21/workerpool/lru"

type cacheGetreq[k comparable, v any] struct {
	key  k
	resp chan cacheGetresp[v]
}
type cacheGetresp[v any] struct {
	value v
	found bool
}

type cacheSetreq[k comparable, v any] struct {
	key   k
	value v
}

type CacheActor[k comparable, v any] struct {
	getreq chan cacheGetreq[k, v]
	setreq chan cacheSetreq[k, v]
	cache  *lru.LRUmap[k, v]
}

func NewCacheActor[k comparable, v any](cache *lru.LRUmap[k, v]) *CacheActor[k, v] {
	ca := &CacheActor[k, v]{
		getreq: make(chan cacheGetreq[k, v]),
		setreq: make(chan cacheSetreq[k, v]),
		cache:  cache,
	}
	go ca.run()
	return ca
}

func (c *CacheActor[k, v]) run() {
	for {
		select {
		case req := <-c.getreq:
			val, ok := c.cache.Get(req.key)
			req.resp <- cacheGetresp[v]{value: val, found: ok}
		case req := <-c.setreq:
			c.cache.Put(req.key, req.value)
		}
	}
}

func (c *CacheActor[k, v]) Get(key k) (v, bool) {
	resp := make(chan cacheGetresp[v])
	c.getreq <- cacheGetreq[k, v]{key: key, resp: resp}
	r := <-resp
	return r.value, r.found
}

func (c *CacheActor[k, v]) Set(key k, value v) {
	c.setreq <- cacheSetreq[k, v]{key: key, value: value}
}
