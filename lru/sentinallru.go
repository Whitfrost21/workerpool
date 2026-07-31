// This is LRU cache implementation used with sentinals
//(which means head and tails are non-null nodes) for faster and better caching

package lru

type Node[k comparable, v any] struct {
	key   k
	value v
	next  *Node[k, v]
	prev  *Node[k, v]
}

type LRUmap[k comparable, v any] struct {
	capacity int
	cache    map[k]*Node[k, v]
	head     *Node[k, v]
	tail     *Node[k, v]
}

func New[k comparable, v any](capacity int) *LRUmap[k, v] {
	head := &Node[k, v]{}
	tail := &Node[k, v]{}
	head.next = tail
	tail.prev = head
	return &LRUmap[k, v]{
		capacity: capacity,
		cache:    make(map[k]*Node[k, v]),
		head:     head,
		tail:     tail,
	}
}
func (l *LRUmap[k, v]) remove(n *Node[k, v]) {
	n.next.prev = n.prev
	n.prev.next = n.next
}
func (l *LRUmap[k, v]) insertfront(curr *Node[k, v]) {
	curr.next = l.head.next
	curr.prev = l.head
	l.head.next.prev = curr
	l.head.next = curr
}

func (l *LRUmap[k, v]) Put(key k, value v) {
	curr, exists := l.cache[key]
	if exists {
		curr.value = value
		l.remove(curr)
		l.insertfront(curr)
		return
	} else {
		if len(l.cache) == l.capacity {
			lastnode := l.tail.prev
			l.remove(lastnode)
			delete(l.cache, lastnode.key)
		}
		n := &Node[k, v]{key: key, value: value, next: l.head.next, prev: l.head}
		l.insertfront(n)
		l.cache[key] = n
		return
	}
}

func (l *LRUmap[k, v]) Get(key k) (value v, b bool) {
	n, exists := l.cache[key]
	if !exists {
		var zero v
		return zero, false
	}
	l.remove(n)
	l.insertfront(n)
	return n.value, true
}

// func main() {
// 	mycache := new[int, string](2)
// 	mycache.put(1, "leo")
// 	fmt.Println(mycache.get(1))
// 	mycache.put(2, "erling")
// 	fmt.Println(mycache.get(1))
// 	mycache.put(3, "ages")
// 	fmt.Println(mycache)
// }
