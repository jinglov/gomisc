package pool

import "sync"

type MapPool struct {
	sync.Pool
}

func NewMapPool() MapPool {
	return MapPool{
		sync.Pool{New: func() interface{} {
			return make(map[string]interface{})
		}}}
}

func (mp *MapPool) Get() map[string]interface{} {
	m, ok := mp.Pool.Get().(map[string]interface{})
	if !ok {
		return nil
	}
	for k := range m {
		delete(m, k)
	}
	return m
}

func (mp *MapPool) Put(m map[string]interface{}) {
	mp.Pool.Put(m)
}
