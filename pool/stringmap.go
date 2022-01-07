package pool

import "sync"

type StringMapPool struct {
	sync.Pool
}

func NewStringMapPool() StringMapPool {
	return StringMapPool{
		sync.Pool{New: func() interface{} {
			return make(map[string]string)
		}}}
}

func (mp *StringMapPool) Get() map[string]string {
	m, ok := mp.Pool.Get().(map[string]string)
	if !ok {
		return nil
	}
	for k := range m {
		delete(m, k)
	}
	return m
}

func (mp *StringMapPool) Put(m map[string]string) {
	mp.Pool.Put(m)
}
