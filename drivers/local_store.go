package drivers

import (
	"encoding/base64"
	"encoding/binary"
	"github.com/jinglov/gomisc/logger"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
	"sync/atomic"
	"time"
)

type LocalStore struct {
	db       *leveldb.DB
	index    uint64
	wg       sync.WaitGroup
	exit     chan struct{}
	process  func(string, []byte)
	loopTime time.Duration
	log      logger.Logi
}

func NewLocalStore(path string, process func(string, []byte), loopTime time.Duration, log logger.Logi) (*LocalStore, error) {
	p := &LocalStore{}
	p.log = log
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	iter.Last()
	key := iter.Key()
	if key != nil {
		p.index = binary.BigEndian.Uint64(key)
	}
	if p.log != nil {
		p.log.Infof("local cache %s index is now at %d", path, p.index)
	}
	p.db = db
	p.process = process
	if loopTime.Seconds() < 5 {
		loopTime = 10 * time.Second
	}
	p.loopTime = loopTime
	return p, nil
}

func (p *LocalStore) Start() {
	p.exit = make(chan struct{})
	p.ProcessAll()
	p.wg.Add(1)
	go p.Loop()
}

func (p *LocalStore) Put(key string, value []byte) error {
	i := atomic.AddUint64(&p.index, 1)
	bk := make([]byte, 8)
	binary.BigEndian.PutUint64(bk, i)
	bk = append(bk, key...)
	v2 := make([]byte, base64.StdEncoding.EncodedLen(len(value)))
	base64.StdEncoding.Encode(v2, value)
	return p.db.Put(bk, v2, nil)
}

func (p *LocalStore) Loop() {
	defer p.wg.Done()
	tick := time.NewTicker(p.loopTime)
	for {
		select {
		case <-tick.C:
			p.ProcessAll()
		case <-p.exit:
			return
		}
	}
}

func (p *LocalStore) ProcessAll() {
	iter := p.db.NewIterator(nil, nil)
	for iter.Next() {
		v := iter.Value()
		if v == nil {
			break
		}
		value := make([]byte, base64.StdEncoding.DecodedLen(len(v)))
		n, err := base64.StdEncoding.Strict().Decode(value, v)
		if err != nil {
			if p.log != nil {
				p.log.Errorf("failed decode: %s", string(v))
			}
			continue
		}
		k := iter.Key()
		if len(k) < 8 {
			if p.log != nil {
				p.log.Errorf("failed key %d", k)
			}
			continue
		}
		p.process(string(k[8:]), value[0:n])
		if err := p.db.Delete(iter.Key(), nil); err != nil && p.log != nil {
			p.log.Errorf("failed to delete: %s", err.Error())
		}
	}
	iter.Release()
}

func (p *LocalStore) Stop() {
	close(p.exit)
	p.wg.Wait()
}

func (p *LocalStore) Close() {
	p.db.Close()
}
