package drivers

import (
	"github.com/aerospike/aerospike-client-go"
	"github.com/jinglov/gomisc/monitor"
	"github.com/jinglov/gomisc/random"
	"testing"
	"time"
)

var hosts = []string{"172.25.23.57:3000"}

func InitAs() (*AsStore, error) {
	return NewAsV2(hosts, "fireeyes", WithAsTimeout(500*time.Millisecond),
		WithAsWriteTimout(40*time.Millisecond),
		WithAsReadTimeout(40*time.Millisecond))
}

func TestAsConnect(t *testing.T) {
	asvec := monitor.NewAsMetrics("ac", "test", "fireeyes")
	db, err := NewAsV2(hosts, "fireeyes",
		WithAsTimeout(1*time.Second),
		WithAsMonitor(asvec),
		WithAsQueueSize(1024),
	)
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

func TestAerospikeConnect_Sets(t *testing.T) {
	AerospikeDb, err := InitAs()
	if err != nil {
		t.Error(err)
		return
	}
	m := aerospike.BinMap{
		"k1": "v1",
		"k3": 3,
	}
	err = AerospikeDb.Sets("t1", "test", m)
	if err != nil {
		t.Error(err)
	}
}

func TestAerospikeConnect_Get(t *testing.T) {
	AerospikeDb, err := InitAs()
	if err != nil {
		t.Error(err)
		return
	}
	m, err := AerospikeDb.Get("t1", "test")
	if err != nil {
		t.Error(err)
	}
	t.Log(m)
}

func TestAsStore_PutMap(t *testing.T) {
	AerospikeDb, err := InitAs()
	if err != nil {
		t.Error(err)
		return
	}
	policy1 := aerospike.NewWritePolicy(0, 5)
	policy2 := aerospike.NewWritePolicy(0, 10)
	AerospikeDb.PutMap("m1", "k1", "bin1", "10s", "10", policy1)
	AerospikeDb.PutMap("m1", "k1", "bin1", "60s", "60", policy2)
}

func TestAsStore_Append(t *testing.T) {
	AerospikeDb, err := InitAs()
	if err != nil {
		t.Error(err)
		return
	}
	policy2 := aerospike.NewWritePolicy(0, 1000)

	a, e := AerospikeDb.AppendUniq("l1", "k1", "bin3", "v1", policy2)
	t.Log(a)
	t.Log(e)
	c, e := AerospikeDb.GetListLen("l1", "k1", "bin3")
	t.Log(c)
	t.Log(e)
}

func TestAsStore_RemoveUniq(t *testing.T) {
	AerospikeDb, err := InitAs()
	if err != nil {
		t.Error(err)
		return
	}
	policy2 := aerospike.NewWritePolicy(0, 1000)

	a, e := AerospikeDb.AppendUniq("l1", "k1", "bin3", "v1", policy2)
	AerospikeDb.AppendUniq("l1", "k1", "bin3", "v2", policy2)
	AerospikeDb.AppendUniq("l1", "k1", "bin3", "v3", policy2)
	AerospikeDb.AppendUniq("l1", "k1", "bin3", "v4", policy2)
	AerospikeDb.AppendUniq("l1", "k1", "bin3", "v5", policy2)
	t.Log(a)
	t.Log(e)
	c, e := AerospikeDb.GetListLen("l1", "k1", "bin3")
	t.Log(c)
	t.Log(e)
	r, e := AerospikeDb.RemoveList("l1", "k1", "bin3", []string{"v1", "v5"}, policy2)
	t.Log(r)
	t.Log(e)
	d, e := AerospikeDb.GetList("l1", "k1", "bin3")
	t.Log(d)
	t.Log(e)
	c, e = AerospikeDb.GetListLen("l1", "k1", "bin3")
	t.Log(c)
	t.Log(e)
}

func TestConnect2(t *testing.T) {
	asvec := monitor.NewAsMetrics("ac", "test", "fireeyes")
	db, err := NewAsV2([]string{"192.168.57.7:3000", "192.168.57.8:3000", "192.168.57.9:3000"}, "test",
		WithAsTimeout(50*time.Millisecond),
		WithAsMonitor(asvec),
		WithAsQueueSize(512),
	)
	if err != nil {
		t.Error(err)
		return
	}
	m := aerospike.BinMap{
		"k1": "v1",
		"k3": 3,
	}
	ch := make(chan bool, 100)
	i := 0
	e := 0
	for k := 0; k < 100; k++ {
		ch <- true
	}
	for <-ch {
		i++
		t.Log(i)
		go func() {
			for {
				key := random.String(8)
				err := db.Sets("t1", key, m)
				if err != nil {
					ch <- true
					e++
					// if i%10 == 0 {
					t.Errorf("seq=%d , err:%s", e, err.Error())
					return
					// }
				}
			}
		}()
	}
	t.Log(db)
}
