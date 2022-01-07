package drivers

import (
	"errors"
	"github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"github.com/jinglov/gomisc/monitor"
	"strconv"
	"strings"
	"time"
)

type AsStore struct {
	C          *aerospike.Client
	namespace  string
	monitorVec *monitor.AsMetrics
}

var (
	ErrNamespaceNil = errors.New("namespace is nil")
)

func NewAs(monitorVec *monitor.AsMetrics, hosts []string, args ...string) (*AsStore, error) {
	if len(args) == 0 {
		return nil, ErrNamespaceNil
	}
	as := &AsStore{monitorVec: monitorVec, namespace: args[0]}
	hs := make([]*aerospike.Host, len(hosts))
	for i, h := range hosts {
		host := strings.Split(h, ":")
		port := 0
		if len(host) == 2 {
			var err error
			if port, err = strconv.Atoi(host[1]); err != nil {
				return nil, err
			}
		}
		if port == 0 {
			port = 3000
		}
		hs[i] = aerospike.NewHost(host[0], port)
	}
	policy := aerospike.NewClientPolicy()
	if len(args) > 1 {
		policy.User = args[1]
	}
	if len(args) > 2 {
		policy.Password = args[2]
	}
	client, err := aerospike.NewClientWithPolicyAndHost(policy, hs...)
	if err != nil {
		return nil, err
	}
	as.C = client
	return as, nil
}

func NewAsV2(hosts []string, namespace string, opts ...AsOpt) (asp *AsStore, returnErr error) {
	o := newAsOption()
	for _, opt := range opts {
		opt(o)
	}
	as := &AsStore{
		monitorVec: o.monitorVec,
		namespace:  namespace,
	}
	hs := make([]*aerospike.Host, len(hosts))
	for i, h := range hosts {
		host := strings.Split(h, ":")
		port := 0
		if len(host) == 2 {
			var err error
			if port, err = strconv.Atoi(host[1]); err != nil {
				return nil, err
			}
		}
		if port == 0 {
			port = 3000
		}
		hs[i] = aerospike.NewHost(host[0], port)
	}
	policy := aerospike.NewClientPolicy()

	// authMode身份验证模式，社区版不用设置
	policy.AuthMode = o.authMode
	// 身份验证帐号，社区版不用设置
	policy.User = o.user
	// 身份验证密码，社区版不用设置
	policy.Password = o.password
	// 集群ID，社区版不用设置
	policy.ClusterName = o.clusterName
	// 初始化连接时超时时间
	policy.Timeout = o.timeout
	// 连接空闲超时时间，当超过空闲时间没被使用的连接会被回收
	policy.IdleTimeout = o.idelTimeout
	// 身份验证超时时间，一般用于外部连接
	policy.LoginTimeout = o.loginTimeout
	// 指定连接队列per node大小
	policy.ConnectionQueueSize = o.connectionQueueSize
	defer func() {
		as.MetricsError("init", returnErr)
	}()
	client, err := aerospike.NewClientWithPolicyAndHost(policy, hs...)
	if err != nil {
		return nil, err
	}
	if o.writeTimeout > 0 {
		writePolicy := aerospike.NewWritePolicy(0, 0)
		writePolicy.SocketTimeout = o.writeTimeout
		writePolicy.MaxRetries = o.maxRetries
		client.DefaultWritePolicy = writePolicy
	}
	if o.readTimeout > 0 {
		readPolicy := aerospike.NewPolicy()
		readPolicy.SocketTimeout = o.readTimeout
		readPolicy.MaxRetries = o.maxRetries
		client.DefaultPolicy = readPolicy
	}
	as.C = client
	return as, nil
}

func (as *AsStore) MetricsTime(setName, action string, start time.Time) {
	if as.monitorVec == nil {
		return
	}
	as.monitorVec.VecObServe(&monitor.ASTimeLabel{Setname: setName, Action: action}, float64(time.Since(start))/float64(time.Millisecond))
}

func (as *AsStore) MetricsError(setName string, err error) {
	if as.monitorVec == nil || err == nil {
		return
	}
	if e, ok := err.(types.AerospikeError); ok {
		as.monitorVec.IncError(&monitor.AsErrLabel{
			Setname: setName,
			Code:    strconv.Itoa(int(e.ResultCode())),
		})
	} else {
		as.monitorVec.IncError(&monitor.AsErrLabel{
			Setname: setName,
			Code:    "error",
		})
	}
}

func (as *AsStore) Namespace() string {
	return as.namespace
}

func (as *AsStore) Sets(setName, keyName string, value aerospike.BinMap) (returnErr error) {
	defer as.MetricsTime(setName, "sets", time.Now())
	defer func() { as.MetricsError(setName, returnErr) }()
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return err
	}
	return as.C.Put(nil, key, value)
}

func (as *AsStore) SetHasPolicy(setName, keyName string, value aerospike.BinMap, policy *aerospike.WritePolicy) (returnErr error) {
	defer as.MetricsTime(setName, "sets", time.Now())
	defer func() { as.MetricsError(setName, returnErr) }()
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return err
	}
	return as.C.Put(policy, key, value)
}

func (as *AsStore) Get(setName, keyName string) (b aerospike.BinMap, returnErr error) {
	defer as.MetricsTime(setName, "get", time.Now())
	defer func() { as.MetricsError(setName, returnErr) }()
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return nil, err
	}
	rec, err := as.C.Get(nil, key)
	if err != nil {
		if err == types.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}
	return rec.Bins, nil
}

func (as *AsStore) Delete(setName, keyName string) (b bool, returnErr error) {
	defer as.MetricsTime(setName, "delete", time.Now())
	defer func() { as.MetricsError(setName, returnErr) }()
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return false, err
	}
	return as.C.Delete(nil, key)
}

func (as *AsStore) Exists(setName, keyName string) (b bool, returnErr error) {
	defer as.MetricsTime(setName, "exists", time.Now())
	defer func() { as.MetricsError(setName, returnErr) }()
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return false, err
	}
	return as.C.Exists(nil, key)
}

func (as *AsStore) PutMap(setName, keyName, binName, k, v string, opPolicy *aerospike.WritePolicy) (b int, returnErr error) {
	defer func() { as.MetricsError(setName, returnErr) }()
	policy := aerospike.NewMapPolicy(aerospike.MapOrder.UNORDERED, aerospike.MapWriteMode.UPDATE)
	op := aerospike.MapPutOp(policy, binName, k, v)
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return 0, err
	}
	res, err := as.C.Operate(opPolicy, key, op)
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	bin := res.Bins[binName]
	if bin == nil {
		return 0, err
	}
	if c, ok := bin.(int); ok {
		return c, nil
	}
	return 0, err
}

// https://github.com/aerospike/aerospike-client-go/blob/master/cdt_map_test.go#L115
func (as *AsStore) GetMapValue(setName, keyName, binName, k string) (m map[interface{}]interface{}, returnErr error) {
	defer as.MetricsTime(setName, "get_map_val", time.Now())
	defer func() { as.MetricsError(setName, returnErr) }()
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return nil, err
	}
	binMap, err := as.C.Get(nil, key, binName)
	if err != nil {
		if err == types.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	if binMap == nil {
		return nil, nil
	}

	if binMap.Bins[binName] == nil {
		return nil, nil
	}
	return binMap.Bins[binName].(map[interface{}]interface{}), nil
}

func (as *AsStore) AppendUniq(setName, keyName, binName, value string, opPolicy *aerospike.WritePolicy) (r *aerospike.Record, returnErr error) {
	defer as.MetricsTime(setName, "append_uniq", time.Now())
	defer func() { as.MetricsError(setName, returnErr) }()
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return nil, err
	}
	police := aerospike.NewListPolicy(aerospike.ListOrderUnordered, aerospike.ListWriteFlagsAddUnique)
	op := aerospike.ListAppendWithPolicyOp(police, binName, value)
	return as.C.Operate(opPolicy, key, op)
}

func (as *AsStore) RemoveList(setName, keyName, binName string, value []string, opPolicy *aerospike.WritePolicy) (r *aerospike.Record, returnErr error) {
	defer as.MetricsTime(setName, "remove_uniq", time.Now())
	defer func() { as.MetricsError(setName, returnErr) }()
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return nil, err
	}
	valuei := make([]interface{}, len(value))
	for i, v := range value {
		valuei[i] = v
	}
	op := aerospike.ListRemoveByValueListOp(binName, valuei, aerospike.ListReturnTypeNone)
	return as.C.Operate(opPolicy, key, op)
}

// https://github.com/aerospike/aerospike-client-go/blob/master/cdt_list_test.go
func (as *AsStore) GetList(setName, keyName, binName string) (i interface{}, returnErr error) {
	defer as.MetricsTime(setName, "get_all_list", time.Now())
	defer func() { as.MetricsError(setName, returnErr) }()
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return nil, err
	}
	op := aerospike.GetOpForBin(binName)
	result, err := as.C.Operate(nil, key, op)
	if err != nil {
		if err == types.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	if result.Bins == nil {
		return nil, nil
	}

	return result.Bins[binName], nil
}

func (as *AsStore) GetListLen(setName, keyName, binName string) (i int, returnErr error) {
	defer as.MetricsTime(setName, "get_list_len", time.Now())
	defer func() { as.MetricsError(setName, returnErr) }()
	key, err := aerospike.NewKey(as.namespace, setName, keyName)
	if err != nil {
		return 0, err
	}
	op := aerospike.ListSizeOp(binName)
	result, err := as.C.Operate(nil, key, op)
	if err != nil {
		if err == types.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}
	if result == nil {
		return 0, nil
	}
	if result.Bins == nil {
		return 0, nil
	}
	c, ok := result.Bins[binName].(int)
	if !ok {
		return 0, errors.New("result.Bins[binName].(int) error")
	}
	return c, nil
}
