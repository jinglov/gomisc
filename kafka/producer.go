package kafka

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/jinglov/gomisc/drivers"
	"github.com/jinglov/gomisc/logger"
	"github.com/jinglov/gomisc/monitor"
	"sync"
)

type Producer struct {
	numWorkers int
	works      []*producerWorker
	queue      chan *sarama.ProducerMessage
	localCache *drivers.LocalStore
	wg         *sync.WaitGroup
	monitor    *monitor.KafkaVec
	log        logger.Logi
}

type producerWorker struct {
	producer   sarama.AsyncProducer
	queue      chan *sarama.ProducerMessage
	monitor    *monitor.KafkaVec
	localCache *drivers.LocalStore
	version    sarama.KafkaVersion
	log        logger.Logi
}

func NewProducer(producerName, user, password string, brokers []string, numWorkers, queueSize int, monitor *monitor.KafkaVec, cachePath string, version string, log logger.Logi) (*Producer, error) {
	p := &Producer{
		numWorkers: numWorkers,
		works:      make([]*producerWorker, numWorkers),
		queue:      make(chan *sarama.ProducerMessage, queueSize),
		monitor:    monitor,
	}
	p.log = log
	if cachePath != "" {
		var err error
		p.localCache, err = drivers.NewLocalStore(cachePath, p.Retry, 10, p.log)
		if err != nil {
			return nil, err
		}
	}
	v := kafkaVersion(version)
	for i := 0; i < numWorkers; i++ {
		w, err := newProducerWorker(producerName, user, password, brokers, p.queue, monitor, p.localCache, v, log)
		if err != nil {
			return nil, err
		}
		p.works[i] = w
	}
	return p, nil
}

func (p *Producer) Start() {
	if p.localCache != nil {
		p.localCache.Start()
	}
	p.wg = &sync.WaitGroup{}
	for i := 0; i < len(p.works); i++ {
		p.wg.Add(1)
		p.works[i].start(p.wg)
	}
}

// 关闭kafka时需要close掉worker的queue才可以
func (p *Producer) Close() {
	if p.localCache != nil {
		p.localCache.Stop()
	}
	close(p.queue)
	p.wg.Wait()
	if p.localCache != nil {
		p.localCache.Close()
	}
}

func (p *Producer) Send(topic string, data []byte) {
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(data)}
	select {
	case p.queue <- msg:
		if p.monitor != nil {
			p.monitor.Inc(&monitor.KafkaLabels{Partition: -1, Topic: msg.Topic, Status: "sent"})
		}
	default:
		if p.monitor != nil {
			p.monitor.Inc(&monitor.KafkaLabels{Partition: -1, Topic: msg.Topic, Status: "queuefull"})
		}
		e := producerWriteToLocal(p.localCache, msg)
		if e != nil && p.log != nil {
			p.log.Errorf("failed to write to local: %s", e.Error())
		}
	}
}

func (p *Producer) SendUseKey(topic string, data []byte, key sarama.Encoder) {
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(data), Key: key}
	select {
	case p.queue <- msg:
		if p.monitor != nil {
			p.monitor.Inc(&monitor.KafkaLabels{Partition: -1, Topic: msg.Topic, Status: "sent"})
		}
	default:
		if p.monitor != nil {
			p.monitor.Inc(&monitor.KafkaLabels{Partition: -1, Topic: msg.Topic, Status: "queuefull"})
		}
		e := producerWriteToLocal(p.localCache, msg)
		if e != nil && p.log != nil {
			p.log.Errorf("failed to write to local: %s", e.Error())
		}
	}
}

func (p *Producer) Retry(topic string, data []byte) {
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(data)}
	select {
	case p.queue <- msg:
		if p.monitor != nil {
			p.monitor.Inc(&monitor.KafkaLabels{Partition: -1, Topic: msg.Topic, Status: "retry"})
		}
	default:
		if p.monitor != nil {
			p.monitor.Inc(&monitor.KafkaLabels{Partition: -1, Topic: msg.Topic, Status: "queuefull"})
		}
		e := producerWriteToLocal(p.localCache, msg)
		if e != nil && p.log != nil {
			p.log.Errorf("failed to write to local: %s", e.Error())
		}
	}
}

func newProducerWorker(producerName, user, password string, brokers []string, queue chan *sarama.ProducerMessage, monitor *monitor.KafkaVec, localCache *drivers.LocalStore, version sarama.KafkaVersion, log logger.Logi) (*producerWorker, error) {
	c := sarama.NewConfig()
	c.ClientID = producerName
	c.Version = version
	if user != "" {
		c.Net.SASL.Enable = true
		c.Net.SASL.User = user
		c.Net.SASL.Password = password
	}
	c.Producer.Compression = sarama.CompressionSnappy
	c.Producer.Return.Successes = true
	c.Producer.Return.Errors = true
	// c.ChannelBufferSize = conf.Load().Report.KafkaBufferSize
	p, err := sarama.NewAsyncProducer(brokers, c)
	if err != nil {
		return nil, err
	}

	w := &producerWorker{
		producer:   p,
		queue:      queue,
		monitor:    monitor,
		localCache: localCache,
	}
	w.log = log
	return w, nil
}

func (pw *producerWorker) start(wg *sync.WaitGroup) {
	defer wg.Done()
	// 接收消息
	wg.Add(1)
	go pw.doMessage(wg)
	// 接收成功通知
	wg.Add(1)
	go pw.doSuccess(wg)
	// 接收错误通知
	wg.Add(1)
	go pw.doError(wg)
}

func (pw *producerWorker) doMessage(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case e, ok := <-pw.queue:
			if !ok {
				_ = pw.close()
				return
			}
			pw.producer.Input() <- e
		}
	}
}

func (pw *producerWorker) doSuccess(wg *sync.WaitGroup) {
	defer wg.Done()
	for m := range pw.producer.Successes() {
		if pw.log != nil {
			pw.log.Debugf("success")
			pw.log.Debugf("%v", m.Value)
		}
		if pw.monitor != nil {
			pw.monitor.Inc(&monitor.KafkaLabels{Partition: m.Partition, Topic: m.Topic, Status: "ok"})
		}
	}
}

func (pw *producerWorker) doError(wg *sync.WaitGroup) {
	defer wg.Done()
	for err := range pw.producer.Errors() {
		if err == nil {
			continue
		}
		if pw.monitor != nil {
			pw.monitor.Inc(&monitor.KafkaLabels{Partition: err.Msg.Partition, Topic: err.Msg.Topic, Status: "error"})
		}
		p, e := err.Msg.Value.Encode()
		if e != nil {
			if pw.log != nil {
				pw.log.Errorf("failed to get message payload from error: %s", e.Error())
			}
			continue
		}

		if err.Err == sarama.ErrMessageSizeTooLarge {
			if pw.log != nil {
				pw.log.Errorf("discard message because the size is too large: %d", len(p))
			}
			continue
		}
		if pw.log != nil {
			pw.log.Warnf("%s %s", err.Error(), err.Err)
			pw.log.Warnf("t:%s,p:%d", err.Msg.Topic, err.Msg.Partition)
		}
		if pw.monitor != nil {
			pw.monitor.Inc(&monitor.KafkaLabels{Partition: err.Msg.Partition, Topic: err.Msg.Topic, Status: "errorcache"})
		}
		e = producerWriteToLocal(pw.localCache, err.Msg)
		if e != nil && pw.log != nil {
			pw.log.Errorf("failed to write to local: %s", e.Error())
		}
	}
}

func (pw *producerWorker) close() error {
	return pw.producer.Close()
}

var ErrLocalStoreNil = errors.New("local store not init")

func producerWriteToLocal(d *drivers.LocalStore, msg *sarama.ProducerMessage) error {
	if d == nil {
		return ErrLocalStoreNil
	}
	v, err := msg.Value.Encode()
	if err != nil {
		return err
	}
	return d.Put(msg.Topic, v)
}

func NewProducerV2(producerName, version string, brokers []string, opts ...optFun) (*Producer, error) {

	options := &Options{
		Name:    producerName,
		brokers: brokers,
	}
	for _, o := range opts {
		o(options)
	}
	FillProducerOption(options)
	err := ValidProducerOption(options)
	if err != nil {
		return nil, err
	}

	p := &Producer{
		numWorkers: options.numWorkers,
		works:      make([]*producerWorker, options.numWorkers),
		queue:      make(chan *sarama.ProducerMessage, options.queueSize),
		monitor:    options.vec,
	}
	if options.cachePath != "" {
		p.localCache, err = drivers.NewLocalStore(options.cachePath, p.Retry, 10, options.log)
		if err != nil {
			return nil, err
		}
	}
	v := kafkaVersion(version)
	for i := 0; i < p.numWorkers; i++ {
		w, err := newProducerWorker(producerName, options.user, options.password, options.brokers, p.queue, p.monitor, p.localCache, v, options.log)
		if err != nil {
			return nil, err
		}
		p.works[i] = w
	}
	return p, nil
}
