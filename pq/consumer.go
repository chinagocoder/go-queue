// @Title  consumer.go
// @Description  pulsar
// @Author  xushuai  2022/11/26 8:43 PM

package pq

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"strings"
	"time"

	_ "github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/lz4"
	_ "github.com/segmentio/kafka-go/snappy"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/queue"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/threading"
	"github.com/zeromicro/go-zero/core/timex"
)

const (
	defaultCommitInterval = time.Second
	defaultMaxWait        = time.Second
	defaultQueueCapacity  = 1000
)

type (
	ConsumeHandle func(key, value string) error

	ConsumeHandler interface {
		Consume(key, value string) error
	}

	queueOptions struct {
		commitInterval time.Duration
		queueCapacity  int
		maxWait        time.Duration
		metrics        *stat.Metrics
	}

	QueueOption func(*queueOptions)

	pulsarQueue struct {
		c                Conf
		consumer         pulsar.Consumer
		handler          ConsumeHandler
		channel          chan pulsar.ConsumerMessage
		consumerRoutines *threading.RoutineGroup
		metrics          *stat.Metrics
	}

	pulsarQueues struct {
		queues []queue.MessageQueue
		group  *service.ServiceGroup
	}
)

func MustNewQueue(c Conf, handler ConsumeHandler, opts ...QueueOption) *pulsarQueues {
	q, err := NewQueue(c, handler, opts...)
	if err != nil {
		log.Fatal(err)
	}

	return q
}

func NewQueue(c Conf, handler ConsumeHandler, opts ...QueueOption) (*pulsarQueues, error) {
	var options queueOptions
	for _, opt := range opts {
		opt(&options)
	}

	ensureQueueOptions(c, &options)

	if c.Conns < 1 {
		c.Conns = 1
	}

	q := &pulsarQueues{
		group: service.NewServiceGroup(),
	}
	for i := 0; i < c.Conns; i++ {
		q.queues = append(q.queues, newPulsarQueue(c, handler, options))
	}

	return q, nil
}

func (q *pulsarQueues) AddQueue(c Conf, handler ConsumeHandler, opts ...QueueOption) error {
	var options queueOptions
	for _, opt := range opts {
		opt(&options)
	}

	ensureQueueOptions(c, &options)

	if c.Conns < 1 {
		c.Conns = 1
	}

	for i := 0; i < c.Conns; i++ {
		q.queues = append(q.queues, newPulsarQueue(c, handler, options))
	}

	return nil
}

func newPulsarQueue(c Conf, handler ConsumeHandler, options queueOptions) *pulsarQueue {

	url := fmt.Sprintf("pulsar://%s", strings.Join(c.Brokers, ","))
	client, err := pulsar.NewClient(
		pulsar.ClientOptions{
			URL:               url,
			ConnectionTimeout: 5 * time.Second,
			OperationTimeout:  5 * time.Second,
		},
	)

	messageChannel := make(chan pulsar.ConsumerMessage)
	consumer, err := client.Subscribe(
		pulsar.ConsumerOptions{
			Topic:            c.Topic,
			Type:             pulsar.Shared,
			SubscriptionName: c.SubscriptionName,
			MessageChannel:   messageChannel,
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	return &pulsarQueue{
		c:                c,
		consumer:         consumer,
		handler:          handler,
		channel:          messageChannel,
		consumerRoutines: threading.NewRoutineGroup(),
		metrics:          options.metrics,
	}
}

func (q *pulsarQueue) Start() {
	q.startConsumers()
	q.consumerRoutines.Wait()
}

func (q *pulsarQueue) Stop() {
	q.consumer.Close()
	logx.Close()
}

func (q *pulsarQueues) Start() {
	for _, each := range q.queues {
		q.group.Add(each)
	}
	q.group.Start()
}

func (q *pulsarQueues) Stop() {
	q.group.Stop()
}

func (q *pulsarQueue) consumeOne(key, val string) error {
	startTime := timex.Now()
	err := q.handler.Consume(key, val)
	q.metrics.Add(stat.Task{
		Duration: timex.Since(startTime),
	})
	return err
}
func (q *pulsarQueue) startConsumers() {
	for i := 0; i < q.c.Processors; i++ {
		q.consumerRoutines.Run(func() {
			for msg := range q.channel {
				if err := q.consumeOne(msg.Key(), string(msg.Payload())); err != nil {
					logx.Errorf("Error on consuming: %s, error: %v", string(msg.Payload()), err)
				}
				q.consumer.Ack(msg)
			}
		})
	}
}

func WithCommitInterval(interval time.Duration) QueueOption {
	return func(options *queueOptions) {
		options.commitInterval = interval
	}
}

func WithQueueCapacity(queueCapacity int) QueueOption {
	return func(options *queueOptions) {
		options.queueCapacity = queueCapacity
	}
}

func WithHandle(handle ConsumeHandle) ConsumeHandler {
	return innerConsumeHandler{
		handle: handle,
	}
}

func WithMaxWait(wait time.Duration) QueueOption {
	return func(options *queueOptions) {
		options.maxWait = wait
	}
}

func WithMetrics(metrics *stat.Metrics) QueueOption {
	return func(options *queueOptions) {
		options.metrics = metrics
	}
}

type innerConsumeHandler struct {
	handle ConsumeHandle
}

func (ch innerConsumeHandler) Consume(k, v string) error {
	return ch.handle(k, v)
}

func ensureQueueOptions(c Conf, options *queueOptions) {
	if options.commitInterval == 0 {
		options.commitInterval = defaultCommitInterval
	}
	if options.queueCapacity == 0 {
		options.queueCapacity = defaultQueueCapacity
	}
	if options.maxWait == 0 {
		options.maxWait = defaultMaxWait
	}
	if options.metrics == nil {
		options.metrics = stat.NewMetrics(c.Name)
	}
}
