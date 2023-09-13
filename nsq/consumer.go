// @Title  consumer.go
// @Description  消费者
// @Author  xushuai  2022/11/26 8:43 PM

package nsq

import (
	"encoding/json"
	"fmt"
	"github.com/nsqio/go-nsq"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/queue"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/threading"
	"log"
	"time"
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
	}

	QueueOption func(*queueOptions)

	Queue struct {
		c                Conf
		consumer         *nsq.Consumer
		handler          ConsumeHandler
		channel          chan nsq.Message
		consumerRoutines *threading.RoutineGroup
	}

	Queues struct {
		queues []queue.MessageQueue
		group  *service.ServiceGroup
	}
)

type innerConsumeHandler struct {
	handle ConsumeHandle
}

func (ch innerConsumeHandler) Consume(k, v string) error {
	return ch.handle(k, v)
}

type nHandler struct {
	fnc ConsumeHandle
}

func (h *nHandler) HandleMessage(message *nsq.Message) error {
	msg := &Message{}
	err := json.Unmarshal(message.Body, msg)
	if err == nil {
		err = h.fnc(msg.Key, string(msg.Payload))
	} else {
		err = h.fnc("", string(message.Body))
	}
	return err
}

func MustNewQueue() *Queues {
	q := &Queues{
		group: service.NewServiceGroup(),
	}
	return q
}

func (q *Queues) AddQueue(c Conf, handler ConsumeHandler, opts ...QueueOption) error {
	var options queueOptions
	for _, opt := range opts {
		opt(&options)
	}

	ensureQueueOptions(c, &options)

	if c.Conns < 1 {
		c.Conns = 1
	}

	for i := 0; i < c.Conns; i++ {
		q.queues = append(q.queues, newNsqQueue(c, handler, options))
	}

	return nil
}

func newNsqQueue(c Conf, handler ConsumeHandler, options queueOptions) *Queue {

	// Instantiate a producer.
	config := nsq.NewConfig()

	if c.ConnectionTimeout > 0 {
		config.DialTimeout = c.ConnectionTimeout
	}

	if c.ReadTimeout > 0 {
		config.ReadTimeout = c.ReadTimeout
	}

	if c.WriteTimeout > 0 {
		config.WriteTimeout = c.WriteTimeout
	}

	channel := c.Channel
	if channel == "" {
		channel = "channel"
	}

	consumer, err := nsq.NewConsumer(c.Topic, channel, config)
	if err != nil {
		fmt.Println("NewConsumer", err)
		log.Fatal(err)
	}

	// Set the Handler for messages received by this Consumer. Can be called multiple times.
	// See also AddConcurrentHandlers.
	consumer.AddHandler(&nHandler{handler.Consume})
	messageChannel := make(chan nsq.Message)

	return &Queue{
		c:                c,
		consumer:         consumer,
		handler:          handler,
		channel:          messageChannel,
		consumerRoutines: threading.NewRoutineGroup(),
	}
}

func (q *Queue) Start() {
	q.startConsumers()
	q.consumerRoutines.Wait()
}

func (q *Queue) Stop() {
	q.consumer.Stop()
	logx.Close()
}

func (q *Queues) Start() {
	for _, each := range q.queues {
		q.group.Add(each)
	}
	q.group.Start()
	<-make(chan bool)
}

func (q *Queues) Stop() {
	q.group.Stop()
}

//	func (q *Queue) consumeOne(key, val string) error {
//		err := q.handler.HandleMessage()
//		return err
//	}
func (q *Queue) startConsumers() {
	for i := 0; i < q.c.Processors; i++ {
		q.consumerRoutines.Run(func() {
			if err := q.consumer.ConnectToNSQDs(q.c.Brokers); err != nil {
				logx.Errorf("Error on  error: %v", err)
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
}
