package nsq

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/nsqio/go-nsq"
	"github.com/zeromicro/go-zero/core/logx"
	"math/rand"
	"time"
)

var ErrNotSupport = errors.New("not support")

type CallOptions func(interface{})

type (
	Pusher struct {
		producer *nsq.Producer
		topic    string
	}

	callOptions struct {
		isSync  bool
		message *Message
	}

	RawMessage nsq.Message

	Message struct {
		// Payload for the message
		Payload []byte

		// Value and payload is mutually exclusive, `Value interface{}` for schema message.
		Value interface{}

		// Key sets the key of the message for routing policy
		Key string

		// EventTime set the event time for a given message
		// By default, messages don't have an event time associated, while the publish
		// time will be be always present.
		// Set the event time to a non-zero timestamp to explicitly declare the time
		// that the event "happened", as opposed to when the message is being published.
		EventTime time.Time

		// DeliverAfter requests to deliver the message only after the specified relative delay.
		// Note: messages are only delivered with delay when a consumer is consuming
		//     through a `SubscriptionType=Shared` subscription. With other subscription
		//     types, the messages will still be delivered immediately.
		DeliverAfter time.Duration
	}
	MessageResult struct {
	}
)

type Producer *nsq.Producer

func NewPusher(c Conf) (*Pusher, error) {
	resp := &Pusher{}
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

	count := len(c.Brokers)
	rand.New(rand.NewSource(time.Now().UnixNano()))

	index := rand.Intn(count) // 生成0-count之间的随机整数

	addr := c.Brokers[index]

	producer, err := nsq.NewProducer(addr, config)
	if err != nil {
		logx.Errorf("new nsq producer fail: %v", err)
		return resp, err
	}

	return &Pusher{
		producer: producer,
		topic:    c.Topic,
	}, nil
}

func (p *Pusher) Close() error {
	p.producer.Stop()
	return nil
}

func (p *Pusher) Name() string {
	return p.topic
}

func (p *Pusher) Push(ctx context.Context, k, v []byte, opts ...CallOptions) (
	*MessageResult, error) {
	op := new(callOptions)
	op.message = new(Message)
	for _, opt := range opts {
		opt(op)
	}

	msg := &Message{
		Payload:      v,
		Value:        op.message.Value,
		Key:          string(k),
		EventTime:    op.message.EventTime,
		DeliverAfter: op.message.DeliverAfter,
	}

	if msg.EventTime.IsZero() {
		msg.EventTime = time.Now()
	}

	messageBody, _ := json.Marshal(msg)

	var err error
	if msg.DeliverAfter > 0 {
		err = p.producer.DeferredPublish(p.topic, msg.DeliverAfter, messageBody)
	} else {
		err = p.producer.Publish(p.topic, messageBody)
	}

	if err != nil {
		return nil, err
	}
	return &MessageResult{}, nil
}

func WithDeliverAt(deliverAt time.Time) CallOptions {
	return func(i interface{}) {
		m, ok := i.(*callOptions)
		if !ok {
			panic(ErrNotSupport)
		}

		m.message.DeliverAfter = deliverAt.Sub(time.Now())
	}
}

func WithDeliverAfter(deliverAfter time.Duration) CallOptions {
	return func(i interface{}) {
		m, ok := i.(*callOptions)
		if !ok {
			panic(ErrNotSupport)
		}
		m.message.DeliverAfter = deliverAfter
	}
}
