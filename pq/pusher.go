// @Title  pusher.go
// @Description  生产者
// @Author  xushuai  2022/11/26 8:43 PM

package pq

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
	"strings"
	"time"
)

var ErrNotSupport = errors.New("not support")

type CallOptions func(interface{})

type (
	PushOption func(options *chunkOptions)

	Pusher struct {
		producer pulsar.Producer
		client   pulsar.Client
		topic    string
		executor *executors.ChunkExecutor
	}

	callOptions struct {
		isSync  bool
		message *Message
	}

	Message struct {
		// Payload for the message
		Payload []byte

		// Value and payload is mutually exclusive, `Value interface{}` for schema message.
		Value interface{}

		// Key sets the key of the message for routing policy
		Key string

		// OrderingKey sets the ordering key of the message
		OrderingKey string

		// Properties attach application defined properties on the message
		Properties map[string]string

		// EventTime set the event time for a given message
		// By default, messages don't have an event time associated, while the publish
		// time will be be always present.
		// Set the event time to a non-zero timestamp to explicitly declare the time
		// that the event "happened", as opposed to when the message is being published.
		EventTime time.Time

		// ReplicationClusters override the replication clusters for this message.
		ReplicationClusters []string

		// DisableReplication disables the replication for this message
		DisableReplication bool

		// SequenceID sets the sequence id to assign to the current message
		SequenceID *int64

		// DeliverAfter requests to deliver the message only after the specified relative delay.
		// Note: messages are only delivered with delay when a consumer is consuming
		//     through a `SubscriptionType=Shared` subscription. With other subscription
		//     types, the messages will still be delivered immediately.
		DeliverAfter time.Duration

		// DeliverAt delivers the message only at or after the specified absolute timestamp.
		// Note: messages are only delivered with delay when a consumer is consuming
		//     through a `SubscriptionType=Shared` subscription. With other subscription
		//     types, the messages will still be delivered immediately.
		DeliverAt time.Time
	}

	chunkOptions struct {
		chunkSize     int
		flushInterval time.Duration
	}
)

func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	url := fmt.Sprintf("pulsar://%s", strings.Join(addrs, ","))
	client, err := pulsar.NewClient(
		pulsar.ClientOptions{
			URL:               url,
			ConnectionTimeout: 5 * time.Second,
			OperationTimeout:  5 * time.Second,
		},
	)

	if err != nil {
		logx.Errorf("could not instantiate Pulsar client: %v", err)
	}

	producer, err := client.CreateProducer(
		pulsar.ProducerOptions{
			Topic: topic,
		},
	)

	if err != nil {
		logx.Error(err)
	}

	pusher := &Pusher{
		producer: producer,
		client:   client,
		topic:    topic,
	}

	pusher.executor = executors.NewChunkExecutor(
		func(tasks []interface{}) {
			for i := range tasks {
				if _, err := pusher.producer.Send(
					context.Background(), tasks[i].(*pulsar.ProducerMessage),
				); err != nil {
					logx.Error(err)
				}
			}

		}, newOptions(opts)...,
	)

	return pusher
}

func (p *Pusher) Close() error {
	p.producer.Close()
	p.client.Close()
	return nil
}

func (p *Pusher) Name() string {
	return p.topic
}

func (p *Pusher) Push(ctx context.Context, k, v []byte, opts ...CallOptions) (
	interface{}, error) {

	op := new(callOptions)
	op.message = new(Message)
	for _, opt := range opts {
		opt(op)
	}

	msg := &pulsar.ProducerMessage{
		Payload:             v,
		Value:               op.message.Value,
		Key:                 string(k),
		OrderingKey:         op.message.OrderingKey,
		Properties:          op.message.Properties,
		EventTime:           op.message.EventTime,
		ReplicationClusters: op.message.ReplicationClusters,
		DisableReplication:  op.message.DisableReplication,
		SequenceID:          op.message.SequenceID,
		DeliverAfter:        op.message.DeliverAfter,
		DeliverAt:           op.message.DeliverAt,
	}

	if p.executor == nil {
		op.isSync = true
	}

	if op.isSync {
		id, err := p.producer.Send(ctx, msg)
		return id, err
	} else {
		return nil, p.executor.Add(msg, len(v))

	}
}

func WithChunkSize(chunkSize int) PushOption {
	return func(options *chunkOptions) {
		options.chunkSize = chunkSize
	}
}

func WithFlushInterval(interval time.Duration) PushOption {
	return func(options *chunkOptions) {
		options.flushInterval = interval
	}
}

func newOptions(opts []PushOption) []executors.ChunkOption {
	var options chunkOptions
	for _, opt := range opts {
		opt(&options)
	}

	var chunkOpts []executors.ChunkOption
	if options.chunkSize > 0 {
		chunkOpts = append(chunkOpts, executors.WithChunkBytes(options.chunkSize))
	}

	if options.flushInterval > 0 {
		chunkOpts = append(chunkOpts, executors.WithFlushInterval(options.flushInterval))
	}

	return chunkOpts
}

func WithMessage(message Message) CallOptions {
	return func(i interface{}) {
		m, ok := i.(*callOptions)
		if !ok {
			panic(ErrNotSupport)
		}
		*m.message = message
	}
}

func WithDeliverAt(deliverAt time.Time) CallOptions {
	return func(i interface{}) {
		m, ok := i.(*callOptions)
		if !ok {
			panic(ErrNotSupport)
		}
		m.message.DeliverAt = deliverAt
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

func WithOrderingKey(OrderingKey string) CallOptions {
	return func(i interface{}) {

		m, ok := i.(*callOptions)
		if !ok {
			panic(ErrNotSupport)
		}
		m.message.OrderingKey = OrderingKey
	}
}

func WithSync() CallOptions {
	return func(i interface{}) {
		options, ok := i.(*callOptions)
		if !ok {
			panic(ErrNotSupport)
		}

		options.isSync = true
	}
}
