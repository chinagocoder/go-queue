// @Title  pusher.go
// @Description  生产者
// @Author  xushuai  2022/11/26 8:43 PM

package pq

import (
	"context"
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zeromicro/go-zero/core/logx"
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
	MessageResult struct {
		EntryID      int64
		Serialize    []byte
		BatchIdx     int32
		LedgerID     int64
		PartitionIdx int32
	}

	chunkOptions struct {
		chunkSize     int
		flushInterval time.Duration
	}
)

func NewPusher(c Conf) (*Pusher, error) {
	// 初始化pulsar连接
	client, err := NewClient(c)

	if err != nil {
		logx.Errorf("could not instantiate Pulsar client: %v", err)
		return nil, err
	}

	producer, err := client.CreateProducer(
		pulsar.ProducerOptions{
			Topic: c.Topic,
		},
	)

	if err != nil {
		logx.Error(err)
		return nil, err
	}

	pusher := &Pusher{
		producer: producer,
		client:   client,
		topic:    c.Topic,
	}

	return pusher, nil
}

func NewPusherWithParam(adds []string, topic string, authName string, authParams string, connectionTimeout int, operationTimeout int) (*Pusher, error) {
	return NewPusher(Conf{
		Brokers:           adds,
		Topic:             topic,
		AuthName:          authName,
		AuthParams:        authParams,
		ConnectionTimeout: connectionTimeout,
		OperationTimeout:  operationTimeout,
	})
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
	*MessageResult, error) {

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

	messageId, err := p.producer.Send(ctx, msg)
	if err != nil {
		return nil, err
	}
	return &MessageResult{
		EntryID:      messageId.EntryID(),
		Serialize:    messageId.Serialize(),
		BatchIdx:     messageId.BatchIdx(),
		LedgerID:     messageId.LedgerID(),
		PartitionIdx: messageId.PartitionIdx(),
	}, nil
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
