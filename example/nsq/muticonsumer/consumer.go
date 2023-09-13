package main

import (
	"fmt"
	"github.com/chinagocoder/go-queue/nsq"
	"github.com/zeromicro/go-zero/core/conf"
)

type Config struct {
	Topic1 nsq.Conf
	Topic2 nsq.Conf
}

// HandleMessage implements the Handler interface.
func myMessageHandler1(m *nsq.Message) error {
	fmt.Println("111111" + string(m.Payload))
	return nil
}

func myMessageHandler2(m *nsq.Message) error {
	fmt.Println("222222" + string(m.Payload))
	return nil
}

func main() {
	var c Config
	conf.MustLoad("config.yaml", &c)

	fmt.Println(c)

	q := nsq.MustNewQueue()

	q.AddQueue(c.Topic1, myMessageHandler1)
	q.AddQueue(c.Topic2, myMessageHandler2)
	//q.AddQueue(c.PulsarTopic2, pq.WithHandle(func(k, v string) error {
	//	fmt.Printf("=> %s\n", v)
	//	return nil
	//}))
	defer q.Stop()
	q.Start()
}
