package main

import (
	"fmt"
	"github.com/chinagocoder/go-queue/nsq"
	"github.com/chinagocoder/go-queue/pq"
	"github.com/zeromicro/go-zero/core/conf"
)

type Config struct {
	Topic1 nsq.Conf
	Topic2 nsq.Conf
}

func main() {
	var c Config
	conf.MustLoad("config.yaml", &c)

	fmt.Println(c)

	q := nsq.MustNewQueue()

	q.AddQueue(c.Topic1, pq.WithHandle(func(k, v string) error {
		fmt.Printf("111111=> %s\n", v)
		return nil
	}))
	q.AddQueue(c.Topic2, pq.WithHandle(func(k, v string) error {
		fmt.Printf("222222=> %s\n", v)
		return nil
	}))
	defer q.Stop()
	q.Start()
}
