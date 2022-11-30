package main

import (
	"fmt"
	"github.com/chinagocoder/go-queue/pq"
	"github.com/zeromicro/go-zero/core/conf"
)

type Config struct {
	PulsarTopic1 pq.Conf
	PulsarTopic2 pq.Conf
}

func main() {
	var c Config
	conf.MustLoad("config.yaml", &c)

	fmt.Println(c)

	q := pq.MustNewQueue(c.PulsarTopic1, pq.WithHandle(func(k, v string) error {
		fmt.Printf("=> %s\n", v)
		return nil
	}))
	q.AddQueue(c.PulsarTopic2, pq.WithHandle(func(k, v string) error {
		fmt.Printf("=> %s\n", v)
		return nil
	}))
	defer q.Stop()
	q.Start()
}
