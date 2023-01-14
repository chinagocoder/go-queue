package main

import (
	"fmt"
	"github.com/chinagocoder/go-queue/pq"
	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c pq.Conf
	conf.MustLoad("config.yaml", &c)

	fmt.Printf("配置内容:%+v\n", c)

	q := pq.MustNewQueue()
	err := q.AddQueue(c, pq.WithHandle(func(k, v string) error {
		// 接收消息，并处理
		fmt.Printf("收到新消息,消息内容:%s\n", v)
		return nil
	}))
	if err != nil {
		fmt.Printf("消费者监听失败,err:%v\n", err)
	} else {
		fmt.Printf("消费者监听成功,监听中...\n")
	}
	defer q.Stop()
	q.Start()
}
