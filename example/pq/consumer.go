package main

import (
	"fmt"
	"github.com/chinagocoder/go-queue/pq"
	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c pq.Conf
	conf.MustLoad("config.yaml", &c)

	fmt.Println(c)

	q := pq.MustNewQueue(c, pq.WithHandle(func(k, v string) error {
		fmt.Printf("=> %s\n", v)
		return nil
	}))
	defer q.Stop()
	q.Start()
}
