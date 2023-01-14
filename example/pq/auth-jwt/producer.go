// @Title  producer.go
// @Description  todo
// @Author  xushuai  2022/11/26 11:04 PM

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chinagocoder/go-queue/pq"
	"github.com/zeromicro/go-zero/core/conf"
	"time"
)

type CustomMessage struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	var c pq.Conf
	conf.MustLoad("config.yaml", &c)
	fmt.Printf("配置内容:%+v\n", c)

	//初始化生产者客户端
	pusher, err := pq.NewPusher(c)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 消息体
	m := CustomMessage{
		Id:   1,
		Name: "张三",
		Age:  24,
	}

	body, _ := json.Marshal(m)

	fmt.Printf("消息体:%s\n", string(body))

	// 生成消息key
	k := []byte(fmt.Sprintf("%d", time.Now().UnixNano()))

	messageId, err := pusher.Push(
		context.Background(), k, body,
	)
	if err != nil {
		fmt.Printf("消息发送失败,err:%v\n", err)
	} else {
		fmt.Printf("消息发送成功,resp:%+v\n", messageId)
	}
}
