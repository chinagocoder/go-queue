// @Title  producer.go
// @Description  生产者
// @Author  xushuai  2022/11/26 11:04 PM

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chinagocoder/go-queue/nsq"
	"math/rand"
	"strconv"
	"time"
)

type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}

func main() {
	pusher, _ := nsq.NewPusher(nsq.Conf{
		Brokers: []string{
			"gocoder.com.cn:32772",
		},
		Topic: "topic1",
	},
	)

	for round := 0; round < 1; round++ {
		count := rand.Intn(100)
		m := message{
			Key:     strconv.FormatInt(time.Now().UnixNano(), 10),
			Value:   fmt.Sprintf("%d,%d", round, count),
			Payload: fmt.Sprintf("%s,%d,%d", "topic1", round, count),
		}
		body, _ := json.Marshal(m)

		fmt.Println(string(body))

		if _, err := pusher.Push(
			context.Background(),
			[]byte(strconv.FormatInt(time.Now().UnixNano(), 10)),
			body,
			nsq.WithDeliverAt(time.Now().Add(time.Second*10)),
		); err != nil {
			fmt.Println(err)
		}
	}
}
