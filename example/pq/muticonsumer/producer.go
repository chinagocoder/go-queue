// @Title  producer.go
// @Description  生产者
// @Author  xushuai  2022/11/26 11:04 PM

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chinagocoder/go-queue/pq"
	"github.com/zeromicro/go-zero/core/cmdline"
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
	pusher, _ := pq.NewPusher(pq.Conf{
		Brokers: []string{
			"127.0.0.1:6650",
		},
		Topic:      "topic1",
		AuthName:   "token",
		AuthParams: "{\"token\":\"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.CNYN8h04Z_wJcvNssVhcyZKDlqvwOSxmkXeOy6WH8pM\"}",
	},
	)

	for round := 0; round < 3; round++ {
		count := rand.Intn(100)
		m := message{
			Key:     strconv.FormatInt(time.Now().UnixNano(), 10),
			Value:   fmt.Sprintf("%d,%d", round, count),
			Payload: fmt.Sprintf("%s,%d,%d", "topic1", round, count),
		}
		body, _ := json.Marshal(m)

		if _, err := pusher.Push(
			context.Background(), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), body,
		); err != nil {
			fmt.Println(err)
		}
	}

	pusher2, _ := pq.NewPusher(
		pq.Conf{
			Brokers: []string{
				"127.0.0.1:6650",
			},
			Topic:      "topic2",
			AuthName:   "token",
			AuthParams: "{\"token\":\"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.CNYN8h04Z_wJcvNssVhcyZKDlqvwOSxmkXeOy6WH8pM\"}",
		},
	)

	for round := 0; round < 3; round++ {

		count := rand.Intn(100)
		m := message{
			Key:     strconv.FormatInt(time.Now().UnixNano(), 10),
			Value:   fmt.Sprintf("%d,%d", round, count),
			Payload: fmt.Sprintf("%s,%d,%d", "topic2", round, count),
		}
		body, _ := json.Marshal(m)

		if _, err := pusher2.Push(
			context.Background(), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), body,
		); err != nil {
			fmt.Println(err)
		}
	}

	cmdline.EnterToContinue()
}
