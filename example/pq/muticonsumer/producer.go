// @Title  producer.go
// @Description  todo
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
	pusher := pq.NewPusher(
		[]string{
			"127.0.0.1:6650",
		}, "topic1",
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

	pusher2 := pq.NewPusher(
		[]string{
			"114.115.255.244:6650",
		}, "topic2",
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
