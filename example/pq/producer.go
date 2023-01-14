// @Title  producer.go
// @Description  todo
// @Author  xushuai  2022/11/26 11:04 PM

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chinagocoder/go-queue/pq"
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
	pusher, err := pq.NewPusher(pq.Conf{
		Brokers: []string{
			"127.0.0.1:6650",
		},
		Topic:      "topic1",
		AuthName:   "token",
		AuthParams: "{\"token\":\"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.CNYN8h04Z_wJcvNssVhcyZKDlqvwOSxmkXeOy6WH8pM\"}",
	},
	)
	fmt.Println(err)

	ticker := time.NewTicker(time.Millisecond)
	for round := 0; round < 3; round++ {
		<-ticker.C

		count := rand.Intn(100)
		m := message{
			Key:     strconv.FormatInt(time.Now().UnixNano(), 10),
			Value:   fmt.Sprintf("%d,%d", round, count),
			Payload: fmt.Sprintf("%d,%d", round, count),
		}
		body, _ := json.Marshal(m)

		fmt.Println(string(body))

		t := time.Now().Add(10 * time.Second)

		messageId, err := pusher.Push(
			context.Background(), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), body, pq.WithDeliverAt(t),
		)

		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("%#v\n", messageId)
			fmt.Println(t.Format("2006-01-02 15:04:05"))
		}

	}
}
