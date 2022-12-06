# go-queue

> Pulsar Pub/Sub framework. Reference: https://github.com/zeromicro/go-queue

## installation

```shell
go get -u github.com/chinagocoder/go-queue
```

## pulsar

Pulsar Pub/Sub framework

### consumer example

config.yaml

```yaml
Name: pulsar
Brokers:
  - 127.0.0.1:6650
Topic: topic_message
Conns: 1
Processors: 1
SubscriptionName: topic_message
```

consumer code

```go
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

	q := pq.MustNewQueue()
	q.AddQueue(c, pq.WithHandle(func(k, v string) error {
		fmt.Printf("=> %s\n", v)
		return nil
	}))
	defer q.Stop()
	q.Start()
}
```

producer code

```go
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
		}, "topic_message",
	)

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

		if _, err := pusher.Push(
			context.Background(), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), body, pq.WithDeliverAt(t),
		); err != nil {
			fmt.Println(err)
		}
	}

	cmdline.EnterToContinue()
}
```