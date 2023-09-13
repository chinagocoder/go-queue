package nsq

import "time"

type Conf struct {
	Brokers           []string
	ConnectionTimeout time.Duration `json:",optional"`
	ReadTimeout       time.Duration `json:",optional"`
	WriteTimeout      time.Duration `json:",optional"`
	AuthName          string        `json:",optional"`
	AuthParams        string        `json:",optional"`
	Topic             string
	Channel           string `json:",optional"`
	Conns             int    `json:",default=1"`
	Processors        int    `json:",default=1"`
}
