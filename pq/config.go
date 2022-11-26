// @Title  config.go
// @Description  todo
// @Author  xushuai  2022/11/26 8:42 PM

package pq

type Conf struct {
	Name             string
	Brokers          []string
	Topic            string
	SubscriptionName string
	Conns            int `json:",default=1"`
	Processors       int `json:",default=8"`
}
