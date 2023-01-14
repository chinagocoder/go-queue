// @Title  config.go
// @Description  配置
// @Author  xushuai  2022/11/26 8:42 PM

package pq

type Conf struct {
	Brokers           []string
	ConnectionTimeout int    `json:",optional"`
	OperationTimeout  int    `json:",optional"`
	AuthName          string `json:",optional"`
	AuthParams        string `json:",optional"`
	Topic             string
	SubscriptionName  string `json:",optional"`
	Conns             int    `json:",default=1"`
	Processors        int    `json:",default=1"`
}
