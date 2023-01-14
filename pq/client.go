// @Title  client.go
// @Description  客户端
// @Author  xushuai  2023/1/14 3:59 PM

package pq

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zeromicro/go-zero/core/logx"
	"strings"
	"time"
)

type Client pulsar.Client

func NewClient(c Conf) (Client, error) {

	clientOptions := pulsar.ClientOptions{
		URL: fmt.Sprintf("pulsar://%s", strings.Join(c.Brokers, ",")),
	}

	// 认证
	if c.AuthName != "" && c.AuthParams != "" {
		authentication, err := pulsar.NewAuthentication(c.AuthName, c.AuthParams)
		if err != nil {
			logx.Errorf("could not generate Pulsar Authentication: %v", err)
			return nil, err
		}
		clientOptions.Authentication = authentication
	}

	// 连接超时时间
	if c.ConnectionTimeout > 0 {
		clientOptions.ConnectionTimeout = time.Second * time.Duration(c.ConnectionTimeout)
	}

	// 操作超时时间
	if c.OperationTimeout > 0 {
		clientOptions.OperationTimeout = time.Second * time.Duration(c.OperationTimeout)
	}

	client, err := pulsar.NewClient(
		clientOptions,
	)

	if err != nil {
		logx.Errorf("could not instantiate Pulsar client: %v", err)
		return nil, err
	}
	return client, nil
}
