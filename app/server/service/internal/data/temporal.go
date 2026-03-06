package data

import (
	conf "go-wind-admin/api/gen/go/conf/v1"

	"go.temporal.io/sdk/client"
)

// NewTemporalClient 创建 Temporal 客户端实例
func NewTemporalClient(cfg *conf.Bootstrap) client.Client {
	// 1. 从配置文件获取 Temporal 地址 (假设 conf.Data.Temporal.Addr 存在)
	// 如果配置文件没配，给个默认值或报错
	target := "localhost:7233"
	if cfg.Temporal != nil && cfg.Temporal.Address != "" {
		target = cfg.Temporal.Address
	}

	// 2. 初始化 Options
	opts := client.Options{
		HostPort: target,
	}

	// 3. 创建 Client
	c, err := client.Dial(opts)
	if err != nil {
		// 客户端创建失败通常是致命错误，Panic 或者 Fatal 都可以
		panic(err)
	}

	return c
}
