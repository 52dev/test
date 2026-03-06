package data

import (
	"fmt"
	//"flag"
	conf "go-wind-admin/api/gen/go/conf/v1"

	"github.com/tx7do/kratos-bootstrap/bootstrap"
)

// NewBootstrapConfig 创建 Bootstrap 配置
func NewBootstrapConfig(ctx *bootstrap.Context) *conf.Bootstrap {
	l := ctx.NewLoggerHelper("conf/data/server-service")

	var cfg *conf.Bootstrap
	rawCfg, ok := ctx.GetCustomConfig("bootstrap_extra")
	if ok {
		cfg = rawCfg.(*conf.Bootstrap)
	}
	if cfg == nil {
		l.Error("bootstrap_extra is nil")
		panic("bootstrap_extra is nil")
	}
	fmt.Println(rawCfg)
	return cfg
}
