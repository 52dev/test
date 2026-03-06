package data

import (
	"context"
	conf "go-wind-admin/api/gen/go/conf/v1"
	"go-wind-admin/pkg/lakefs"

	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
)

// NewLakeFSClient 创建lakefs客户端
func NewLakeFSClient(ctx *bootstrap.Context, cfg *conf.Bootstrap) (*lakefs.ClientWithResponses, error) {
	log := ctx.NewLoggerHelper("lakefs/data/server-service")
	client, err := lakefs.NewClientWithResponses(
		cfg.Lakefs.Endpoint,
		lakefs.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.SetBasicAuth(cfg.Lakefs.AccessKeyId, cfg.Lakefs.SecretAccessKey)
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("客户端初始化失败: %v", err)
		return nil, err
	}
	log.Info("🚀 客户端初始化成功")

	return client, err
}
