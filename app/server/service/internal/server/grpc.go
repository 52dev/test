package server

import (
	modelV1 "go-wind-admin/api/gen/go/model/service/v1"
	tenantV1 "go-wind-admin/api/gen/go/tenant/service/v1"
	"go-wind-admin/app/server/service/internal/service"

	"github.com/go-kratos/kratos/v2/middleware"
	//"github.com/go-kratos/kratos/v2/middleware"
	//"github.com/go-kratos/kratos/v2/middleware/logging"
	//"github.com/go-kratos/kratos/v2/middleware/selector"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	_ "github.com/google/gnostic/openapiv3"
	_ "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"github.com/tx7do/kratos-bootstrap/rpc"
	_ "google.golang.org/genproto/googleapis/api/annotations"
)

// NewGrpcServer new an GRPC server.
func NewGrpcServer(
	ctx *bootstrap.Context,
	middlewares []middleware.Middleware,
	modelService *service.ModelService,
	tenantService *service.TenantService,
) (*grpc.Server, error) {
	cfg := ctx.GetConfig()

	if cfg == nil || cfg.Server == nil || cfg.Server.Grpc == nil {
		return nil, nil
	}

	srv, err := rpc.CreateGrpcServer(cfg, middlewares...)
	if err != nil {
		return nil, err
	}
	modelV1.RegisterModelServiceServer(srv, modelService)
	tenantV1.RegisterTenantServiceServer(srv, tenantService)
	return srv, nil
}
