package server

import (
	//adminV1 "go-wind-admin/api/gen/go/admin/service/v1"

	serverV1 "go-wind-admin/api/gen/go/server/service/v1"
	"go-wind-admin/app/server/service/internal/service"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/middleware/logging"
	"github.com/go-kratos/kratos/v2/middleware/selector"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	swaggerUI "github.com/tx7do/kratos-swagger-ui"

	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"github.com/tx7do/kratos-bootstrap/rpc"

	"go-wind-admin/app/server/service/cmd/server/assets"
)

// NewRestMiddleware 创建中间件
func NewRestMiddleware(
	ctx *bootstrap.Context,
// accessTokenChecker auth.AccessTokenChecker,
// apiAuditLogRepo *data.ApiAuditLogRepo
) []middleware.Middleware {
	var ms []middleware.Middleware
	ms = append(ms, logging.Server(ctx.GetLogger()))

	//ms = append(ms, applogging.Server(
	////applogging.WithWriteApiLogFunc(func(ctx context.Context, data *auditV1.ApiAuditLog) error {
	////	// TODO 如果系统的负载比较小，可以同步写入数据库，否则，建议使用异步方式，即投递进队列。
	////	//return apiAuditLogRepo.Create(ctx, &auditV1.CreateApiAuditLogRequest{Data: data})
	////}),
	////applogging.WithWriteLoginLogFunc(func(ctx context.Context, data *auditV1.LoginAuditLog) error {
	////	// TODO 如果系统的负载比较小，可以同步写入数据库，否则，建议使用异步方式，即投递进队列。
	////	//return loginLogRepo.Create(ctx, &auditV1.CreateLoginAuditLogRequest{Data: data})
	////}),
	//))
	//
	//// add white list for authentication.
	rpc.AddWhiteList(
		//adminV1.OperationAuthenticationServiceLogin,
		//OperationFileTransferServiceDownloadFile,
		//OperationFileTransferServicePostUploadFile,
		//OperationFileTransferServicePutUploadFile,
	)

	//ms = append(ms, func(handler middleware.Handler) middleware.Handler {
	//	return func(ctx context.Context, req interface{}) (interface{}, error) {
	//		tr, ok := transport.FromServerContext(ctx)
	//		if !ok {
	//			return handler(ctx, req)
	//		}
	//		ht, ok := tr.(http.Transporter)
	//		if !ok {
	//			return handler(ctx, req)
	//		}
	//
	//		reqHeader := ht.RequestHeader()
	//		origin := reqHeader.Get("Origin")
	//		if origin == "" {
	//			return handler(ctx, req)
	//		}
	//
	//		resHeader := ht.ReplyHeader()
	//		resHeader.Set("Access-Control-Allow-Origin", origin)
	//		resHeader.Set("Vary", "Origin")
	//		resHeader.Set("Access-Control-Allow-Methods", "OPTIONS, GET, POST, PUT, PATCH, DELETE")
	//		resHeader.Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Idempotent")
	//		resHeader.Set("Access-Control-Allow-Credentials", "true")
	//
	//		if reqHeader.Get("Access-Control-Request-Method") != "" &&
	//			reqHeader.Get("Access-Control-Request-Headers") != "" &&
	//			tr.Operation() == "" {
	//			// 预检请求，直接返回
	//			return struct{}{}, nil
	//		}
	//
	//		return handler(ctx, req)
	//	}
	//})
	ms = append(ms, selector.Server(
		//auth.Server(
		//	//auth.WithAccessTokenChecker(accessTokenChecker),
		//	auth.WithInjectMetadata(false),
		//	auth.WithInjectEnt(true),
		//),
		//authz.Server(authorizer.Engine()),
	).
		Match(rpc.NewRestWhiteListMatcher()).
		Build(),
	)

	return ms
}

// NewRestServer new an REST server.
func NewRestServer(
	ctx *bootstrap.Context,
	middlewares []middleware.Middleware,

	modelService *service.ModelService,
	sandboxService *service.SandboxService,
	tenantService *service.TenantService,
) (*http.Server, error) {
	cfg := ctx.GetConfig()

	if cfg == nil || cfg.Server == nil || cfg.Server.Rest == nil {
		return nil, nil
	}

	srv, err := rpc.CreateRestServer(cfg,
		middlewares...,
	)
	if err != nil {
		return nil, err
	}

	//serverV1.RegisterModelServiceServer(grpcSrv, modelService)
	serverV1.RegisterModelServiceHTTPServer(srv, modelService)
	serverV1.RegisterSandboxServiceHTTPServer(srv, sandboxService)
	serverV1.RegisterTenantServiceHTTPServer(srv, tenantService)
	srv.Handle("/metrics", promhttp.Handler())
	// 注册文件传输服务，用于处理文件上传下载等功能
	// TODO 它不能够使用代码生成器生成的Handler，需要手动注册。代码生成器生成的Handler无法处理文件上传下载的请求。
	// 但，代码生成器生成代码可以提供给OpenAPI使用。

	if cfg.GetServer().GetRest().GetEnableSwagger() {
		swaggerUI.RegisterSwaggerUIServerWithOption(
			srv,
			swaggerUI.WithTitle("AIMS server Service API"),
			swaggerUI.WithMemoryData(assets.OpenApiData, "yaml"),
		)
	}

	return srv, nil
}
