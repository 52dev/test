package ent

import (
	"context"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/tx7do/go-crud/viewer"
	"go.opentelemetry.io/otel/trace"

	appViewer "go-wind-admin/pkg/entgo/viewer"
	"go-wind-admin/pkg/metadata"
)

// Server 设置 Ent Viewer 到 Context 中的中间件
func Server() middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			data, _ := metadata.FromContext(ctx)
			if data == nil {
				return handler(ctx, req)
			}

			var traceID string
			spanContext := trace.SpanContextFromContext(ctx)
			if spanContext.HasTraceID() {
				traceID = spanContext.TraceID().String()
			}

			userViewer := appViewer.NewUserViewer(
				data.GetUserId(),
				data.GetTenantId(),
				data.GetOrgUnitId(),
				traceID,
				data.GetDataScope(),
			)
			ctx = viewer.WithContext(ctx, userViewer)

			return handler(ctx, req)
		}
	}
}
