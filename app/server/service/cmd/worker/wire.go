//go:build wireinject
// +build wireinject

//go:generate go run github.com/google/wire/cmd/wire

package main

import (
	"github.com/google/wire"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	bizProviders "go-wind-admin/app/server/service/internal/biz/providers"
	dataProviders "go-wind-admin/app/server/service/internal/data/providers"

	activityProviders "go-wind-admin/app/server/service/internal/workflow/activity/providers"
)

// initApp 注入 Worker 应用所需的依赖
func initApp(*bootstrap.Context) (*WorkerApp, func(), error) {
	panic(wire.Build(
		// 1. 基础依赖 (Data, Biz, Activity)
		dataProviders.ProviderSet,
		bizProviders.ProviderSet,

		// 2. 注入 Activity (需要手动写个 ProviderSet 或者直接 New)
		activityProviders.ProviderSet,

		// 3. Worker App 结构体
		newWorkerApp,
	))
}
