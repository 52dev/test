package main

import (
	"context"
	workflowDef "go-wind-admin/app/server/service/internal/workflow/workflow"

	"fmt"
	confv1 "go-wind-admin/api/gen/go/conf/v1"
	"go-wind-admin/pkg/service"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/spf13/cobra"
	conf "github.com/tx7do/kratos-bootstrap/api/gen/go/conf/v1"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	bConfig "github.com/tx7do/kratos-bootstrap/config"
	bLogger "github.com/tx7do/kratos-bootstrap/logger"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"go-wind-admin/app/server/service/internal/workflow/activity"
)

var (
	version = "1.0.0"
)

// InitAppFunc 应用初始化函数类型
type InitAppFunc func(ctx *bootstrap.Context) (app *WorkerApp, cleanup func(), err error)

// WorkerApp 封装了 Worker 所需的所有组件
type WorkerApp struct {
	activities     *activity.SandboxActivities
	temporalClient client.Client
}

func newWorkerApp(
	acts *activity.SandboxActivities,
	tClient client.Client,
) *WorkerApp {
	return &WorkerApp{
		activities:     acts,
		temporalClient: tClient,
	}
}

var (
	flags = bootstrap.NewCommandFlags()
)

func boot(ctx *bootstrap.Context, initApp InitAppFunc) error {
	if err := bConfig.LoadBootstrapConfig(flags.Conf); err != nil {
		panic(err)
	}

	cfg := &confv1.Bootstrap{}
	ctx.RegisterCustomConfig("bootstrap_extra", cfg)
	fmt.Println(cfg.Kubernetes)
	ctx.PrintAppInfo()

	// 3. 依赖注入 (调用 wire_gen.go 生成的代码)
	app, cleanup, err := initApp(ctx)
	if err != nil {
		panic(err)
	}
	// 5. 创建 Worker
	w := worker.New(app.temporalClient, "sandbox-task-queue", worker.Options{})

	// 6. 注册 Workflows
	w.RegisterWorkflow(workflowDef.CreateSandboxWorkflow)
	w.RegisterWorkflow(workflowDef.DeleteSandboxWorkflow)
	w.RegisterWorkflow(workflowDef.StopSandboxWorkflow)
	w.RegisterWorkflow(workflowDef.StartSandboxWorkflow)
	w.RegisterWorkflow(workflowDef.RestartSandboxWorkflow)
	w.RegisterWorkflow(workflowDef.SandboxTimerWorkflow)
	// 7. 注册 Activities (这里最关键，把注入好的 Biz/Data 逻辑注册进去)
	w.RegisterActivity(app.activities)

	// 8. 启动监听
	log.Info("Worker started successfully")
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatal("Unable to start worker", err)
	}

	fmt.Println(111)
	defer cleanup()
	fmt.Println(222)
	return nil
}

func RunApp(ctx *bootstrap.Context, initApp InitAppFunc, opts ...func(root *cobra.Command)) error {
	if ctx == nil {
		return fmt.Errorf("bootstrap context is nil")
	}

	// 注入命令行参数
	root := bootstrap.NewRootCmd(flags, func(cmd *cobra.Command, args []string) error {
		return boot(ctx, initApp)
	})

	// 允许调用方定制 root（如添加子命令、注册额外 flag 等）
	for _, opt := range opts {
		if opt != nil {
			opt(root)
		}
	}

	// 如果 flags 实现了 Register，就在 Execute 前注册到命令上，确保 cobra 能解析这些 flag
	if rb, ok := interface{}(flags).(interface{ Register(cmd *cobra.Command) }); ok {
		rb.Register(root)
	}

	if err := root.Execute(); err != nil {
		return err
	}
	return nil
}

func runApp() error {
	appInfo := &conf.AppInfo{
		Project: service.Project,
		AppId:   service.WorkerService,
		Version: version,
	}
	bc := bConfig.GetBootstrapConfig()
	// 2. 初始化上下文和日志
	ctx := bootstrap.NewContextWithParam(
		context.Background(),
		appInfo,
		bc,
		bLogger.NewLoggerProvider(bc.Logger, appInfo),
	)
	ctx.RegisterCustomConfig("bootstrap_extra", &confv1.Bootstrap{})
	return RunApp(ctx, initApp)
}

func main() {
	if err := runApp(); err != nil {
		panic(err)
	}
}
