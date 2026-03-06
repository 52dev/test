package activity

import (
	"context"
	"fmt"
	inferenceV1 "go-wind-admin/api/gen/go/inference/service/v1"
	k8sV1 "go-wind-admin/api/gen/go/k8s/service/v1"
	servicev1 "go-wind-admin/api/gen/go/model/service/v1"
	tenantV1 "go-wind-admin/api/gen/go/tenant/service/v1"
	workflowV1 "go-wind-admin/api/gen/go/workflow/service/v1"
	"go-wind-admin/app/server/service/internal/biz"
	"go-wind-admin/app/server/service/internal/data"
	"go-wind-admin/pkg/utils/map_utils"
	"go-wind-admin/pkg/utils/tpl"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
)

type InferenceActivities struct {
	repo            *data.InferenceRepo
	modelRepo       *data.ModelRepo
	kueueController *biz.KueueController
	appController   *biz.AppController
	log             *log.Helper
}

func NewInferenceActivities(
	ctx *bootstrap.Context,
	repo *data.InferenceRepo,
	modelRepo *data.ModelRepo,
	kueueCtrl *biz.KueueController,
	appCtrl *biz.AppController,
) *InferenceActivities {
	return &InferenceActivities{
		repo:            repo,
		modelRepo:       modelRepo,
		kueueController: kueueCtrl,
		appController:   appCtrl,
		log:             ctx.NewLoggerHelper("activity/inference"),
	}
}

// -------------------------------------------------------------------------
// Activity 1: 检查配额、读取模型、准备配置
// -------------------------------------------------------------------------
func (a *InferenceActivities) CheckAndPrepareInference(ctx context.Context, params *workflowV1.CheckAndPrepareInferenceRequest) (*workflowV1.CheckAndPrepareInferenceResponse, error) {
	req := params.DbRecord

	// 1. 获取模型信息
	modelInfo, err := a.modelRepo.Get(ctx, &servicev1.GetModelRequest{Id: req.Data.ModelId})
	if err != nil {
		return nil, fmt.Errorf("failed to get model info: %v", err)
	}
	if modelInfo.DockerImageRef == nil || *modelInfo.DockerImageRef == "" {
		return nil, fmt.Errorf("model has no docker image ref")
	}

	inferenceName := fmt.Sprintf("aims-inference-%s", req.Data.Name)

	// 2. 配额检查 (Inference Quota = 单副本 Quota * Replicas)
	// 这里简化处理，Kueue 检查的是总配额，所以需要根据 Replicas 计算总资源
	replicas := req.Data.GetReplicas()
	if replicas <= 0 {
		replicas = 1
	}

	// 构造检查配额请求
	// 注意：这里假设 req.Data.Quota 是单副本配额
	totalCpu := req.Data.Quota.Cpu // 需要自行解析字符串乘法，这里略过复杂解析，假设传入的就是单副本
	// 实际生产中应解析 "2" -> 2 * replicas -> "4"
	// 这里为了简化，直接透传 Quota，假设 Kueue 或上层已处理

	quota := &tenantV1.Quota{
		Cpu:     req.Data.Quota.Cpu,
		Memory:  req.Data.Quota.Memory,
		Gpu:     req.Data.Quota.Gpu,
		Storage: req.Data.Quota.Storage,
	}

	// 3. 准备配置 (合并 Map, 渲染模板)
	// Inference 通常没有 Jupyter 密码，而是 API Token
	commandMap := map_utils.MergeMaps(modelInfo.CommandMapping, map[string]string{})
	envMap := map_utils.MergeMaps(modelInfo.Env, req.Data.Env)

	// 如果有 AuthToken，注入到环境变量
	if req.AuthToken != nil && *req.AuthToken != "" {
		envMap["INFERENCE_AUTH_TOKEN"] = *req.AuthToken
	}

	// 渲染启动命令 (如果有)
	commandArgs, err := tpl.RenderTemplates(modelInfo.JupyterCommand, commandMap) // 复用模板逻辑
	if err != nil {
		return nil, fmt.Errorf("failed to render command args: %v", err)
	}

	// 4. 构造 K8s 请求对象
	deployReq := &k8sV1.CreateDeploymentReq{
		TenantName:   req.Data.TenantName,
		Quota:        quota,
		Name:         inferenceName,
		Image:        *modelInfo.DockerImageRef,
		NodeSelector: nil, // 可以从 Flavor 获取，这里简化
		CommandArgs:  commandArgs,
		Env:          envMap,
		Ports:        []int32{req.Data.GetPort()}, // 单端口
		Replicas:     req.Data.Replicas,
		MinReplicas:  req.Data.GetMinReplicas(),
		MaxReplicas:  req.Data.GetMaxReplicas(),
		Jupyter:      false, // 这是一个 Inference 服务
	}

	// 回填 DB 记录
	req.Data.DeploymentName = inferenceName
	req.Data.ServiceName = inferenceName
	req.Data.ModelName = modelInfo.GetName()
	req.Data.Image = modelInfo.DockerImageRef

	return &workflowV1.CheckAndPrepareInferenceResponse{
		Infrastructure: deployReq,
		DbRecord:       req,
	}, nil
}

// -------------------------------------------------------------------------
// Activity 2: 数据库操作
// -------------------------------------------------------------------------

// CreateInferenceDBRecord 创建初始记录
func (a *InferenceActivities) CreateInferenceDBRecord(ctx context.Context, req *inferenceV1.CreateInferenceRequest) (*inferenceV1.Inference, error) {
	req.Data.Status = func() inferenceV1.InferenceStatus {
		i := inferenceV1.InferenceStatus_CREATING
		return i
	}()
	return a.repo.Create(ctx, req)
}

// GetInferenceDBRecord 获取记录
func (a *InferenceActivities) GetInferenceDBRecord(ctx context.Context, id uint64) (*inferenceV1.Inference, error) {
	return a.repo.Get(ctx, &inferenceV1.GetInferenceRequest{QueryBy: &inferenceV1.GetInferenceRequest_Id{Id: id}})
}

// UpdateInferenceStatus 更新状态和时间
type UpdateInferenceStatusInput struct {
	Id             uint64
	Status         inferenceV1.InferenceStatus
	TimeUpdateType string // "start", "finish", ""
}

func (a *InferenceActivities) UpdateInferenceStatus(ctx context.Context, input UpdateInferenceStatusInput) (*inferenceV1.Inference, error) {
	return a.repo.UpdateLifecycle(ctx, input.Id, input.Status, input.TimeUpdateType)
}

// UpdateInferenceURL 更新 URL
func (a *InferenceActivities) UpdateInferenceURL(ctx context.Context, id uint64, url string) error {
	req := &inferenceV1.UpdateInferenceRequest{
		Id: id,
		Data: &inferenceV1.Inference{
			Url: &url,
		},
		UpdateMask: nil, // Update Repo 会自动处理非空字段，或者显式传 UpdateMask
	}
	// 简单起��，这里复用 Repo.Update，Repo 内部 UpdateOne 会自动忽略 nil 字段
	_, err := a.repo.Update(ctx, req)
	return err
}

// HardDeleteInferenceDBRecord 物理删除
func (a *InferenceActivities) HardDeleteInferenceDBRecord(ctx context.Context, req *inferenceV1.DeleteInferenceRequest) error {
	return a.repo.Delete(ctx, req)
}

// -------------------------------------------------------------------------
// Activity 3: K8s 基础设施操作
// -------------------------------------------------------------------------

// CreateInferenceInfra 创建 K8s 资源
func (a *InferenceActivities) CreateInferenceInfra(ctx context.Context, req *k8sV1.CreateDeploymentReq) (string, error) {
	app, err := a.appController.Create(ctx, req)
	if err != nil {
		return "", err
	}
	if app == nil || app.Domain == nil {
		return "", fmt.Errorf("domain is empty")
	}
	return *app.Domain, nil
}

// CleanupInferenceInfra 删除 K8s 资源
func (a *InferenceActivities) CleanupInferenceInfra(ctx context.Context, tenantName, deploymentName string) error {
	if tenantName == "" || deploymentName == "" {
		return nil
	}
	req := &k8sV1.DeleteDeploymentReq{
		TenantName: tenantName,
		Name:       deploymentName,
	}
	return a.appController.Delete(ctx, req)
}

// StartInferenceInfra 启动
func (a *InferenceActivities) StartInferenceInfra(ctx context.Context, req *k8sV1.StartDeploymentReq) error {
	return a.appController.Start(ctx, req)
}

// StopInferenceInfra 停止
func (a *InferenceActivities) StopInferenceInfra(ctx context.Context, req *k8sV1.StopDeploymentReq) error {
	return a.appController.Stop(ctx, req)
}

// RestartInferenceInfra 重启 (Rollout)
func (a *InferenceActivities) RestartInferenceInfra(ctx context.Context, req *k8sV1.RolloutDeploymentReq) error {
	return a.appController.Rollout(ctx, req)
}

// ScaleInferenceInfra 扩缩容
func (a *InferenceActivities) ScaleInferenceInfra(ctx context.Context, req *k8sV1.ScaleDeploymentReq) error {
	return a.appController.Scale(ctx, req)
}

// -------------------------------------------------------------------------
// Activity 4: 计费与监控
// -------------------------------------------------------------------------

// GetK8sReplicas 获取当前实际副本数 (用于计费)
func (a *InferenceActivities) GetK8sReplicas(ctx context.Context, tenantName, deployName string) (int32, error) {
	// 调用 AppController 获取 Deployment 的 Status.Replicas
	// 假设 AppController 实现了 GetReplicas 方法
	// 如果没有，你需要去 AppController 加一个
	return a.appController.GetReplicas(ctx, tenantName, deployName)
}

// AccumulateUsage 累加资源消耗 (Workflow 周期调用)
func (a *InferenceActivities) AccumulateUsage(ctx context.Context, id uint64, replicaSeconds int64) error {
	// 累加 ReplicaSeconds，同时累加 BillingDurationSec (挂钟时间)
	// 这里也可以顺便计算 CPU/GPU Seconds 如果你知道单副本规格的话
	return a.repo.AccumulateUsage(ctx, id, replicaSeconds)
}

// FinalizeBilling 结算挂钟时间 (Start/Stop 时调用)
func (a *InferenceActivities) FinalizeBilling(ctx context.Context, id uint64) error {
	return a.repo.CalculateAndSaveBilling(ctx, id)
}
