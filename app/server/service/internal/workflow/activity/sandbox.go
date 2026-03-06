package activity

import (
	"context"
	"fmt"
	k8sV1 "go-wind-admin/api/gen/go/k8s/service/v1"
	servicev1 "go-wind-admin/api/gen/go/model/service/v1"
	sandboxV1 "go-wind-admin/api/gen/go/sandbox/service/v1"
	tenantV1 "go-wind-admin/api/gen/go/tenant/service/v1"
	workflowV1 "go-wind-admin/api/gen/go/workflow/service/v1"
	"go-wind-admin/app/server/service/internal/biz"
	"go-wind-admin/app/server/service/internal/data"
	"go-wind-admin/pkg/crypto"
	"go-wind-admin/pkg/utils/map_utils"
	"go-wind-admin/pkg/utils/tpl"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
)

// SandboxActivities 包含所有沙箱相关的原子操作
type SandboxActivities struct {
	repo            *data.SandboxRepo
	modelRepo       *data.ModelRepo
	kueueController *biz.KueueController
	appController   *biz.AppController
	log             *log.Helper
}

func NewSandboxActivities(
	ctx *bootstrap.Context,
	repo *data.SandboxRepo,
	modelRepo *data.ModelRepo,
	kueueCtrl *biz.KueueController,
	appCtrl *biz.AppController,
) *SandboxActivities {
	return &SandboxActivities{
		repo:            repo,
		modelRepo:       modelRepo,
		kueueController: kueueCtrl,
		appController:   appCtrl,
		log:             ctx.NewLoggerHelper("activity/sandbox"),
	}
}

// -------------------------------------------------------------------------
// Activity 1: 检查配额、读取模型、准备配置 (纯计算/数据库读取逻辑)
// -------------------------------------------------------------------------
func (a *SandboxActivities) CheckAndPrepare(ctx context.Context, params *workflowV1.CheckAndPrepareRequest) (*workflowV1.CheckAndPrepareResponse, error) {
	req := params.DbRecord
	// 1. 获取模型信息
	modelInfo, err := a.modelRepo.Get(ctx, &servicev1.GetModelRequest{Id: req.Data.ModelId})
	if err != nil {
		return nil, fmt.Errorf("failed to get model info: %v", err)
	}

	if modelInfo.DockerImageRef == nil || *modelInfo.DockerImageRef == "" {
		return nil, fmt.Errorf("model has no docker image ref")
	}

	sandboxName := fmt.Sprintf("aims-sandbox-%s", req.Data.Name)

	// 2. 配额检查
	quota := &tenantV1.Quota{
		Cpu:     req.Data.Quota.Cpu,
		Memory:  req.Data.Quota.Memory,
		Storage: req.Data.Quota.Storage,
		Gpu:     req.Data.Quota.Gpu,
	}

	checkReq := &tenantV1.CheckQuotaReq{
		TenantName: req.Data.TenantName,
		Quota:      quota,
		FlavorName: req.FlavorName,
	}

	allowed, reason, err := a.kueueController.CheckQuotaSufficient(ctx, checkReq)
	if err != nil {
		return nil, fmt.Errorf("quota check error: %v", err)
	}
	if !allowed {
		return nil, fmt.Errorf("quota insufficient: %s", reason)
	}

	// 3. 获取 Flavor 对应的 NodeSelector
	flavorInfo, err := a.kueueController.GetFlavor(ctx, req.FlavorName)
	if err != nil {
		return nil, fmt.Errorf("flavor not found: %v", err)
	}
	nodeSelector := flavorInfo.Labels

	// 4. 准备配置 (合并 Map, 生成密码, 渲染模板)
	commandMap := map_utils.MergeMaps(modelInfo.CommandMapping, req.Data.CommandMapping)

	// 通用密码处理逻辑 (建议使用之前讨论的 modelInfo.AuthAlgo，这里保持你原有逻辑)
	if req.Password != nil {
		passwordHash, err := crypto.GenerateJupyterPasswordHash(*req.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to generate password hash: %v", err)
		}
		commandMap["SandboxPassword"] = passwordHash
	}

	envMap := map_utils.MergeMaps(modelInfo.Env, req.Data.Env)

	commandArgs, err := tpl.RenderTemplates(modelInfo.JupyterCommand, commandMap)
	if err != nil {
		return nil, fmt.Errorf("failed to render command args: %v", err)
	}

	// 5. 构造 K8s 请求对象返回
	deployReq := &k8sV1.CreateDeploymentReq{
		TenantName:    req.Data.TenantName,
		Quota:         quota,
		Name:          sandboxName,
		Image:         *modelInfo.DockerImageRef,
		NodeSelector:  nodeSelector,
		CommandArgs:   commandArgs,
		Env:           envMap,
		Ports:         modelInfo.Ports,
		WritablePaths: modelInfo.WritablePaths,
		Jupyter:       true,
	}
	req.Data.DeploymentName = sandboxName
	req.Data.Quota = quota
	req.Data.ModelId = *modelInfo.Id
	req.Data.ModelName = modelInfo.Name
	req.Data.Env = envMap
	req.Data.CommandMapping = commandMap
	response := &workflowV1.CheckAndPrepareResponse{
		Infrastructure: deployReq,
		DbRecord:       req,
	}
	return response, nil
}

// -------------------------------------------------------------------------
// Activity 2: 创建 K8s 基础设施 (网络 IO 密集型)
// -------------------------------------------------------------------------
func (a *SandboxActivities) CreateInfrastructure(ctx context.Context, req *k8sV1.CreateDeploymentReq) (string, error) {
	// 调用 AppController
	app, err := a.appController.Create(ctx, req)
	if err != nil {
		// 这里不需要处理 AlreadyExists，因为 Temporal 重试时，如果 K8s 已经创建成功，
		// 我们的 Controller 最好能幂等处理 (即如果存在就 Update 或 返回现有)
		// 如果 Controller 返回 AlreadyExists Error，Temporal 默认会重试，直到成功或超时。
		// 建议：AppController.Create 内部如果发现同名资源，应该执行 Patch/Get 操作而不是报错。
		return "", err
	}
	if app == nil || app.Domain == nil {
		return "", fmt.Errorf("app created but domain is empty")
	}
	return *app.Domain, nil
}

// -------------------------------------------------------------------------
// Activity 3: 删除 K8s 基础设施
// -------------------------------------------------------------------------
func (a *SandboxActivities) CleanupInfrastructure(ctx context.Context, tenantName string, deploymentName string) error {
	// 防御性检查
	if tenantName == "" || deploymentName == "" {
		a.log.Warnf("Skip cleaning up infrastructure due to empty tenant/deployment name")
		return nil // 视为空操作成功，避免 Workflow 无限重试卡死
	}
	req := &k8sV1.DeleteDeploymentReq{
		TenantName: tenantName,
		Name:       deploymentName,
	}
	return a.appController.Delete(ctx, req)
}

// -------------------------------------------------------------------------
// Activity 4: 更新数据库状态
// -------------------------------------------------------------------------
type UpdateStatusInput struct {
	Id     uint64
	Status sandboxV1.Sandbox_Status
	// 时间更新类型: "", "start", "finish"
	TimeUpdateType string
	Reason         string // 出错原因
}

func (a *SandboxActivities) UpdateSandboxStatus(ctx context.Context, input UpdateStatusInput) (*sandboxV1.Sandbox, error) {
	// 使用 Repo 里的新方法
	return a.repo.UpdateLifecycle(ctx, input.Id, input.Status, input.TimeUpdateType)
}

// FinalizeBilling 结算计费
func (a *SandboxActivities) FinalizeBilling(ctx context.Context, id uint64) error {
	return a.repo.CalculateAndSaveBilling(ctx, id)
}

func (a *SandboxActivities) UpdateSandbox(ctx context.Context, input *sandboxV1.UpdateSandboxRequest) (*sandboxV1.Sandbox, error) {
	// 1. 先查询数据 (Check & Get)
	sandbox, err := a.repo.Update(ctx, input)
	return sandbox, err
}

// CreateSandboxDBRecord 在数据库中创建初始记录
// 返回创建后的 ID，供后续流程使用
func (a *SandboxActivities) CreateSandboxDBRecord(ctx context.Context, req *sandboxV1.CreateSandboxRequest) (*sandboxV1.Sandbox, error) {
	// 强制设置初始状态
	req.Data.Status = func() *sandboxV1.Sandbox_Status { i := sandboxV1.Sandbox_CREATING; return &i }()

	created, err := a.repo.Create(ctx, req)
	if err != nil {
		return nil, err
	}
	return created, nil
}

// HardDeleteSandboxDBRecord 物理删除数据库记录 (Delete流程最后一步)
func (a *SandboxActivities) HardDeleteSandboxDBRecord(ctx context.Context, req *sandboxV1.DeleteSandboxRequest) error {
	return a.repo.Delete(ctx, req)
}

func (a *SandboxActivities) StartInfrastructure(ctx context.Context, req *k8sV1.StartDeploymentReq) error {
	return a.appController.Start(ctx, req)
}

func (a *SandboxActivities) StopInfrastructure(ctx context.Context, req *k8sV1.StopDeploymentReq) error {
	return a.appController.Stop(ctx, req)
}

func (a *SandboxActivities) RestartInfrastructure(ctx context.Context, req *k8sV1.RolloutDeploymentReq) error {
	return a.appController.Rollout(ctx, req)
}
