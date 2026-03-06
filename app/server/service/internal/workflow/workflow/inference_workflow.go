package workflow

import (
	"fmt"
	"time"

	inferenceV1 "go-wind-admin/api/gen/go/inference/service/v1"
	k8sV1 "go-wind-admin/api/gen/go/k8s/service/v1"
	workflowV1 "go-wind-admin/api/gen/go/workflow/service/v1"
	"go-wind-admin/app/server/service/internal/workflow/activity"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// InferenceLifecycleWorkflow [修复 1]:
// 取代原有的 CreateWorkflow。这是一个长运行 Workflow (Entity Pattern)。
// 它负责：创建 -> 运行(监控计费) -> 响应信号(停/启/删) -> 退出。
func InferenceLifecycleWorkflow(ctx workflow.Context, req *inferenceV1.CreateInferenceRequest) error {
	logger := workflow.GetLogger(ctx)
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 10,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.InferenceActivities

	// ------------------- 阶段 1: 资源初始化 -------------------
	// 1. Check & Prepare
	prepareOutput := &workflowV1.CheckAndPrepareInferenceResponse{}
	if err := workflow.ExecuteActivity(ctx, a.CheckAndPrepareInference, &workflowV1.CheckAndPrepareInferenceRequest{DbRecord: req}).Get(ctx, prepareOutput); err != nil {
		return err
	}

	// 2. DB Create
	var record *inferenceV1.Inference
	if err := workflow.ExecuteActivity(ctx, a.CreateInferenceDBRecord, prepareOutput.DbRecord).Get(ctx, &record); err != nil {
		return err
	}
	resourceID := record.GetId()

	// 3. K8s Create (含 HPA)
	var domainUrl string
	if err := workflow.ExecuteActivity(ctx, a.CreateInferenceInfra, prepareOutput.Infrastructure).Get(ctx, &domainUrl); err != nil {
		// 回滚逻辑：清理半成品的 K8s 资源并标记 DB 为 Error
		_ = workflow.ExecuteActivity(ctx, a.CleanupInferenceInfra, req.Data.TenantName, prepareOutput.Infrastructure.Name).Get(ctx, nil)
		_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, activity.UpdateInferenceStatusInput{Id: resourceID, Status: inferenceV1.InferenceStatus_ERROR}).Get(ctx, nil)
		return err
	}

	// 4. Update Status -> RUNNING
	_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceURL, resourceID, domainUrl).Get(ctx, nil)
	_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, activity.UpdateInferenceStatusInput{Id: resourceID, Status: inferenceV1.InferenceStatus_RUNNING, TimeUpdateType: "start"}).Get(ctx, nil)

	// ------------------- 阶段 2: 启动计费监控 -------------------
	// 使用 Child Workflow 启动监控，父流程不退出，保持对子流程的控制
	monitorCtx, monitorCancel := workflow.WithCancel(ctx) // 用于手动停止监控
	monitorID := fmt.Sprintf("monitor-inference-%d", resourceID)
	childOpts := workflow.ChildWorkflowOptions{
		WorkflowID:        monitorID,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_TERMINATE, // 父流程结束(如Delete)时，监控必须结束
		TaskQueue:         "inference-task-queue",
	}
	monitorCtx = workflow.WithChildOptions(monitorCtx, childOpts)

	// 异步启动监控
	workflow.ExecuteChildWorkflow(monitorCtx, InferenceBillingMonitorWorkflow, resourceID, record.TenantName, record.DeploymentName)

	// ------------------- 阶段 3: 信号监听循环 (Daemon) -------------------
	selector := workflow.NewSelector(ctx)

	// 信号通道定义
	sigStop := workflow.GetSignalChannel(ctx, "SIGNAL_STOP")
	sigStart := workflow.GetSignalChannel(ctx, "SIGNAL_START")
	sigDelete := workflow.GetSignalChannel(ctx, "SIGNAL_DELETE")
	sigScale := workflow.GetSignalChannel(ctx, "SIGNAL_SCALE")

	for {
		var exitLoop bool
		selector.AddReceive(sigStop, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			logger.Info("Received STOP signal")
			// 1. 停止监控
			monitorCancel()
			// 2. K8s Scale to 0
			_ = workflow.ExecuteActivity(ctx, a.StopInferenceInfra, &k8sV1.StopDeploymentReq{TenantName: record.TenantName, Name: record.DeploymentName}).Get(ctx, nil)
			// 3. Update Status
			_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, activity.UpdateInferenceStatusInput{Id: resourceID, Status: inferenceV1.InferenceStatus_STOPPED, TimeUpdateType: "finish"}).Get(ctx, nil)
			_ = workflow.ExecuteActivity(ctx, a.FinalizeBilling, resourceID).Get(ctx, nil)
		})

		selector.AddReceive(sigStart, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			logger.Info("Received START signal")
			// 1. K8s Start (恢复副本数)
			// 需先获取当前配置中的 replicas，这里简化为 1 或从 DB 查
			startReq := &k8sV1.StartDeploymentReq{TenantName: record.TenantName, Name: record.DeploymentName}
			_ = workflow.ExecuteActivity(ctx, a.StartInferenceInfra, startReq).Get(ctx, nil)
			// 2. Update Status
			_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, activity.UpdateInferenceStatusInput{Id: resourceID, Status: inferenceV1.InferenceStatus_RUNNING, TimeUpdateType: "start"}).Get(ctx, nil)

			// 3. 重启监控 (重建 Context)
			monitorCtx, monitorCancel = workflow.WithCancel(ctx)
			monitorCtx = workflow.WithChildOptions(monitorCtx, childOpts)
			workflow.ExecuteChildWorkflow(monitorCtx, InferenceBillingMonitorWorkflow, resourceID, record.TenantName, record.DeploymentName)
		})

		selector.AddReceive(sigScale, func(c workflow.ReceiveChannel, _ bool) {
			var scaleReq k8sV1.ScaleDeploymentReq
			c.Receive(ctx, &scaleReq)
			// 执行扩缩容逻辑
			_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, activity.UpdateInferenceStatusInput{Id: resourceID, Status: inferenceV1.InferenceStatus_SCALING}).Get(ctx, nil)
			_ = workflow.ExecuteActivity(ctx, a.ScaleInferenceInfra, &scaleReq).Get(ctx, nil)
			_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, activity.UpdateInferenceStatusInput{Id: resourceID, Status: inferenceV1.InferenceStatus_RUNNING}).Get(ctx, nil)
		})

		selector.AddReceive(sigDelete, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			logger.Info("Received DELETE signal")
			// 1. 停止监控
			monitorCancel()
			// 2. 清理 K8s
			_ = workflow.ExecuteActivity(ctx, a.CleanupInferenceInfra, record.TenantName, record.DeploymentName).Get(ctx, nil)
			// 3. 结算 & 删 DB
			_ = workflow.ExecuteActivity(ctx, a.FinalizeBilling, resourceID).Get(ctx, nil)
			_ = workflow.ExecuteActivity(ctx, a.HardDeleteInferenceDBRecord, &inferenceV1.DeleteInferenceRequest{Id: resourceID}).Get(ctx, nil)

			exitLoop = true // 退出循环，结束 Workflow
		})

		selector.Select(ctx)
		if exitLoop {
			break
		}
	}

	return nil
}

// InferenceBillingMonitorWorkflow [修复 3]: 计费与数据一致性
// 优化点：缩短轮询间隔 + 本地聚合写 DB + 余额熔断检测
func InferenceBillingMonitorWorkflow(ctx workflow.Context, id uint64, tenantName, deployName string) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.InferenceActivities
	logger := workflow.GetLogger(ctx)

	// 10秒轮询一次，比30秒更准
	interval := time.Second * 10
	var accumulatedSeconds int64 = 0

	for {
		// 检查 Workflow 是否被 Cancel
		if ctx.Err() != nil {
			return nil
		}

		// 1. 获取 K8s 真实副本数
		var currentReplicas int32
		err := workflow.ExecuteActivity(ctx, a.GetK8sReplicas, tenantName, deployName).Get(ctx, &currentReplicas)

		if err == nil && currentReplicas > 0 {
			// 2. 计算：副本数 * 时间间隔 (秒)
			usage := int64(currentReplicas) * int64(interval.Seconds())
			accumulatedSeconds += usage

			// 3. 批量写入 DB (每满 60 秒写一次，减少 DB IO)
			// 或者你可以每次都写，取决于 DB 抗压能力。这里演示每分钟聚合。
			if accumulatedSeconds >= 60 {
				err = workflow.ExecuteActivity(ctx, a.AccumulateUsage, id, accumulatedSeconds).Get(ctx, nil)
				if err != nil {
					// TODO: 如果 Activity 返回 "余额不足" 错误，发送 SIGNAL_STOP 给父流程
					logger.Error("Billing failed", "error", err)
				} else {
					accumulatedSeconds = 0 // 重置
				}
			}
		}

		// 4. Sleep
		_ = workflow.Sleep(ctx, interval)
	}
}

// CreateInferenceWorkflow 创建推理服务
func CreateInferenceWorkflow(ctx workflow.Context, params *inferenceV1.CreateInferenceRequest) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 10,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.InferenceActivities

	// 1. Check & Prepare
	prepareInput := &workflowV1.CheckAndPrepareInferenceRequest{DbRecord: params}
	prepareOutput := &workflowV1.CheckAndPrepareInferenceResponse{}
	if err := workflow.ExecuteActivity(ctx, a.CheckAndPrepareInference, prepareInput).Get(ctx, prepareOutput); err != nil {
		return err
	}

	// 2. DB Create
	var record *inferenceV1.Inference
	if err := workflow.ExecuteActivity(ctx, a.CreateInferenceDBRecord, prepareOutput.DbRecord).Get(ctx, &record); err != nil {
		return err
	}

	// 3. K8s Create (含 HPA 配置)
	var domainUrl string
	if err := workflow.ExecuteActivity(ctx, a.CreateInferenceInfra, prepareOutput.Infrastructure).Get(ctx, &domainUrl); err != nil {
		// 回滚：清理 K8s (如果部分创建) 和 更新 DB 为 Error
		disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
		_ = workflow.ExecuteActivity(disconnectedCtx, a.CleanupInferenceInfra, params.Data.TenantName, prepareOutput.Infrastructure.Name).Get(disconnectedCtx, nil)

		failParam := activity.UpdateInferenceStatusInput{
			Id: record.GetId(), Status: inferenceV1.InferenceStatus_ERROR,
		}
		_ = workflow.ExecuteActivity(disconnectedCtx, a.UpdateInferenceStatus, failParam).Get(disconnectedCtx, nil)
		return err
	}

	// 4. Update Status -> RUNNING
	err := workflow.ExecuteActivity(ctx, a.UpdateInferenceURL, record.GetId(), domainUrl).Get(ctx, nil)
	if err != nil {
		return err
	}

	statusParam := activity.UpdateInferenceStatusInput{
		Id: record.GetId(), Status: inferenceV1.InferenceStatus_RUNNING, TimeUpdateType: "start",
	}
	if err := workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, statusParam).Get(ctx, nil); err != nil {
		return err
	}

	// 5. 启动 Billing Monitor (Child Workflow)
	// 这个监控会一直运行，直到 Inference 被删除或停止
	monitorID := fmt.Sprintf("monitor-inference-%d", record.GetId())
	childOpts := workflow.ChildWorkflowOptions{
		WorkflowID:        monitorID,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON, // 父流程结束，监控继续跑
		TaskQueue:         "inference-task-queue",
	}
	childCtx := workflow.WithChildOptions(ctx, childOpts)

	// 异步启动，不等待结果
	workflow.ExecuteChildWorkflow(childCtx, InferenceBillingMonitorWorkflow, record.GetId(), record.TenantName, record.DeploymentName)

	return nil
}

// ScaleInferenceWorkflow Scale 扩缩容
func ScaleInferenceWorkflow(ctx workflow.Context, req *inferenceV1.ScaleInferenceRequest) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.InferenceActivities

	// 1. 获取当前信息 (Tenant, Deployment Name)
	var info *inferenceV1.Inference
	if err := workflow.ExecuteActivity(ctx, a.GetInferenceDBRecord, req.GetId()).Get(ctx, &info); err != nil {
		return err
	}

	// 2. Update Status -> SCALING
	statusParam := activity.UpdateInferenceStatusInput{
		Id: req.GetId(), Status: inferenceV1.InferenceStatus_SCALING,
	}
	_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, statusParam).Get(ctx, nil)

	// 3. K8s Scale
	k8sReq := &k8sV1.ScaleDeploymentReq{
		TenantName:  info.TenantName,
		Name:        info.DeploymentName,
		Replicas:    req.Replicas,
		MinReplicas: req.MinReplicas,
		MaxReplicas: req.MaxReplicas,
	}
	err := workflow.ExecuteActivity(ctx, a.ScaleInferenceInfra, k8sReq).Get(ctx, nil)

	// 4. Update Status -> RUNNING (or Error)
	finalStatus := inferenceV1.InferenceStatus_RUNNING
	if err != nil {
		finalStatus = inferenceV1.InferenceStatus_ERROR
	}
	finalParam := activity.UpdateInferenceStatusInput{
		Id: req.GetId(), Status: finalStatus,
	}
	_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, finalParam).Get(ctx, nil)

	return err
}

// StopInferenceWorkflow Stop 停止服务
func StopInferenceWorkflow(ctx workflow.Context, req *inferenceV1.StopInferenceRequest) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 2,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.InferenceActivities

	// 1. Get Info
	var info *inferenceV1.Inference
	if err := workflow.ExecuteActivity(ctx, a.GetInferenceDBRecord, req.GetId()).Get(ctx, &info); err != nil {
		return err
	}

	// 2. Cancel Billing Monitor
	monitorID := fmt.Sprintf("monitor-inference-%d", req.GetId())
	_ = workflow.RequestCancelExternalWorkflow(ctx, monitorID, "").Get(ctx, nil)

	// 3. Update Status -> STOPPED & Finalize Billing (Wall Clock)
	statusParam := activity.UpdateInferenceStatusInput{
		Id: req.GetId(), Status: inferenceV1.InferenceStatus_STOPPED, TimeUpdateType: "finish",
	}
	if err := workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, statusParam).Get(ctx, nil); err != nil {
		return err
	}
	_ = workflow.ExecuteActivity(ctx, a.FinalizeBilling, req.GetId()).Get(ctx, nil)

	// 4. K8s Stop (Scale to 0)
	stopReq := &k8sV1.StopDeploymentReq{TenantName: info.TenantName, Name: info.DeploymentName}
	if err := workflow.ExecuteActivity(ctx, a.StopInferenceInfra, stopReq).Get(ctx, nil); err != nil {
		return err
	}

	return nil
}

// StartInferenceWorkflow Start 启动服务 (Stop 后再 Start)
func StartInferenceWorkflow(ctx workflow.Context, req *inferenceV1.StartInferenceRequest) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 2,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.InferenceActivities

	// 1. Get Info (包含 Replicas 配置)
	var info *inferenceV1.Inference
	if err := workflow.ExecuteActivity(ctx, a.GetInferenceDBRecord, req.GetId()).Get(ctx, &info); err != nil {
		return err
	}

	// 2. K8s Start
	startReq := &k8sV1.StartDeploymentReq{
		TenantName: info.TenantName,
		Name:       info.DeploymentName,
		Replicas:   func() *int32 { r := info.GetReplicas(); return &r }(), // 恢复之前的副本数
	}
	if err := workflow.ExecuteActivity(ctx, a.StartInferenceInfra, startReq).Get(ctx, nil); err != nil {
		return err
	}

	// 3. Update Status -> RUNNING
	statusParam := activity.UpdateInferenceStatusInput{
		Id: req.GetId(), Status: inferenceV1.InferenceStatus_RUNNING, TimeUpdateType: "start",
	}
	if err := workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, statusParam).Get(ctx, nil); err != nil {
		return err
	}

	// 4. Restart Billing Monitor
	monitorID := fmt.Sprintf("monitor-inference-%d", req.GetId())
	childOpts := workflow.ChildWorkflowOptions{
		WorkflowID:        monitorID,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
		TaskQueue:         "inference-task-queue",
	}
	childCtx := workflow.WithChildOptions(ctx, childOpts)

	// Fire and forget
	workflow.ExecuteChildWorkflow(childCtx, InferenceBillingMonitorWorkflow, req.GetId(), info.TenantName, info.DeploymentName)

	return nil
}

// RestartInferenceWorkflow Restart 重启服务
func RestartInferenceWorkflow(ctx workflow.Context, req *inferenceV1.RestartInferenceRequest) error {
	ao := workflow.ActivityOptions{StartToCloseTimeout: time.Minute * 2}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.InferenceActivities

	var info *inferenceV1.Inference
	if err := workflow.ExecuteActivity(ctx, a.GetInferenceDBRecord, req.GetId()).Get(ctx, &info); err != nil {
		return err
	}

	rolloutReq := &k8sV1.RolloutDeploymentReq{TenantName: info.TenantName, Name: info.DeploymentName}
	if err := workflow.ExecuteActivity(ctx, a.RestartInferenceInfra, rolloutReq).Get(ctx, nil); err != nil {
		return err
	}

	// Update Status to ensure RUNNING
	statusParam := activity.UpdateInferenceStatusInput{
		Id: req.GetId(), Status: inferenceV1.InferenceStatus_RUNNING,
	}
	_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, statusParam).Get(ctx, nil)

	return nil
}

// DeleteInferenceWorkflow Delete 删除服务
func DeleteInferenceWorkflow(ctx workflow.Context, req *inferenceV1.DeleteInferenceRequest) error {
	ao := workflow.ActivityOptions{StartToCloseTimeout: time.Minute * 5}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.InferenceActivities

	// 1. Get Info & Update Status -> DELETING
	statusParam := activity.UpdateInferenceStatusInput{
		Id: req.GetId(), Status: inferenceV1.InferenceStatus_DELETING,
	}
	var info *inferenceV1.Inference
	if err := workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, statusParam).Get(ctx, &info); err != nil {
		return nil // Maybe already deleted
	}

	// 2. Cancel Monitor
	monitorID := fmt.Sprintf("monitor-inference-%d", req.GetId())
	_ = workflow.RequestCancelExternalWorkflow(ctx, monitorID, "").Get(ctx, nil)

	// 3. Finalize Billing & Mark STOPPED
	finishParam := activity.UpdateInferenceStatusInput{
		Id: req.GetId(), Status: inferenceV1.InferenceStatus_STOPPED, TimeUpdateType: "finish",
	}
	_ = workflow.ExecuteActivity(ctx, a.UpdateInferenceStatus, finishParam).Get(ctx, nil)
	_ = workflow.ExecuteActivity(ctx, a.FinalizeBilling, req.GetId()).Get(ctx, nil)

	// 4. K8s Delete
	_ = workflow.ExecuteActivity(ctx, a.CleanupInferenceInfra, info.TenantName, info.DeploymentName).Get(ctx, nil)

	// 5. DB Hard Delete
	return workflow.ExecuteActivity(ctx, a.HardDeleteInferenceDBRecord, req).Get(ctx, nil)
}
