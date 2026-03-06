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

// InferenceBillingMonitorWorkflow Billing Monitor (核心计费逻辑)
func InferenceBillingMonitorWorkflow(ctx workflow.Context, id uint64, tenantName, deployName string) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 10, // K8s 查询很快
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.InferenceActivities

	logger := workflow.GetLogger(ctx)
	logger.Info("Billing Monitor Started", "id", id)

	// 轮询间隔 (例如 30s)
	interval := time.Second * 30

	for {
		// 1. 查询当前 Replicas (K8s 真实值)
		var currentReplicas int32
		err := workflow.ExecuteActivity(ctx, a.GetK8sReplicas, tenantName, deployName).Get(ctx, &currentReplicas)

		if err == nil && currentReplicas > 0 {
			// 2. 计算消耗 (Replica * Seconds)
			// 这里简单按 interval 计算
			usage := int64(currentReplicas) * int64(interval.Seconds())

			// 3. 累加到 DB
			_ = workflow.ExecuteActivity(ctx, a.AccumulateUsage, id, usage).Get(ctx, nil)
		} else {
			// 如果查不到或者 Replicas=0，可能服务已停，或者 K8s 短暂故障
			logger.Warn("Failed to get replicas or zero replicas", "err", err)
		}

		// 4. Sleep 等待下一轮
		// 监听 Cancel 信号 (Stop/Delete 时会 Cancel 此 Workflow)
		if err := workflow.Sleep(ctx, interval); err != nil {
			logger.Info("Monitor Cancelled")
			return nil
		}
	}
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
