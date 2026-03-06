package workflow

import (
	"fmt"
	k8sV1 "go-wind-admin/api/gen/go/k8s/service/v1"
	workflowV1 "go-wind-admin/api/gen/go/workflow/service/v1"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	sandboxV1 "go-wind-admin/api/gen/go/sandbox/service/v1"
	"go-wind-admin/app/server/service/internal/workflow/activity" // 引用上面的 Activity 包

	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// CreateSandboxWorkflow 创建资源 -> 启动定时器 -> 结束
func CreateSandboxWorkflow(ctx workflow.Context, params *sandboxV1.CreateSandboxRequest) error {
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
	var a *activity.SandboxActivities

	// 1. 准备 & 检查
	prepareInput := &workflowV1.CheckAndPrepareRequest{DbRecord: params}
	prepareOutput := &workflowV1.CheckAndPrepareResponse{}
	if err := workflow.ExecuteActivity(ctx, a.CheckAndPrepare, prepareInput).Get(ctx, prepareOutput); err != nil {
		return err
	}

	// 2. 创建 DB 记录 (Creating)
	var sandboxRecord *sandboxV1.Sandbox
	if err := workflow.ExecuteActivity(ctx, a.CreateSandboxDBRecord, prepareOutput.DbRecord).Get(ctx, &sandboxRecord); err != nil {
		return err
	}

	// 3. 创建 K8s 资源
	var domainUrl string
	if err := workflow.ExecuteActivity(ctx, a.CreateInfrastructure, prepareOutput.Infrastructure).Get(ctx, &domainUrl); err != nil {
		// 回滚逻辑
		disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
		_ = workflow.ExecuteActivity(disconnectedCtx, a.CleanupInfrastructure, params.Data.TenantName, prepareOutput.Infrastructure.Name).Get(disconnectedCtx, nil)

		failParam := activity.UpdateStatusInput{
			Id: sandboxRecord.GetId(), Status: sandboxV1.Sandbox_ERROR, Reason: err.Error(),
		}
		_ = workflow.ExecuteActivity(disconnectedCtx, a.UpdateSandboxStatus, failParam)
		return err
	}

	// 4. 更新 DB 状态 -> RUNNING 并记录 StartedAt
	// 乐观认为 K8s 创建成功即开始计费 (或者你可以等待 Admitted 信号)
	sandboxRecord.Status = func() *sandboxV1.Sandbox_Status { s := sandboxV1.Sandbox_RUNNING; return &s }()
	sandboxRecord.Url = &domainUrl

	// 使用 UpdateSandbox 也可以，但最好用我们新加的 UpdateSandboxStatus 记录时间
	updateRunningParams := activity.UpdateStatusInput{
		Id:             sandboxRecord.GetId(),
		Status:         sandboxV1.Sandbox_RUNNING,
		TimeUpdateType: "start", // 关键：记录 started_at
	}
	if err := workflow.ExecuteActivity(ctx, a.UpdateSandboxStatus, updateRunningParams).Get(ctx, nil); err != nil {
		return err
	}

	// 补全 URL 信息
	updateUrlParams := &sandboxV1.UpdateSandboxRequest{
		Id: sandboxRecord.GetId(), Data: sandboxRecord,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"url"}},
	}
	_ = workflow.ExecuteActivity(ctx, a.UpdateSandbox, updateUrlParams).Get(ctx, nil)

	// 5. 【关键】启动自动关机定时器 (Fire-and-Forget)
	if params.Data.GetExpirationMinutes() > 0 {
		totalSeconds := params.Data.GetExpirationMinutes() * 60
		timerWorkflowID := fmt.Sprintf("timer-sandbox-%d", sandboxRecord.GetId())
		childOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        timerWorkflowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON, // 父流程结束，子流程继续跑
			TaskQueue:         "sandbox-task-queue",
		}
		childCtx := workflow.WithChildOptions(ctx, childOpts)

		// 异步启动 SandboxTimerWorkflow
		childFuture := workflow.ExecuteChildWorkflow(childCtx, SandboxTimerWorkflow, sandboxRecord.GetId(), totalSeconds)

		// 2. 【新增】等待子流程“确认启动” (Wait for Execution Started)
		// 这一步会阻塞直到 Server 确认子流程已创建（分配了 RunID）
		// 如果参数类型不对、或者 Server 拒绝创建，这里会立刻报错
		var childExecution workflow.Execution
		if err := childFuture.GetChildWorkflowExecution().Get(childCtx, &childExecution); err != nil {
			workflow.GetLogger(ctx).Error("Failed to start timer workflow", "error", err)
			// 即使这里失败，主流程可能也算成功，但至少你能看到日志
		} else {
			workflow.GetLogger(ctx).Info("Timer workflow started successfully", "WorkflowID", childExecution.ID, "RunID", childExecution.RunID)
		}
	}

	return nil
}

// SandboxTimerWorkflow 纯净的倒计时闹钟
func SandboxTimerWorkflow(ctx workflow.Context, sandboxId uint64, timeoutSeconds int64) error {
	// 1. 睡觉
	workflow.GetLogger(ctx).Info("TIMER STARTED!!!")
	duration := time.Duration(timeoutSeconds) * time.Second
	// 增加一个极小的缓冲检查，防止 duration <= 0 导致 Sleep 立即返回（虽然逻辑上不应该发生，防御性）
	if duration <= 0 {
		workflow.GetLogger(ctx).Warn("Timer duration is 0 or negative, treating as expired immediately")
	} else {
		if err := workflow.Sleep(ctx, duration); err != nil {
			// 如果被 Cancel 了 (用户手动停止)，这里会收到 CancelError
			// 直接退出即可，不需要做删除动作
			workflow.GetLogger(ctx).Info("Timer cancelled", "error", err)
			return nil
		}
	}

	// 2. 醒来 -> 触发删除
	workflow.GetLogger(ctx).Info("Timer expired, triggering deletion", "sandboxId", sandboxId)

	deleteReq := &sandboxV1.DeleteSandboxRequest{Id: sandboxId}

	childOpts := workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("auto-delete-%d", sandboxId), // 避免 ID 冲突
		TaskQueue:  "sandbox-task-queue",                     // 显式指定队列
	}
	childCtx := workflow.WithChildOptions(ctx, childOpts)

	err := workflow.ExecuteChildWorkflow(childCtx, DeleteSandboxWorkflow, deleteReq).Get(childCtx, nil)
	return err
}

// DeleteSandboxWorkflow 停止 -> 结算 -> 清理
func DeleteSandboxWorkflow(ctx workflow.Context, req *sandboxV1.DeleteSandboxRequest) error {
	ao := workflow.ActivityOptions{StartToCloseTimeout: time.Minute * 5}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.SandboxActivities

	// 1. 标记为 DELETING (防止重复操作)
	// 同时获取详情以便后续清理
	// 注意：这里我们先不 Hard Delete，而是先 Update 状态
	// 因为我们需要 TenantName 和 DeploymentName
	// 更好的做法是 GetSandboxActivity，这里复用 UpdateStatus
	// 假设 UpdateStatus 会返回 Sandbox 对象

	// 先 Get 数据
	// (这里假设 activity.CheckAndPrepare 里的 GetModelInfo 逻辑不适用)
	// 我们需要一个新的 Activity: GetSandbox
	// 为了不改太多代码，这里用 UpdateSandboxStatus 先拿回对象

	updateParams := activity.UpdateStatusInput{
		Id: req.GetId(), Status: sandboxV1.Sandbox_DELETING,
	}
	var sandbox *sandboxV1.Sandbox
	err := workflow.ExecuteActivity(ctx, a.UpdateSandboxStatus, updateParams).Get(ctx, &sandbox)
	if err != nil {
		// 如果记录没了，说明已删，幂等返回
		return nil
	}

	// 2. 标记结束时间 (STOPPED) & 结算计费
	// 虽然前面标记了 Deleting，但为了计费，我们需要一个明确的 FinishedAt
	// 这里我们再更新一次 Stopped 并记录 Time
	finishParams := activity.UpdateStatusInput{
		Id: req.GetId(), Status: sandboxV1.Sandbox_STOPPED, TimeUpdateType: "finish",
	}
	_ = workflow.ExecuteActivity(ctx, a.UpdateSandboxStatus, finishParams).Get(ctx, nil)

	// 3. 计算时长并保存 (Billing)
	err = workflow.ExecuteActivity(ctx, a.FinalizeBilling, req.GetId()).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("Billing calculation failed", "error", err)
		// 计费失败是否继续删除？建议继续，否则资源泄露
	}

	// 4. 清理 K8s 资源
	err = workflow.ExecuteActivity(ctx, a.CleanupInfrastructure, sandbox.TenantName, sandbox.DeploymentName).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("K8s cleanup failed", "error", err)
	}

	// 5. 物理删除 DB 记录 (可选)
	// 如果你要保留计费记录，这里就不要 HardDelete，或者状态保留为 STOPPED/DELETED 即可
	// 根据你的代码，你最后执行了 HardDelete

	err = workflow.ExecuteActivity(ctx, a.HardDeleteSandboxDBRecord, req).Get(ctx, nil)
	return err
}

// StartSandboxWorkflow 手动启动（Stop 后再次 Start）
func StartSandboxWorkflow(ctx workflow.Context, req *sandboxV1.StartSandboxRequest) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 2,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.SandboxActivities

	// ---------------------------------------------------------------
	// 1. 获取 Sandbox 信息 (包含 ExpirationMinutes 和 BillingDurationSec)
	// ---------------------------------------------------------------
	// 我们需要一个新的 Activity 专门用来 Check 状态和余额，而不是由 UpdateStatus 副作用来做
	// 但为了保持你现有结构，我们先 Get 再 Update

	// 假设 UpdateSandboxStatus 返回完整对象
	updateParam := activity.UpdateStatusInput{
		Id:             req.GetId(),
		Status:         sandboxV1.Sandbox_RUNNING, // 乐观更新
		TimeUpdateType: "start",                   // 标记开始时间(用于下一次结算)
	}
	var sandbox *sandboxV1.Sandbox
	err := workflow.ExecuteActivity(ctx, a.UpdateSandboxStatus, updateParam).Get(ctx, &sandbox)
	if err != nil {
		return err
	}

	// ---------------------------------------------------------------
	// 2. 【时间补偿核心逻辑】计算剩余时长
	// ---------------------------------------------------------------
	var remainingSeconds int64 = 0

	// 只有设置了过期时间才需要计算
	if sandbox.ExpirationMinutes > 0 {
		totalLimitSec := sandbox.ExpirationMinutes * 60
		usedSec := sandbox.GetBillingDurationSec() // 也就是 DB 里的累积时长

		remainingSeconds = totalLimitSec - usedSec

		if remainingSeconds <= 0 {
			// A. 时间已耗尽，拒绝启动
			// 回滚状态为 STOPPED
			failParam := activity.UpdateStatusInput{
				Id: req.GetId(), Status: sandboxV1.Sandbox_STOPPED, Reason: "quota_expired",
			}
			_ = workflow.ExecuteActivity(ctx, a.UpdateSandboxStatus, failParam).Get(ctx, nil)
			return fmt.Errorf("sandbox quota expired (total: %dm, used: %ds)", sandbox.ExpirationMinutes, usedSec)
		}

		// 这里不再进行分钟取整，而是直接使用剩余秒数
		workflow.GetLogger(ctx).Info("Calculated remaining time", "seconds", remainingSeconds)
	}

	// ---------------------------------------------------------------
	// 3. 执行 K8s Start
	// ---------------------------------------------------------------
	param := &k8sV1.StartDeploymentReq{
		TenantName: sandbox.TenantName,
		Name:       sandbox.DeploymentName,
		Replicas:   func() *int32 { r := int32(1); return &r }(),
	}
	err = workflow.ExecuteActivity(ctx, a.StartInfrastructure, param).Get(ctx, nil)
	if err != nil {
		// 失败回滚
		failParam := activity.UpdateStatusInput{
			Id:             req.GetId(),
			Status:         sandboxV1.Sandbox_STOPPED,
			Reason:         err.Error(),
			TimeUpdateType: "finish", // 标记结束
		}
		_ = workflow.ExecuteActivity(ctx, a.UpdateSandboxStatus, failParam).Get(ctx, nil)
		// 注意：启动失败通常不扣减时长，因为 Duration 计算依赖 StartedAt 和 FinishedAt 的差值
		// 如果 StartInfrastructure 失败很快，时长几乎为 0
		return err
	}

	// ---------------------------------------------------------------
	// 4. 启动定时器 (使用剩余时长)
	// ---------------------------------------------------------------
	if remainingSeconds > 0 {
		timerWorkflowID := fmt.Sprintf("timer-sandbox-%d", req.GetId())

		// Cancel 旧的
		_ = workflow.RequestCancelExternalWorkflow(ctx, timerWorkflowID, "").Get(ctx, nil)

		childOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        timerWorkflowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
		}
		childCtx := workflow.WithChildOptions(ctx, childOpts)

		// 传入剩余时长！
		childFuture := workflow.ExecuteChildWorkflow(childCtx, SandboxTimerWorkflow, req.GetId(), remainingSeconds)
		// 2. 【新增】等待子流程“确认启动” (Wait for Execution Started)
		// 这一步会阻塞直到 Server 确认子流程已创建（分配了 RunID）
		// 如果参数类型不对、或者 Server 拒绝创建，这里会立刻报错
		var childExecution workflow.Execution
		if err := childFuture.GetChildWorkflowExecution().Get(childCtx, &childExecution); err != nil {
			workflow.GetLogger(ctx).Error("Failed to start timer workflow", "error", err)
			// 即使这里失败，主流程可能也算成功，但至少你能看到日志
		} else {
			workflow.GetLogger(ctx).Info("Timer workflow started successfully", "WorkflowID", childExecution.ID, "RunID", childExecution.RunID)
		}
	}

	return nil
}

// StopSandboxWorkflow 手动停止
func StopSandboxWorkflow(ctx workflow.Context, req *sandboxV1.StopSandboxRequest) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 2,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.SandboxActivities

	// 1. 尝试取消自动关机定时器 (核心!)
	// 无论是否存在，都尝试 Cancel
	timerWorkflowID := fmt.Sprintf("timer-sandbox-%d", req.GetId())

	// 直接请求取消外部 Workflow
	// 参数: ctx, workflowID, runID (传""表示最新)
	cancelFuture := workflow.RequestCancelExternalWorkflow(ctx, timerWorkflowID, "")

	// 等待取消请求发送完成 (非阻塞等待对方真正取消，只等待请求发出去)
	// 忽略错误：如果定时器不存在(已过期)或已取消，这里会报错，但这不影响停止流程
	_ = cancelFuture.Get(ctx, nil)

	// 2. 执行删除/停止流程
	// 可以直接复用 DeleteSandboxWorkflow，或者保留 Stop 只做 K8s Stop 但不删数据
	// 根据你的原代码，Stop 只是 K8s StopDeployment，不删数据

	// 原逻辑：
	updateParam := activity.UpdateStatusInput{
		Id: req.GetId(), Status: sandboxV1.Sandbox_STOPPED, TimeUpdateType: "finish", // 停止也算结束计费
	}
	var sandbox *sandboxV1.Sandbox
	if err := workflow.ExecuteActivity(ctx, a.UpdateSandboxStatus, updateParam).Get(ctx, &sandbox); err != nil {
		return err
	}

	// 结算计费
	_ = workflow.ExecuteActivity(ctx, a.FinalizeBilling, req.GetId()).Get(ctx, nil)

	// K8s Stop
	param := &k8sV1.StopDeploymentReq{TenantName: sandbox.TenantName, Name: sandbox.DeploymentName}
	if err := workflow.ExecuteActivity(ctx, a.StopInfrastructure, param).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("Failed to stop K8s deployment", "error", err)
		// 即使 K8s 失败，DB 状态已经更新为 Stopped，计费已停止，这是符合预期的
		return err
	}
	return nil
}

// RestartSandboxWorkflow 重启（Stop 后 Start）
func RestartSandboxWorkflow(ctx workflow.Context, req *sandboxV1.RestartSandboxRequest) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 2,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var a *activity.SandboxActivities

	// Restart 不改变 DB 状态，或者改为 RESTARTING (如果定义了)
	// 这里我们需要先获取 Sandbox 信息
	// 由于 UpdateSandboxStatus 承担了 Get 的职责，我们还是调用它，但状态保持 RUNNING
	updateParam := &activity.UpdateStatusInput{
		Id:     req.GetId(),
		Status: sandboxV1.Sandbox_RUNNING, // 保持 RUNNING
	}
	var sandbox *sandboxV1.Sandbox
	err := workflow.ExecuteActivity(ctx, a.UpdateSandboxStatus, updateParam).Get(ctx, &sandbox)
	if err != nil {
		return err
	}

	param := &k8sV1.RolloutDeploymentReq{
		TenantName: sandbox.TenantName,
		Name:       sandbox.DeploymentName,
	}

	err = workflow.ExecuteActivity(ctx, a.RestartInfrastructure, param).Get(ctx, nil)

	if err != nil {
		// 失败回滚状态 (比如回滚到 STOPPED 或者 ERROR)
		failParam := activity.UpdateStatusInput{
			Id:     req.GetId(),
			Status: sandboxV1.Sandbox_ERROR, // 或者 STOPPED
			Reason: err.Error(),
		}
		_ = workflow.ExecuteActivity(ctx, a.UpdateSandboxStatus, failParam).Get(ctx, nil)
	} else {
		// 成功的话，确保状态是 RUNNING
		successParam := activity.UpdateStatusInput{
			Id:     req.GetId(),
			Status: sandboxV1.Sandbox_RUNNING,
		}
		_ = workflow.ExecuteActivity(ctx, a.UpdateSandboxStatus, successParam).Get(ctx, nil)
	}

	info := workflow.GetInfo(ctx)
	workflowID := info.WorkflowExecution.ID
	runID := info.WorkflowExecution.RunID
	fmt.Println(workflowID, runID)
	return err
}
