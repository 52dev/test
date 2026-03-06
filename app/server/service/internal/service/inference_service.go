package service

import (
	"context"
	"fmt"
	inferenceV1 "go-wind-admin/api/gen/go/inference/service/v1"
	serverV1 "go-wind-admin/api/gen/go/server/service/v1"
	workflowDef "go-wind-admin/app/server/service/internal/workflow/workflow"

	"github.com/go-kratos/kratos/v2/log"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/emptypb"

	"go-wind-admin/app/server/service/internal/data"
)

type InferenceService struct {
	serverV1.InferenceServiceHTTPServer

	log *log.Helper

	repo *data.InferenceRepo

	temporalClient client.Client
}

func NewInferenceService(
	ctx *bootstrap.Context,
	repo *data.InferenceRepo,
	temporalClient client.Client,
) *InferenceService {
	return &InferenceService{
		log:            ctx.NewLoggerHelper("inference/service/server-service"),
		repo:           repo,
		temporalClient: temporalClient,
	}
}

func (s *InferenceService) List(ctx context.Context, req *paginationV1.PagingRequest) (*inferenceV1.ListInferenceResponse, error) {
	return s.repo.List(ctx, req)
}

func (s *InferenceService) Get(ctx context.Context, req *inferenceV1.GetInferenceRequest) (*inferenceV1.Inference, error) {
	return s.repo.Get(ctx, req)
}

func (s *InferenceService) Create(ctx context.Context, req *inferenceV1.CreateInferenceRequest) (*emptypb.Empty, error) {
	if req == nil || req.Data == nil {
		return nil, serverV1.ErrorBadRequest("invalid request")
	}

	workflowID := fmt.Sprintf("inference-create-%s-%s", req.Data.TenantName, req.Data.Name)

	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "inference-task-queue",
	}

	_, err := s.temporalClient.ExecuteWorkflow(ctx, workflowOptions, workflowDef.CreateInferenceWorkflow, req)
	if err != nil {
		s.log.Errorf("failed to trigger workflow: %v", err)
		return nil, serverV1.ErrorInternalServerError("failed to submit request")
	}
	return &emptypb.Empty{}, nil
}

func (s *InferenceService) Update(ctx context.Context, req *inferenceV1.UpdateInferenceRequest) (*emptypb.Empty, error) {
	if req == nil || req.Data == nil {
		return nil, serverV1.ErrorBadRequest("invalid request")
	}

	// 如果涉及副本数变更，应该走 Scale 接口，或者在这里判断逻辑
	// 这里仅更新 DB 元数据
	if _, err := s.repo.Update(ctx, req); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *InferenceService) Delete(ctx context.Context, req *inferenceV1.DeleteInferenceRequest) (*emptypb.Empty, error) {
	workflowID := fmt.Sprintf("inference-create-%d", req.GetId()) // 同上
	err := s.temporalClient.SignalWorkflow(ctx, workflowID, "", "SIGNAL_DELETE", nil)
	if err != nil {
		// 如果 Workflow 已经不存在（比如已经结束了），可能需要直接调 Repo 删 DB 兜底
		s.log.Warnf("Workflow not found, fallback to direct delete: %v", err)
		// s.repo.Delete(...)
	}
	return &emptypb.Empty{}, nil
}

func (s *InferenceService) Start(ctx context.Context, req *inferenceV1.StartInferenceRequest) (*emptypb.Empty, error) {
	workflowID := fmt.Sprintf("inference-start-%d", req.GetId())
	opts := client.StartWorkflowOptions{ID: workflowID, TaskQueue: "inference-task-queue"}

	_, err := s.temporalClient.ExecuteWorkflow(ctx, opts, workflowDef.StartInferenceWorkflow, req)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *InferenceService) Stop(ctx context.Context, req *inferenceV1.StopInferenceRequest) (*emptypb.Empty, error) {
	workflowID := fmt.Sprintf("inference-create-%d", req.GetId()) // 注意：ID 必须和 Create 时的一致
	// 如果你之前 Create 用的是 "inference-create-{tenant}-{name}"，这里必须拼出一样的 ID
	// 建议 ID 统一使用 "inference-lifecycle-{db_id}" 格式，方便查找

	// 发送信号
	err := s.temporalClient.SignalWorkflow(ctx, workflowID, "", "SIGNAL_STOP", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to signal stop: %w", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *InferenceService) Restart(ctx context.Context, req *inferenceV1.RestartInferenceRequest) (*emptypb.Empty, error) {
	workflowID := fmt.Sprintf("inference-create-%d", req.GetId()) // 同上，需保证 ID 一致
	err := s.temporalClient.SignalWorkflow(ctx, workflowID, "", "SIGNAL_START", nil)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *InferenceService) Scale(ctx context.Context, req *inferenceV1.ScaleInferenceRequest) (*emptypb.Empty, error) {
	workflowID := fmt.Sprintf("inference-create-%d", req.GetId())

	err := s.temporalClient.SignalWorkflow(ctx, workflowID, "", "SIGNAL_SCALE", req)
	return &emptypb.Empty{}, err
}
