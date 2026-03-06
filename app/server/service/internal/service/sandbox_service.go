package service

import (
	"context"
	"fmt"
	"go-wind-admin/app/server/service/internal/biz"
	"go-wind-admin/app/server/service/internal/data"
	workflowDef "go-wind-admin/app/server/service/internal/workflow/workflow"

	sandboxV1 "go-wind-admin/api/gen/go/sandbox/service/v1"
	serverV1 "go-wind-admin/api/gen/go/server/service/v1"

	"github.com/go-kratos/kratos/v2/log"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/emptypb"
)

type SandboxService struct {
	serverV1.SandboxServiceHTTPServer

	log *log.Helper

	repo *data.SandboxRepo

	modelRepo *data.ModelRepo

	kueueController *biz.KueueController

	appController *biz.AppController

	temporalClient client.Client
}

func NewSandboxService(
	ctx *bootstrap.Context,
	repo *data.SandboxRepo,
	modelRepo *data.ModelRepo,
	kueueCtrl *biz.KueueController,
	appController *biz.AppController,
	temporalClient client.Client,
) *SandboxService {
	return &SandboxService{
		log:             ctx.NewLoggerHelper("sandbox/service/server-service"),
		repo:            repo,
		modelRepo:       modelRepo,
		kueueController: kueueCtrl,
		appController:   appController,
		temporalClient:  temporalClient,
	}
}

func (s *SandboxService) List(ctx context.Context, req *paginationV1.PagingRequest) (*sandboxV1.ListSandboxResponse, error) {
	return s.repo.List(ctx, req)
}

func (s *SandboxService) Get(ctx context.Context, req *sandboxV1.GetSandboxRequest) (*sandboxV1.Sandbox, error) {
	return s.repo.Get(ctx, req)
}

func (s *SandboxService) Create(ctx context.Context, req *sandboxV1.CreateSandboxRequest) (dto *emptypb.Empty, err error) {
	if req == nil || req.Data == nil {
		return nil, serverV1.ErrorBadRequest("invalid request")
	}
	// 1. 业务 ID 生成 (用于幂等性)
	// 如果前端没传 UUID，这里生成一个，作为 Workflow ID
	// 建议在 Request Data 里加一个 RequestId 字段
	workflowID := fmt.Sprintf("sandbox-create-%s-%s", req.Data.TenantName, req.Data.Name)

	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "sandbox-task-queue",
	}

	// 2. 异步触发
	// 这里 req 直接传进去，DB 记录会在 Worker 里创建
	_, err = s.temporalClient.ExecuteWorkflow(ctx, workflowOptions, workflowDef.CreateSandboxWorkflow, req)
	if err != nil {
		s.log.Errorf("failed to trigger workflow: %v", err)
		return nil, serverV1.ErrorInternalServerError("failed to submit request")
	}
	return &emptypb.Empty{}, nil
}

func (s *SandboxService) Update(ctx context.Context, req *sandboxV1.UpdateSandboxRequest) (*emptypb.Empty, error) {
	if req == nil || req.Data == nil {
		return nil, serverV1.ErrorBadRequest("invalid request")
	}

	if req.UpdateMask != nil {
		req.UpdateMask.Paths = append(req.UpdateMask.Paths, "updated_by")
	}

	if _, err := s.repo.Update(ctx, req); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *SandboxService) Delete(ctx context.Context, req *sandboxV1.DeleteSandboxRequest) (*emptypb.Empty, error) {

	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid request")
	}
	_, err := s.repo.Get(ctx, &sandboxV1.GetSandboxRequest{QueryBy: &sandboxV1.GetSandboxRequest_Id{Id: req.GetId()}})
	if err != nil {
		return nil, serverV1.ErrorNotFound("sandbox not found")
	}
	// 2. 触发删除 Workflow
	workflowOptions := client.StartWorkflowOptions{
		ID:        fmt.Sprintf("sandbox-delete-%d", req.Id),
		TaskQueue: "sandbox-task-queue",
	}
	_, err = s.temporalClient.ExecuteWorkflow(ctx, workflowOptions, workflowDef.DeleteSandboxWorkflow, req)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *SandboxService) Restart(ctx context.Context, req *sandboxV1.RestartSandboxRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid request")
	}
	sandbox, err := s.repo.Get(ctx, &sandboxV1.GetSandboxRequest{QueryBy: &sandboxV1.GetSandboxRequest_Id{Id: req.GetId()}})
	if err != nil {
		return nil, serverV1.ErrorNotFound("sandbox not found")
	}
	workflowID := fmt.Sprintf("sandbox-restart-%s-%s", sandbox.TenantName, sandbox.Name)

	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "sandbox-task-queue",
	}
	// 2. 异步触发
	// 这里 req 直接传进去，DB 记录会在 Worker 里创建
	_, err = s.temporalClient.ExecuteWorkflow(ctx, workflowOptions, workflowDef.RestartSandboxWorkflow, req)
	if err != nil {
		s.log.Errorf("failed to trigger workflow: %v", err)
		return nil, serverV1.ErrorInternalServerError("failed to submit request")
	}
	return &emptypb.Empty{}, nil
}

func (s *SandboxService) Start(ctx context.Context, req *sandboxV1.StartSandboxRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid request")
	}
	sandbox, err := s.repo.Get(ctx, &sandboxV1.GetSandboxRequest{QueryBy: &sandboxV1.GetSandboxRequest_Id{Id: req.GetId()}})
	if err != nil {
		return nil, serverV1.ErrorNotFound("sandbox not found")
	}
	workflowID := fmt.Sprintf("sandbox-start-%s-%s", sandbox.TenantName, sandbox.Name)

	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "sandbox-task-queue",
	}
	// 2. 异步触发
	// 这里 req 直接传进去，DB 记录会在 Worker 里创建
	_, err = s.temporalClient.ExecuteWorkflow(ctx, workflowOptions, workflowDef.StartSandboxWorkflow, req)
	if err != nil {
		s.log.Errorf("failed to trigger workflow: %v", err)
		return nil, serverV1.ErrorInternalServerError("failed to submit request")
	}
	return &emptypb.Empty{}, nil
}

func (s *SandboxService) Stop(ctx context.Context, req *sandboxV1.StopSandboxRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid request")
	}
	sandbox, err := s.repo.Get(ctx, &sandboxV1.GetSandboxRequest{QueryBy: &sandboxV1.GetSandboxRequest_Id{Id: req.GetId()}})
	if err != nil {
		return nil, serverV1.ErrorNotFound("sandbox not found")
	}
	workflowID := fmt.Sprintf("sandbox-stop-%s-%s", sandbox.TenantName, sandbox.Name)

	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "sandbox-task-queue",
	}
	// 2. 异步触发
	// 这里 req 直接传进去，DB 记录会在 Worker 里创建
	_, err = s.temporalClient.ExecuteWorkflow(ctx, workflowOptions, workflowDef.StopSandboxWorkflow, req)
	if err != nil {
		s.log.Errorf("failed to trigger workflow: %v", err)
		return nil, serverV1.ErrorInternalServerError("failed to submit request")
	}
	return &emptypb.Empty{}, nil
}

func (s *SandboxService) GetPassword(ctx context.Context, req *sandboxV1.GetSandboxPasswordRequest) (*sandboxV1.GetSandboxPasswordResponse, error) {

	password, err := s.repo.GetPassword(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &sandboxV1.GetSandboxPasswordResponse{
		Password: password,
	}, nil
}
