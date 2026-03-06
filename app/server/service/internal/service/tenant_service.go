package service

import (
	"context"
	"fmt"
	"strings"

	pb "go-wind-admin/api/gen/go/tenant/service/v1"
	"go-wind-admin/app/server/service/internal/biz"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/emptypb"
)

type TenantService struct {
	pb.UnimplementedTenantServiceServer
	controller *biz.KueueController
	log        *log.Helper
}

func NewTenantService(ctx *bootstrap.Context, ctrl *biz.KueueController) *TenantService {
	return &TenantService{
		controller: ctrl,
		log:        ctx.NewLoggerHelper("server/service/tenant-service"),
	}
}

// --- Global Flavors ---

func (s *TenantService) CreateFlavor(ctx context.Context, req *pb.CreateFlavorReq) (*emptypb.Empty, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("flavor name required")
	}
	err := s.controller.CreateFlavor(ctx, req)
	if err != nil {
		s.log.Errorf("CreateFlavor failed: %v", err)
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *TenantService) ListFlavors(ctx context.Context, _ *emptypb.Empty) (*pb.ListFlavorsResp, error) {
	list, err := s.controller.ListFlavors(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.ListFlavorsResp{Flavors: list}, nil
}

// --- Tenant Lifecycle ---

func (s *TenantService) RegisterTenant(ctx context.Context, req *pb.RegisterTenantReq) (*pb.TenantDetailResp, error) {
	if req.DisplayName == "" {
		return nil, fmt.Errorf("display name required")
	}

	// 1. 系统生成唯一租户 ID (Namespace)
	tenantID := generateTenantID()
	s.log.Infof("Registering tenant: %s (%s)", req.DisplayName, tenantID)

	// 2. 初始化环境
	err := s.controller.CreateTenantEnv(ctx, tenantID, req.DisplayName, req.Quota)
	if err != nil {
		s.log.Errorf("CreateTenantEnv failed: %v", err)
		return nil, err
	}

	// 3. 返回详情
	return s.controller.GetTenantDetail(ctx, tenantID)
}

func (s *TenantService) DeleteTenant(ctx context.Context, req *pb.DeleteTenantReq) (*emptypb.Empty, error) {
	if req.TenantName == "" {
		return nil, fmt.Errorf("tenant_name required")
	}
	err := s.controller.DeleteTenantEnv(ctx, req.TenantName)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *TenantService) UpdateQuota(ctx context.Context, req *pb.UpdateTenantQuotaReq) (*pb.TenantDetailResp, error) {
	if req.TenantName == "" {
		return nil, fmt.Errorf("tenant_name required")
	}
	// 更新配额不修改 DisplayName
	err := s.controller.CreateTenantEnv(ctx, req.TenantName, "", req.Quota)
	if err != nil {
		return nil, err
	}
	return s.controller.GetTenantDetail(ctx, req.TenantName)
}

func (s *TenantService) GetTenant(ctx context.Context, req *pb.GetTenantReq) (*pb.TenantDetailResp, error) {
	if req.TenantName == "" {
		return nil, fmt.Errorf("tenant_name required")
	}
	return s.controller.GetTenantDetail(ctx, req.TenantName)
}

// --- Quota Pre-Check (API Layer) ---

func (s *TenantService) CheckQuota(ctx context.Context, req *pb.CheckQuotaReq) (*pb.CheckQuotaResp, error) {
	if req.TenantName == "" {
		return nil, fmt.Errorf("tenant_name required")
	}

	// 默认值处理
	if req.Quota.Cpu == "" {
		req.Quota.Cpu = "0"
	}
	if req.Quota.Memory == "" {
		req.Quota.Memory = "0"
	}
	if req.Quota.Storage == "" {
		req.Quota.Storage = "0"
	}
	if req.Quota.Gpu == "" {
		req.Quota.Gpu = "0"
	}

	allowed, reason, err := s.controller.CheckQuotaSufficient(ctx, req)
	if err != nil {
		// 系统错误
		return nil, err
	}

	return &pb.CheckQuotaResp{
		Allowed: allowed,
		Reason:  reason,
	}, nil
}

// Helper: tenant-xxxxxx
func generateTenantID() string {
	u := strings.ReplaceAll(uuid.New().String(), "-", "")
	return fmt.Sprintf("tenant-%s", u[:8])
}
