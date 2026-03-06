package service

import (
	"context"
	modelV1 "go-wind-admin/api/gen/go/model/service/v1"

	"github.com/go-kratos/kratos/v2/log"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/emptypb"

	"go-wind-admin/app/server/service/internal/data"

	serverV1 "go-wind-admin/api/gen/go/server/service/v1"
)

type ModelService struct {
	serverV1.ModelServiceHTTPServer
	//serverV1.UnimplementedModelServiceServer
	modelV1.UnimplementedModelServiceServer
	modelRegistrySvc *ModelRegistryService
	log              *log.Helper

	repo *data.ModelRepo
}

func NewModelService(ctx *bootstrap.Context, repo *data.ModelRepo, modelRegistrySvc *ModelRegistryService) *ModelService {
	return &ModelService{
		log:              ctx.NewLoggerHelper("model/service/server-service"),
		repo:             repo,
		modelRegistrySvc: modelRegistrySvc,
	}
}

func (s *ModelService) List(ctx context.Context, req *paginationV1.PagingRequest) (*modelV1.ListModelResponse, error) {
	return s.repo.List(ctx, req)
}

func (s *ModelService) Get(ctx context.Context, req *modelV1.GetModelRequest) (*modelV1.Model, error) {
	return s.repo.Get(ctx, req)
}

func (s *ModelService) Create(ctx context.Context, req *modelV1.CreateModelRequest) (dto *modelV1.Model, err error) {
	if req == nil || req.Data == nil {
		return nil, serverV1.ErrorBadRequest("invalid request")
	}
	result, err := s.repo.Create(ctx, req, s.modelRegistrySvc.Create)
	if err != nil {

		return nil, err
	}
	if dto, err = s.repo.UpdateStatus(ctx, result, modelV1.Model_READY); err != nil {
		s.log.Errorf("CRITICAL: model created (id=%d) but failed to update status to READY: %v", result.GetId(), err)
	}
	return dto, nil
}

func (s *ModelService) Update(ctx context.Context, req *modelV1.UpdateModelRequest) (*emptypb.Empty, error) {
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

func (s *ModelService) Delete(ctx context.Context, req *modelV1.DeleteModelRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid request")
	}
	model, err := s.repo.Get(ctx, &modelV1.GetModelRequest{
		Id: req.GetId(),
	})
	if err != nil {
		return nil, err
	}
	_, err = s.modelRegistrySvc.Delete(ctx, model)
	if err != nil {
		s.log.Errorf("CRITICAL: model deleted (id=%d) but failed to delete from registry: %v", model.GetId(), err)
	}
	if err := s.repo.Delete(ctx, req); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *ModelService) Exists(ctx context.Context, req *modelV1.ModelExistsRequest) (*modelV1.ModelExistsResponse, error) {
	return s.repo.Exists(ctx, req)
}

func (s *ModelService) ListTags(ctx context.Context, req *modelV1.ListTagsRequest) (*modelV1.ListTagsResponse, error) {
	model, err := s.repo.Get(ctx, &modelV1.GetModelRequest{
		Id: req.GetId(),
	})
	if err != nil {
		return nil, err
	}
	return s.modelRegistrySvc.ListTags(ctx, model)
}

func (s *ModelService) ListObjects(ctx context.Context, req *modelV1.ListObjectsRequest) (*modelV1.ListObjectsResponse, error) {
	model, err := s.repo.Get(ctx, &modelV1.GetModelRequest{
		Id: req.GetId(),
	})
	if err != nil {
		return nil, err
	}
	return s.modelRegistrySvc.ListObjects(ctx, model.GetUuid(), req.Branch, req.Params)
}
