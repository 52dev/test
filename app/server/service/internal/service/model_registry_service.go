package service

import (
	"context"
	"encoding/json"
	"fmt"
	serverV1 "go-wind-admin/api/gen/go/server/service/v1"
	"go-wind-admin/pkg/oss"
	"net/http"
	"time"

	conf "go-wind-admin/api/gen/go/conf/v1"
	modelV1 "go-wind-admin/api/gen/go/model/service/v1"
	"go-wind-admin/pkg/lakefs"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ModelRegistryService struct {
	lakefsClient *lakefs.ClientWithResponses
	ossClient    *oss.MinIOClient
	c            *conf.Bootstrap
	log          *log.Helper
	svc          *ModelService
}

func NewRegistryService(
	ctx *bootstrap.Context,
	lakefsClient *lakefs.ClientWithResponses,
	ossClient *oss.MinIOClient,
	c *conf.Bootstrap,
) *ModelRegistryService {
	return &ModelRegistryService{
		log:          ctx.NewLoggerHelper("model-registry/service/server-service"),
		lakefsClient: lakefsClient,
		c:            c,
		ossClient:    ossClient,
	}
}

func (s *ModelRegistryService) Get(ctx context.Context, mid string) (*modelV1.Model, error) {

	resp, err := s.lakefsClient.GetRepositoryWithResponse(ctx, mid)
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		if resp.StatusCode() == http.StatusNotFound {
			return nil, fmt.Errorf("model not found")
		}
		return nil, fmt.Errorf("failed to get repository: %s", resp.Status())
	}

	return s.convertRepoToModel(*resp.JSON200), nil
}

func (s *ModelRegistryService) GetStorageNamespace(req *modelV1.Model) string {
	storageNamespace := fmt.Sprintf("s3://%s/%s/%s/base", s.c.Lakefs.BucketName, req.GetSlug(), req.GetUuid())
	return storageNamespace
}

func (s *ModelRegistryService) Create(ctx context.Context, req *modelV1.Model) (*emptypb.Empty, error) {
	storageNamespace := s.GetStorageNamespace(req)
	fmt.Println(storageNamespace, req.GetUuid())
	resp, err := s.lakefsClient.CreateRepositoryWithResponse(ctx, &lakefs.CreateRepositoryParams{}, lakefs.RepositoryCreation{
		Name:             req.GetUuid(),
		StorageNamespace: storageNamespace,
		DefaultBranch:    s.c.Lakefs.DefaultBranch,
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, s.handleRepositoryResponse(resp.HTTPResponse, "create repository")
}

func (s *ModelRegistryService) handleRepositoryResponse(resp *http.Response, msg string) error {
	res := map[int]func(string, ...interface{}) *errors.Error{
		http.StatusBadRequest:          serverV1.ErrorBadRequest,
		http.StatusConflict:            serverV1.ErrorConflict,
		http.StatusInternalServerError: serverV1.ErrorExpectationFailed,
		http.StatusNotFound:            serverV1.ErrorNotFound,
	}
	if res[resp.StatusCode] != nil {
		var errResp lakefs.Error
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return serverV1.ErrorExpectationFailed("failed to decode error response: %w", err)
		}
		return res[resp.StatusCode](fmt.Sprintf("failed to %s: %s", msg, errResp.Message))
	}
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected response status: %d", resp.StatusCode)
	}
	return nil
}

func (s *ModelRegistryService) ListObjects(ctx context.Context, repo string, branch string, req *modelV1.ListObjectsParams) (*modelV1.ListObjectsResponse, error) {
	var result modelV1.ListObjectsResponse
	amount := int(*req.Amount)
	tags, err := s.lakefsClient.ListObjectsWithResponse(ctx, repo, branch, &lakefs.ListObjectsParams{
		UserMetadata: req.UserMetadata,
		Presign:      req.Presign,

		// After return items after this value
		After: req.After,

		// Amount how many items to return
		Amount: &amount,

		// Delimiter delimiter used to group common prefixes by
		Delimiter: req.Delimiter,

		// Prefix return items prefixed with this value
		Prefix: req.Prefix,
	})
	if err != nil {
		s.log.Errorf("failed to list tags: %v", err)
		return nil, err
	}
	if tags.JSON200 != nil {
		result.Pagination = &modelV1.Pagination{
			HasMore:    tags.JSON200.Pagination.HasMore,
			MaxPerPage: int32(tags.JSON200.Pagination.MaxPerPage),
			NextOffset: tags.JSON200.Pagination.NextOffset,
			Results:    int32(tags.JSON200.Pagination.Results),
		}
		for _, stats := range tags.JSON200.Results {
			ref := modelV1.ObjectStats{
				Checksum:    stats.Checksum,
				ContentType: stats.ContentType,
				// Metadata: 用户自定义元数据（可选）
				// Mtime: Unix时间戳（秒级）
				Mtime: stats.Mtime,
				// 路径
				Path: stats.Path,
				// PathType: 路径类型（对应Go的ObjectStatsPathType，string类型）
				PathType: string(stats.PathType),
				// PhysicalAddress: 底层对象存储上的对象位置，原生URI或预签名HTTP URL
				PhysicalAddress: stats.PhysicalAddress,
				// PhysicalAddressExpiry: 预签名URL的过期时间（Unix时间戳，可选）
				PhysicalAddressExpiry: stats.PhysicalAddressExpiry,
				// SizeBytes: 对象大小（字节数，可选）
				SizeBytes: stats.SizeBytes,
			}
			if stats.Metadata != nil {
				ref.Metadata = *stats.Metadata
			}
			result.Results = append(result.Results, &ref)
		}
	} else {
		return nil, s.handleRepositoryResponse(tags.HTTPResponse, "list repository objects")
	}
	return &result, nil
}

func (s *ModelRegistryService) ListTags(ctx context.Context, req *modelV1.Model) (*modelV1.ListTagsResponse, error) {
	var result modelV1.ListTagsResponse
	tags, err := s.lakefsClient.ListTagsWithResponse(ctx, req.GetUuid(), &lakefs.ListTagsParams{})
	if err != nil {
		s.log.Errorf("failed to list tags: %v", err)
		return nil, err
	}
	if tags.JSON200 != nil {
		for _, tag := range tags.JSON200.Results {
			ref := modelV1.Ref{
				Id:       &tag.Id,
				CommitId: &tag.CommitId,
			}
			result.Results = append(result.Results, &ref)
		}
	} else {
		return nil, s.handleRepositoryResponse(tags.HTTPResponse, "create repository")
	}
	return &result, nil
}

func (s *ModelRegistryService) Delete(ctx context.Context, req *modelV1.Model) (*emptypb.Empty, error) {
	name := req.GetUuid()
	if name == "" {
		return nil, fmt.Errorf("model uuid is required")
	}
	key := fmt.Sprintf("%s/%s", req.GetSlug(), req.GetUuid())
	err := s.ossClient.DeletePath(ctx, s.c.Lakefs.BucketName, key)
	if err != nil {
		s.log.Errorf("failed to delete model data from storage: %v", err)
		return nil, err
	}
	resp, err := s.lakefsClient.DeleteRepositoryWithResponse(ctx, name)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusNoContent && resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("failed to delete repository: %s", resp.Status())
	}

	return &emptypb.Empty{}, nil
}

func (s *ModelRegistryService) Exists(ctx context.Context, req *modelV1.ModelExistsRequest) (*modelV1.ModelExistsResponse, error) {
	name := req.GetName()
	if name == "" {
		return &modelV1.ModelExistsResponse{Exist: false}, nil
	}

	resp, err := s.lakefsClient.GetRepositoryWithResponse(ctx, name)
	if err != nil {
		return nil, err
	}
	exists := resp.StatusCode() == http.StatusOK
	return &modelV1.ModelExistsResponse{Exist: exists}, nil
}

func (s *ModelRegistryService) convertRepoToModel(repo lakefs.Repository) *modelV1.Model {
	return &modelV1.Model{
		Name:        &repo.Id,
		Slug:        &repo.Id,
		CreatedAt:   timestamppb.New(time.Unix(repo.CreationDate, 0)),
		Description: &repo.StorageNamespace,
		//DefaultBranch:    &repo.DefaultBranch,
	}
}
