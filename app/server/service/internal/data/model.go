package data

import (
	"context"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/emptypb"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	entCrud "github.com/tx7do/go-crud/entgo"

	"go-wind-admin/app/server/service/internal/data/ent"
	"go-wind-admin/app/server/service/internal/data/ent/model"
	"go-wind-admin/app/server/service/internal/data/ent/predicate"

	"github.com/tx7do/go-utils/copierutil"
	"github.com/tx7do/go-utils/mapper"

	modelV1 "go-wind-admin/api/gen/go/model/service/v1"
	serverV1 "go-wind-admin/api/gen/go/server/service/v1"
)

type ModelRepo struct {
	entClient *entCrud.EntClient[*ent.Client]
	log       *log.Helper

	mapper          *mapper.CopierMapper[modelV1.Model, ent.Model]
	statusConverter *mapper.EnumTypeConverter[modelV1.Model_Status, model.Status]
	typeConverter   *mapper.EnumTypeConverter[modelV1.Model_Type, model.Type]

	repository *entCrud.Repository[
		ent.ModelQuery, ent.ModelSelect,
		ent.ModelCreate, ent.ModelCreateBulk,
		ent.ModelUpdate, ent.ModelUpdateOne,
		ent.ModelDelete,
		predicate.Model,
		modelV1.Model, ent.Model,
	]
}

func NewModelRepo(ctx *bootstrap.Context, entClient *entCrud.EntClient[*ent.Client]) *ModelRepo {
	repo := &ModelRepo{
		log:             ctx.NewLoggerHelper("model/repo/server-service"),
		entClient:       entClient,
		mapper:          mapper.NewCopierMapper[modelV1.Model, ent.Model](),
		statusConverter: mapper.NewEnumTypeConverter[modelV1.Model_Status, model.Status](modelV1.Model_Status_name, modelV1.Model_Status_value),
		typeConverter:   mapper.NewEnumTypeConverter[modelV1.Model_Type, model.Type](modelV1.Model_Type_name, modelV1.Model_Type_value),
	}

	repo.init()

	return repo
}

func (r *ModelRepo) init() {
	r.repository = entCrud.NewRepository[
		ent.ModelQuery, ent.ModelSelect,
		ent.ModelCreate, ent.ModelCreateBulk,
		ent.ModelUpdate, ent.ModelUpdateOne,
		ent.ModelDelete,
		predicate.Model,
		modelV1.Model, ent.Model,
	](r.mapper)

	r.mapper.AppendConverters(copierutil.NewTimeStringConverterPair())
	r.mapper.AppendConverters(copierutil.NewTimeTimestamppbConverterPair())

	r.mapper.AppendConverters(r.statusConverter.NewConverterPair())
	r.mapper.AppendConverters(r.typeConverter.NewConverterPair())
}

func (r *ModelRepo) Count(ctx context.Context, whereCond []func(s *sql.Selector)) (int, error) {
	builder := r.entClient.Client().Model.Query()
	if len(whereCond) != 0 {
		builder.Modify(whereCond...)
	}

	count, err := builder.Count(ctx)
	if err != nil {
		r.log.Errorf("query count failed: %s", err.Error())
		return 0, serverV1.ErrorInternalServerError("query count failed")
	}

	return count, nil
}

func (r *ModelRepo) List(ctx context.Context, req *paginationV1.PagingRequest) (*modelV1.ListModelResponse, error) {
	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}

	builder := r.entClient.Client().Model.Query()

	ret, err := r.repository.ListWithPaging(ctx, builder, builder.Clone(), req)
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return &modelV1.ListModelResponse{Total: 0, Items: nil}, nil
	}

	return &modelV1.ListModelResponse{
		Total: ret.Total,
		Items: ret.Items,
	}, nil
}

func (r *ModelRepo) IsExist(ctx context.Context, id uint64) (bool, error) {
	exist, err := r.entClient.Client().Model.Query().
		Where(model.IDEQ(id)).
		Exist(ctx)
	if err != nil {
		r.log.Errorf("query exist failed: %s", err.Error())
		return false, serverV1.ErrorInternalServerError("query exist failed")
	}
	return exist, nil
}

func (r *ModelRepo) Get(ctx context.Context, req *modelV1.GetModelRequest) (*modelV1.Model, error) {
	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}

	builder := r.entClient.Client().Model.Query()

	var whereCond []func(s *sql.Selector)
	whereCond = append(whereCond, model.IDEQ(req.GetId()))

	dto, err := r.repository.Get(ctx, builder, req.GetViewMask(), whereCond...)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, serverV1.ErrorNotFound("model not found")
		}
		return nil, err
	}

	return dto, err
}

// CreateWithTx 在事务中创建用户
func (r *ModelRepo) CreateWithTx(ctx context.Context, tx *ent.Tx, data *modelV1.Model) (dto *modelV1.Model, err error) {
	if data == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}
	status := model.StatusNotCached
	builder := tx.Model.Create().
		SetNillableName(data.Name).
		SetNillableSlug(data.Slug).
		SetNillableDockerImageRef(data.DockerImageRef).
		SetNillableBaseFramework(data.BaseFramework).
		SetNillableStatus(&status).
		SetNillableDescription(data.Description).
		SetNillableRemark(data.Remark).
		SetNillableReadmeText(data.ReadmeText).
		SetNillableDownloads(data.Downloads).
		SetNillableLikes(data.Likes).
		SetNillableLicense(data.License).
		SetJupyterCommand(data.JupyterCommand).
		SetCommandMapping(data.CommandMapping).
		SetEnv(data.Env).
		SetPorts(sliceToIntSlice(data.Ports)).
		SetWritablePaths(data.WritablePaths).
		SetCreatedAt(time.Now())

	var entity *ent.Model
	if entity, err = builder.Save(ctx); err != nil {
		r.log.Errorf("insert model failed: %s", err.Error())
		return nil, serverV1.ErrorInternalServerError("insert model failed")
	}

	return r.mapper.ToDTO(entity), nil
}

func (r *ModelRepo) Create(ctx context.Context, req *modelV1.CreateModelRequest, afterCreate ...func(ctx context.Context, req *modelV1.Model) (*emptypb.Empty, error)) (dto *modelV1.Model, err error) {
	if req == nil || req.Data == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}

	var tx *ent.Tx
	tx, err = r.entClient.Client().Tx(ctx)
	if err != nil {
		r.log.Errorf("start transaction failed: %s", err.Error())
		return nil, serverV1.ErrorInternalServerError("start transaction failed")
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				r.log.Errorf("transaction rollback failed: %s", rollbackErr.Error())
			}
			return
		}
		if commitErr := tx.Commit(); commitErr != nil {
			r.log.Errorf("transaction commit failed: %s", commitErr.Error())
			err = serverV1.ErrorInternalServerError("transaction commit failed")
		}
	}()

	dto, err = r.CreateWithTx(ctx, tx, req.GetData())
	if err != nil {
		return nil, err
	}
	for _, callback := range afterCreate {
		if _, err = callback(ctx, dto); err != nil {
			r.log.Errorf("after create callback failed: %s", err.Error())
			return nil, err
		}
	}
	return dto, nil
}

func (r *ModelRepo) Update(ctx context.Context, req *modelV1.UpdateModelRequest) (*modelV1.Model, error) {
	if req == nil || req.Data == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}

	// 如果不存在则创建
	if req.GetAllowMissing() {
		exist, err := r.IsExist(ctx, req.GetId())
		if err != nil {
			return nil, err
		}
		if !exist {
			createReq := &modelV1.CreateModelRequest{Data: req.Data}
			return r.Create(ctx, createReq, nil)
		}
	}

	builder := r.entClient.Client().Debug().Model.UpdateOneID(req.GetId())
	result, err := r.repository.UpdateOne(ctx, builder, req.Data, req.GetUpdateMask(),
		func(dto *modelV1.Model) {
			builder.
				SetNillableStatus(r.statusConverter.ToEntity(req.Data.Status)).
				SetNillableDescription(req.Data.Description).
				SetNillableRemark(req.Data.Remark).
				SetUpdatedAt(time.Now())
			if len(req.Data.Ports) > 0 {
				builder.SetPorts(sliceToIntSlice(req.Data.Ports))
			}
			if req.Data.JupyterCommand != nil {
				builder.SetJupyterCommand(req.Data.JupyterCommand)
			}
		},
		func(s *sql.Selector) {
			s.Where(sql.EQ(model.FieldID, req.GetId()))
		},
	)

	return result, err
}

func (r *ModelRepo) Delete(ctx context.Context, req *modelV1.DeleteModelRequest) (err error) {
	var existResp *modelV1.ModelExistsResponse
	existReq := &modelV1.ModelExistsRequest{
		QueryBy: &modelV1.ModelExistsRequest_Id{Id: req.GetId()},
	}
	existResp, err = r.Exists(ctx, existReq)
	if err != nil {
		return err
	}
	if !existResp.Exist {
		return serverV1.ErrorNotFound("user not found")
	}

	var tx *ent.Tx
	tx, err = r.entClient.Client().Tx(ctx)
	if err != nil {
		r.log.Errorf("start transaction failed: %s", err.Error())
		return serverV1.ErrorInternalServerError("start transaction failed")
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				r.log.Errorf("transaction rollback failed: %s", rollbackErr.Error())
			}
			return
		}
		if commitErr := tx.Commit(); commitErr != nil {
			r.log.Errorf("transaction commit failed: %s", commitErr.Error())
			err = serverV1.ErrorInternalServerError("transaction commit failed")
		}
	}()

	builder := tx.Model.Delete()
	builder.Where(model.IDEQ(req.GetId()))

	if _, err = builder.Exec(ctx); err != nil {
		if ent.IsNotFound(err) {
			return serverV1.ErrorNotFound("model not found")
		}

		r.log.Errorf("delete one data failed: %s", err.Error())

		return serverV1.ErrorInternalServerError("delete failed")
	}
	return nil
}
func (r *ModelRepo) Exists(ctx context.Context, req *modelV1.ModelExistsRequest) (*modelV1.ModelExistsResponse, error) {
	builder := r.entClient.Client().Model.Query()

	switch req.QueryBy.(type) {
	case *modelV1.ModelExistsRequest_Id:
		builder.Where(model.IDEQ(req.GetId()))
	case *modelV1.ModelExistsRequest_Name:
		builder.Where(model.NameEQ(req.GetName()))
	default:
		return &modelV1.ModelExistsResponse{
			Exist: false,
		}, serverV1.ErrorBadRequest("invalid query by type")
	}

	exist, err := builder.Exist(ctx)
	if err != nil {
		r.log.Errorf("query exist failed: %s", err.Error())
		return &modelV1.ModelExistsResponse{
			Exist: false,
		}, serverV1.ErrorInternalServerError("query exist failed")
	}

	return &modelV1.ModelExistsResponse{
		Exist: exist,
	}, nil
}

func (r *ModelRepo) UpdateStatus(ctx context.Context, req *modelV1.Model, status modelV1.Model_Status) (*modelV1.Model, error) {
	builder := r.entClient.Client().Debug().Model.UpdateOneID(req.GetId())
	result, err := r.repository.UpdateOne(ctx, builder, req, nil,
		func(dto *modelV1.Model) {
			builder.
				SetNillableStatus(r.statusConverter.ToEntity(&status)).
				SetUpdatedAt(time.Now())
		},
		func(s *sql.Selector) {
			s.Where(sql.EQ(model.FieldID, req.GetId()))
		},
	)
	if err != nil {
		r.log.Errorf("update model status failed: %s", err.Error())
		return nil, serverV1.ErrorInternalServerError("update status failed")
	}
	return result, nil
}

func sliceToIntSlice[T ~int32 | ~uint32 | ~int | ~uint | ~int8 | ~int16 | ~int64 | ~uint8 | ~uint16 | ~uint64](src []T) []int {
	dst := make([]int, len(src))
	for i, v := range src {
		dst[i] = int(v)
	}
	return dst
}
