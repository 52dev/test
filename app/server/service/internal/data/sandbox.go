package data

import (
	"context"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/go-kratos/kratos/v2/log"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	entCrud "github.com/tx7do/go-crud/entgo"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"go-wind-admin/app/server/service/internal/data/ent"
	"go-wind-admin/app/server/service/internal/data/ent/predicate"
	"go-wind-admin/app/server/service/internal/data/ent/sandbox"

	"github.com/tx7do/go-utils/copierutil"
	"github.com/tx7do/go-utils/mapper"

	sandboxV1 "go-wind-admin/api/gen/go/sandbox/service/v1"
	serverV1 "go-wind-admin/api/gen/go/server/service/v1"
)

type SandboxRepo struct {
	entClient *entCrud.EntClient[*ent.Client]
	log       *log.Helper

	mapper          *mapper.CopierMapper[sandboxV1.Sandbox, ent.Sandbox]
	statusConverter *mapper.EnumTypeConverter[sandboxV1.Sandbox_Status, sandbox.Status]

	repository *entCrud.Repository[
		ent.SandboxQuery, ent.SandboxSelect,
		ent.SandboxCreate, ent.SandboxCreateBulk,
		ent.SandboxUpdate, ent.SandboxUpdateOne,
		ent.SandboxDelete,
		predicate.Sandbox,
		sandboxV1.Sandbox, ent.Sandbox,
	]
}

func NewSandboxRepo(ctx *bootstrap.Context, entClient *entCrud.EntClient[*ent.Client]) *SandboxRepo {
	repo := &SandboxRepo{
		log:             ctx.NewLoggerHelper("sandbox/repo/server-service"),
		entClient:       entClient,
		mapper:          mapper.NewCopierMapper[sandboxV1.Sandbox, ent.Sandbox](),
		statusConverter: mapper.NewEnumTypeConverter[sandboxV1.Sandbox_Status, sandbox.Status](sandboxV1.Sandbox_Status_name, sandboxV1.Sandbox_Status_value),
	}

	repo.init()

	return repo
}

func (r *SandboxRepo) init() {
	r.repository = entCrud.NewRepository[
		ent.SandboxQuery, ent.SandboxSelect,
		ent.SandboxCreate, ent.SandboxCreateBulk,
		ent.SandboxUpdate, ent.SandboxUpdateOne,
		ent.SandboxDelete,
		predicate.Sandbox,
		sandboxV1.Sandbox, ent.Sandbox,
	](r.mapper)

	r.mapper.AppendConverters(copierutil.NewTimeStringConverterPair())
	r.mapper.AppendConverters(copierutil.NewTimeTimestamppbConverterPair())

	r.mapper.AppendConverters(r.statusConverter.NewConverterPair())
}

func (r *SandboxRepo) Count(ctx context.Context, whereCond []func(s *sql.Selector)) (int, error) {
	builder := r.entClient.Client().Sandbox.Query()
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

func (r *SandboxRepo) List(ctx context.Context, req *paginationV1.PagingRequest) (*sandboxV1.ListSandboxResponse, error) {
	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}

	builder := r.entClient.Client().Sandbox.Query()

	ret, err := r.repository.ListWithPaging(ctx, builder, builder.Clone(), req)
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return &sandboxV1.ListSandboxResponse{Total: 0, Items: nil}, nil
	}

	return &sandboxV1.ListSandboxResponse{
		Total: ret.Total,
		Items: ret.Items,
	}, nil
}

func (r *SandboxRepo) IsExist(ctx context.Context, id uint64) (bool, error) {
	exist, err := r.entClient.Client().Sandbox.Query().
		Where(sandbox.IDEQ(id)).
		Exist(ctx)
	if err != nil {
		r.log.Errorf("query exist failed: %s", err.Error())
		return false, serverV1.ErrorInternalServerError("query exist failed")
	}
	return exist, nil
}

func (r *SandboxRepo) Get(ctx context.Context, req *sandboxV1.GetSandboxRequest) (*sandboxV1.Sandbox, error) {
	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}

	builder := r.entClient.Client().Sandbox.Query()

	var whereCond []func(s *sql.Selector)
	switch req.QueryBy.(type) {
	default:
	case *sandboxV1.GetSandboxRequest_Id:
		whereCond = append(whereCond, sandbox.IDEQ(req.GetId()))

	case *sandboxV1.GetSandboxRequest_Name:
		whereCond = append(whereCond, sandbox.NameEQ(req.GetName()))
	}

	dto, err := r.repository.Get(ctx, builder, req.GetViewMask(), whereCond...)
	if err != nil {
		return nil, err
	}

	return dto, err
}

func (r *SandboxRepo) GetPassword(ctx context.Context, id uint64) (string, error) {
	// 1. 直接查询 Ent 对象，只查 Password 字段
	s, err := r.entClient.Client().Sandbox.Query().
		Where(sandbox.ID(id)).
		Select(sandbox.FieldPassword). // 只查密码，性能好
		Only(ctx)

	if err != nil {
		if ent.IsNotFound(err) {
			return "", sandboxV1.ErrorNotFound("sandbox not found")
		}
		return "", err
	}

	// 2. 处理空值
	if s.Password == nil {
		return "", nil
	}

	return *s.Password, nil
}

// CreateWithTx 在事务中创建用户
func (r *SandboxRepo) CreateWithTx(ctx context.Context, tx *ent.Tx, data *sandboxV1.Sandbox, password *string) (dto *sandboxV1.Sandbox, err error) {
	if data == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}
	builder := tx.Sandbox.Create().
		SetName(data.Name).
		SetNillableDescription(data.Description).
		SetNillableRemark(data.Remark).
		SetNillableStatus(r.statusConverter.ToEntity(data.Status)).
		SetCreatedAt(time.Now()).
		SetQuota(data.Quota).
		SetTenantName(data.TenantName).
		SetDeploymentName(data.DeploymentName).
		SetNillableURL(data.Url).
		SetModelID(data.ModelId).
		SetCommandMapping(data.CommandMapping).
		SetNillablePassword(password).
		SetEnv(data.Env).
		SetNillableModelName(data.ModelName).
		// 自动关机设置
		SetExpirationMinutes(data.ExpirationMinutes)

	if data.Id != nil {
		builder.SetID(data.GetId())
	}

	var entity *ent.Sandbox
	if entity, err = builder.Save(ctx); err != nil {
		r.log.Errorf("insert user failed: %s", err.Error())
		return nil, serverV1.ErrorInternalServerError("insert user failed")
	}

	return r.mapper.ToDTO(entity), nil
}

func (r *SandboxRepo) Create(ctx context.Context, req *sandboxV1.CreateSandboxRequest) (dto *sandboxV1.Sandbox, err error) {
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

	return r.CreateWithTx(ctx, tx, req.GetData(), req.Password)
}

func (r *SandboxRepo) Update(ctx context.Context, req *sandboxV1.UpdateSandboxRequest) (*sandboxV1.Sandbox, error) {
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
			createReq := &sandboxV1.CreateSandboxRequest{Data: req.Data}
			return r.Create(ctx, createReq)
		}
	}

	builder := r.entClient.Client().Debug().Sandbox.UpdateOneID(req.GetId())
	result, err := r.repository.UpdateOne(ctx, builder, req.Data, req.GetUpdateMask(),
		func(dto *sandboxV1.Sandbox) {
			builder.
				SetNillableStatus(r.statusConverter.ToEntity(req.Data.Status)).
				SetNillableDescription(req.Data.Description).
				SetNillableRemark(req.Data.Remark).
				SetEnv(req.Data.Env).
				SetNillableModelName(req.Data.ModelName).
				SetNillableModelID(&req.Data.ModelId).
				SetNillableURL(req.Data.Url).
				SetUpdatedAt(time.Now())
		},
		func(s *sql.Selector) {
			s.Where(sql.EQ(sandbox.FieldID, req.GetId()))
		},
	)

	return result, err
}

// UpdateLifecycle 专门用于处理生命周期状态跃迁 (开始、停止、计费)
func (r *SandboxRepo) UpdateLifecycle(ctx context.Context, id uint64, status sandboxV1.Sandbox_Status, updateTimeType string) (*sandboxV1.Sandbox, error) {
	builder := r.entClient.Client().Sandbox.UpdateOneID(id).
		SetNillableStatus(r.statusConverter.ToEntity(&status))

	now := time.Now()

	// 根据类型更新特定时间字段
	switch updateTimeType {
	case "start":
		builder.SetStartedAt(now)
	case "finish":
		builder.SetFinishedAt(now)
	}

	// 1. 执行更新
	if err := builder.Exec(ctx); err != nil {
		if ent.IsNotFound(err) {
			return nil, sandboxV1.ErrorNotFound("sandbox not found")
		}
		return nil, err
	}

	// 2. 重新查询完整对象 (确保返回所有字段，供 Workflow 逻辑判断)
	entity, err := r.entClient.Client().Sandbox.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return r.mapper.ToDTO(entity), nil
}

// CalculateAndSaveBilling 计算并 **累加** 计费时长
func (r *SandboxRepo) CalculateAndSaveBilling(ctx context.Context, id uint64) error {
	// 1. 获取 StartedAt, FinishedAt, 和当前的 BillingDurationSec
	s, err := r.entClient.Client().Sandbox.Query().
		Where(sandbox.ID(id)).
		Select(sandbox.FieldStartedAt, sandbox.FieldFinishedAt, sandbox.FieldBillingDurationSec).
		Only(ctx)

	if err != nil {
		// 修正点：只忽略 NotFound 错误
		if ent.IsNotFound(err) {
			r.log.Warnf("billing skipped: sandbox %d not found", id)
			return nil
		}
		return err
	}

	// 2. 校验时间有效性
	if s.StartedAt.IsZero() || s.FinishedAt.IsZero() {
		return nil
	}

	// 3. 计算本次运行时长 (秒)
	currentRunDuration := int64(s.FinishedAt.Sub(s.StartedAt).Seconds())
	if currentRunDuration < 0 {
		currentRunDuration = 0
	}

	// 4. 计算新的累计时长 (旧的累积值 + 本次时长)
	newTotalDuration := s.BillingDurationSec + currentRunDuration
	// 5. 更新 DB
	err = r.entClient.Client().Sandbox.UpdateOneID(id).
		SetBillingDurationSec(newTotalDuration).
		Exec(ctx)

	if err != nil {
		if ent.IsNotFound(err) {
			return nil // 更新时记录被删，忽略
		}
		return err // 更新失败（锁冲突、DB 挂了），需要重试
	}

	return nil
}

func (r *SandboxRepo) Delete(ctx context.Context, req *sandboxV1.DeleteSandboxRequest) error {
	if req == nil {
		return sandboxV1.ErrorBadRequest("invalid parameter")
	}

	if err := r.entClient.Client().Sandbox.DeleteOneID(req.GetId()).Exec(ctx); err != nil {
		if ent.IsNotFound(err) {
			return sandboxV1.ErrorNotFound("Sandbox not found")
		}

		r.log.Errorf("delete one data failed: %s", err.Error())

		return sandboxV1.ErrorInternalServerError("delete failed")
	}

	return nil
}
