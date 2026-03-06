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
	"go-wind-admin/app/server/service/internal/data/ent/inference"
	"go-wind-admin/app/server/service/internal/data/ent/predicate"

	"github.com/tx7do/go-utils/copierutil"
	"github.com/tx7do/go-utils/mapper"

	inferenceV1 "go-wind-admin/api/gen/go/inference/service/v1"
	serverV1 "go-wind-admin/api/gen/go/server/service/v1"
)

type InferenceRepo struct {
	entClient *entCrud.EntClient[*ent.Client]
	log       *log.Helper

	mapper          *mapper.CopierMapper[inferenceV1.Inference, ent.Inference]
	statusConverter *mapper.EnumTypeConverter[inferenceV1.Inference_Status, inference.Status]

	repository *entCrud.Repository[
		ent.InferenceQuery, ent.InferenceSelect,
		ent.InferenceCreate, ent.InferenceCreateBulk,
		ent.InferenceUpdate, ent.InferenceUpdateOne,
		ent.InferenceDelete,
		predicate.Inference,
		inferenceV1.Inference, ent.Inference,
	]
}

func NewInferenceRepo(ctx *bootstrap.Context, entClient *entCrud.EntClient[*ent.Client]) *InferenceRepo {
	repo := &InferenceRepo{
		log:             ctx.NewLoggerHelper("inference/repo/server-service"),
		entClient:       entClient,
		mapper:          mapper.NewCopierMapper[inferenceV1.Inference, ent.Inference](),
		statusConverter: mapper.NewEnumTypeConverter[inferenceV1.Inference_Status, inference.Status](inferenceV1.Inference_Status_name, inferenceV1.Inference_Status_value),
	}

	repo.init()

	return repo
}

func (r *InferenceRepo) init() {
	r.repository = entCrud.NewRepository[
		ent.InferenceQuery, ent.InferenceSelect,
		ent.InferenceCreate, ent.InferenceCreateBulk,
		ent.InferenceUpdate, ent.InferenceUpdateOne,
		ent.InferenceDelete,
		predicate.Inference,
		inferenceV1.Inference, ent.Inference,
	](r.mapper)

	r.mapper.AppendConverters(copierutil.NewTimeStringConverterPair())
	r.mapper.AppendConverters(copierutil.NewTimeTimestamppbConverterPair())

	r.mapper.AppendConverters(r.statusConverter.NewConverterPair())
}

func (r *InferenceRepo) Count(ctx context.Context, whereCond []func(s *sql.Selector)) (int, error) {
	builder := r.entClient.Client().Inference.Query()
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

func (r *InferenceRepo) List(ctx context.Context, req *paginationV1.PagingRequest) (*inferenceV1.ListInferenceResponse, error) {
	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}

	builder := r.entClient.Client().Inference.Query()

	ret, err := r.repository.ListWithPaging(ctx, builder, builder.Clone(), req)
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return &inferenceV1.ListInferenceResponse{Total: 0, Items: nil}, nil
	}

	return &inferenceV1.ListInferenceResponse{
		Total: ret.Total,
		Items: ret.Items,
	}, nil
}

func (r *InferenceRepo) IsExist(ctx context.Context, id uint64) (bool, error) {
	exist, err := r.entClient.Client().Inference.Query().
		Where(inference.IDEQ(id)).
		Exist(ctx)
	if err != nil {
		r.log.Errorf("query exist failed: %s", err.Error())
		return false, serverV1.ErrorInternalServerError("query exist failed")
	}
	return exist, nil
}

func (r *InferenceRepo) Get(ctx context.Context, req *inferenceV1.GetInferenceRequest) (*inferenceV1.Inference, error) {
	if req == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}

	builder := r.entClient.Client().Inference.Query()

	var whereCond []func(s *sql.Selector)
	switch req.QueryBy.(type) {
	default:
	case *inferenceV1.GetInferenceRequest_Id:
		whereCond = append(whereCond, inference.IDEQ(req.GetId()))

	case *inferenceV1.GetInferenceRequest_Name:
		whereCond = append(whereCond, inference.NameEQ(req.GetName()))
	}

	dto, err := r.repository.Get(ctx, builder, req.GetViewMask(), whereCond...)
	if err != nil {
		return nil, err
	}

	return dto, err
}

// GetAuthToken 获取 API 鉴权 Token (敏感字段单独查)
func (r *InferenceRepo) GetAuthToken(ctx context.Context, id uint64) (string, error) {
	s, err := r.entClient.Client().Inference.Query().
		Where(inference.ID(id)).
		Select(inference.FieldAuthToken).
		Only(ctx)

	if err != nil {
		if ent.IsNotFound(err) {
			return "", inferenceV1.ErrorNotFound("inference not found")
		}
		return "", err
	}
	// inference.FieldAuthToken 生成的结构体如果是 *string 或 string，视 optional 而定
	// 假设你 Schema 里是 Optional() -> string (Ent值类型)，如果是空字符串则返回空
	if s.AuthToken == "" {
		return "", nil
	}
	return s.AuthToken, nil
}

// CreateWithTx 在事务中创建
func (r *InferenceRepo) CreateWithTx(ctx context.Context, tx *ent.Tx, data *inferenceV1.Inference, authToken string) (dto *inferenceV1.Inference, err error) {
	if data == nil {
		return nil, serverV1.ErrorBadRequest("invalid parameter")
	}
	builder := tx.Inference.Create().
		SetName(data.Name).
		SetNillableDescription(data.Description).
		SetNillableRemark(data.Remark).
		SetNillableStatus(r.statusConverter.ToEntity(data.Status)).
		SetCreatedAt(time.Now()).
		SetQuota(data.Quota).
		SetTenantName(data.TenantName).
		SetDeploymentName(data.DeploymentName).
		SetServiceName(data.ServiceName).
		SetPort(data.Port).
		SetNillableURL(data.Url).
		SetModelID(data.ModelId).
		SetNillableImage(data.Image).
		SetNillableAuthToken(&authToken).
		SetEnv(data.Env).
		SetCommand(data.Command).
		SetArgs(data.Args).
		SetNillableModelName(data.ModelName).
		// 扩缩容配置
		SetReplicas(data.Replicas).
		SetNillableMinReplicas(data.MinReplicas).
		SetNillableMaxReplicas(data.MaxReplicas).
		// 计费与过期
		SetExpirationMinutes(data.ExpirationMinutes)

	if data.Id != nil {
		builder.SetID(data.GetId())
	}

	var entity *ent.Inference
	if entity, err = builder.Save(ctx); err != nil {
		r.log.Errorf("insert inference failed: %s", err.Error())
		return nil, serverV1.ErrorInternalServerError("insert inference failed")
	}

	return r.mapper.ToDTO(entity), nil
}

func (r *InferenceRepo) Create(ctx context.Context, req *inferenceV1.CreateInferenceRequest) (dto *inferenceV1.Inference, err error) {
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

	// AuthToken 可能在 Req 顶层字段，也可能在 Data 里，这里假设 Req.Data 不包含 sensitive token，由调用方传入
	// 如果你的 Proto 定义里 token 在 request 顶层，记得传进来
	token := ""
	if req.AuthToken != nil {
		token = *req.AuthToken
	}
	return r.CreateWithTx(ctx, tx, req.GetData(), token)
}

func (r *InferenceRepo) Update(ctx context.Context, req *inferenceV1.UpdateInferenceRequest) (*inferenceV1.Inference, error) {
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
			createReq := &inferenceV1.CreateInferenceRequest{Data: req.Data}
			return r.Create(ctx, createReq)
		}
	}

	builder := r.entClient.Client().Debug().Inference.UpdateOneID(req.GetId())
	result, err := r.repository.UpdateOne(ctx, builder, req.Data, req.GetUpdateMask(),
		func(dto *inferenceV1.Inference) {
			builder.
				SetNillableStatus(r.statusConverter.ToEntity(req.Data.Status)).
				SetNillableDescription(req.Data.Description).
				SetNillableRemark(req.Data.Remark).
				SetEnv(req.Data.Env).
				SetCommand(req.Data.Command).
				SetArgs(req.Data.Args).
				SetNillableModelName(req.Data.ModelName).
				SetNillableModelID(&req.Data.ModelId).
				SetNillableURL(req.Data.Url).
				SetReplicas(req.Data.Replicas).
				SetNillableMinReplicas(req.Data.MinReplicas).
				SetNillableMaxReplicas(req.Data.MaxReplicas).
				SetUpdatedAt(time.Now())
		},
		func(s *sql.Selector) {
			s.Where(sql.EQ(inference.FieldID, req.GetId()))
		},
	)

	return result, err
}

// UpdateLifecycle 专门用于处理生命周期状态跃迁 (开始、停止、计费)
func (r *InferenceRepo) UpdateLifecycle(ctx context.Context, id uint64, status inferenceV1.Inference_Status, updateTimeType string) (*inferenceV1.Inference, error) {
	builder := r.entClient.Client().Inference.UpdateOneID(id).
		SetNillableStatus(r.statusConverter.ToEntity(&status))

	now := time.Now()

	// 根据类型更新特定时间字段
	switch updateTimeType {
	case "start":
		builder.SetStartedAt(now)
	case "finish":
		builder.SetFinishedAt(now)
	}

	// 总是更新 LastActivityTime
	builder.SetLastActivityTime(now)

	// 1. 执行更新
	if err := builder.Exec(ctx); err != nil {
		if ent.IsNotFound(err) {
			return nil, inferenceV1.ErrorNotFound("inference not found")
		}
		return nil, err
	}

	// 2. 重新查询完整对象
	entity, err := r.entClient.Client().Inference.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return r.mapper.ToDTO(entity), nil
}

// CalculateAndSaveBilling 计算并 **累加** 挂钟时间 (Wall Clock Time)
// 这只是记录服务“在线”了多久，不代表资源消耗
func (r *InferenceRepo) CalculateAndSaveBilling(ctx context.Context, id uint64) error {
	s, err := r.entClient.Client().Inference.Query().
		Where(inference.ID(id)).
		Select(inference.FieldStartedAt, inference.FieldFinishedAt, inference.FieldBillingDurationSec).
		Only(ctx)

	if err != nil {
		if ent.IsNotFound(err) {
			r.log.Warnf("billing skipped: inference %d not found", id)
			return nil
		}
		return err
	}

	if s.StartedAt.IsZero() || s.FinishedAt.IsZero() {
		return nil
	}

	// 计算本次运行时长 (秒)
	currentRunDuration := int64(s.FinishedAt.Sub(s.StartedAt).Seconds())
	if currentRunDuration < 0 {
		currentRunDuration = 0
	}

	newTotalDuration := s.BillingDurationSec + currentRunDuration

	err = r.entClient.Client().Inference.UpdateOneID(id).
		SetBillingDurationSec(newTotalDuration).
		Exec(ctx)

	if err != nil && !ent.IsNotFound(err) {
		return err
	}
	return nil
}

// AccumulateUsage 核心 HPA 计费逻辑
// 供 Workflow 周期性调用，累加资源消耗
func (r *InferenceRepo) AccumulateUsage(ctx context.Context, id uint64, replicaSeconds int64) error {
	// 原子累加 (AddUsageReplicaSeconds)
	// 如果你需要同时累加 CPU/GPU，需要先查 Quota 再乘，或者让 Caller 算好传进来
	// 这里演示最核心的 ReplicaSeconds 累加

	// 注意：Ent 的 AddXxx 方法生成的 SQL 是 atomic update (SET col = col + val)
	err := r.entClient.Client().Inference.UpdateOneID(id).
		AddUsageReplicaSeconds(replicaSeconds).
		// AddUsageCpuSeconds(cpuSeconds).
		// AddUsageGpuSeconds(gpuSeconds).
		Exec(ctx)

	if err != nil {
		if ent.IsNotFound(err) {
			return nil // 忽略
		}
		return err
	}
	return nil
}

func (r *InferenceRepo) Delete(ctx context.Context, req *inferenceV1.DeleteInferenceRequest) error {
	if req == nil {
		return inferenceV1.ErrorBadRequest("invalid parameter")
	}

	if err := r.entClient.Client().Inference.DeleteOneID(req.GetId()).Exec(ctx); err != nil {
		if ent.IsNotFound(err) {
			return inferenceV1.ErrorNotFound("Inference not found")
		}
		r.log.Errorf("delete one data failed: %s", err.Error())
		return inferenceV1.ErrorInternalServerError("delete failed")
	}

	return nil
}
