package schema

import (
	tenantV1 "go-wind-admin/api/gen/go/tenant/service/v1"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/tx7do/go-crud/entgo/mixin"
)

// Inference holds the schema definition for the Inference entity.
type Inference struct {
	ent.Schema
}

func (Inference) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{
			Table:     "inferences",
			Charset:   "utf8mb4",
			Collation: "utf8mb4_bin",
		},
		entsql.WithComments(true),
		schema.Comment("推理服务表"),
	}
}

// Fields of the Inference.
func (Inference) Fields() []ent.Field {
	return []ent.Field{
		// --- 基础信息 ---
		field.String("name").
			Comment("推理服务名称").
			NotEmpty().
			Immutable().
			Nillable(),

		field.String("tenant_name").
			Comment("租户名").
			NotEmpty().
			Immutable().
			Nillable(),

		field.Enum("status").
			Comment("状态").
			Optional().
			// 去掉 Nillable，让默认值生效
			Default("UNSPECIFIED").
			NamedValues(
				"Unspecified", "UNSPECIFIED",
				"Creating", "CREATING",
				"Running", "RUNNING",
				"Stopped", "STOPPED",
				"Restarting", "RESTARTING",
				"Scaling", "SCALING", // 新增：扩缩容中
				"Error", "ERROR",
				"Deleting", "DELETING",
			),

		// --- 关联模型 ---
		field.Uint64("model_id").
			Comment("关联的模型ID").
			Optional(),

		field.String("model_name").
			Comment("模型名称快照").
			Optional(),

		field.String("image").
			Comment("镜像地址(允许覆盖模型默认镜像)").
			Optional(),

		// --- 伸缩与配置 (核心差异点) ---

		field.Int32("replicas").
			Comment("当前期望副本数").
			Default(1).
			Min(0),

		field.Int32("min_replicas").
			Comment("HPA最小副本数(0表示不启用HPA)").
			Default(0).
			Optional(),

		field.Int32("max_replicas").
			Comment("HPA最大副本数").
			Default(0).
			Optional(),

		field.Int32("port").
			Comment("服务监听端口").
			Default(8000),

		// --- 环境与配置 ---

		field.JSON("command", []string{}).
			Comment("启动命令(覆盖默认)").
			Optional(),

		field.JSON("args", []string{}).
			Comment("启动参数").
			Optional(),

		field.JSON("env", map[string]string{}).
			Comment("环境变量").
			Optional(),

		// --- 网络与安全 ---

		field.String("deployment_name").
			Comment("K8s Deployment名称").
			NotEmpty().
			Immutable(),

		field.String("service_name").
			Comment("K8s Service名称").
			NotEmpty().
			Immutable(),

		field.String("url").
			Comment("访问域名").
			Optional(),

		field.String("auth_token").
			Comment("API访问令牌(Bearer Token)").
			Sensitive(). // 标记为敏感字段，日志脱敏
			Optional(),

		// --- 资源配额 ---

		// 注意：这里的 Quota 通常指 "单 Pod" 的配额
		// 总配额消耗 = Quota * Replicas
		field.JSON("quota", &tenantV1.Quota{}).
			Comment("单副本资源配额").
			Optional(),

		// --- 生命周期与计费 ---

		field.Time("started_at").
			Comment("最近一次启动时间").
			Optional(),

		field.Time("finished_at").
			Comment("最近一次停止时间").
			Optional(),

		field.Time("last_activity_time").
			Comment("最近一次活跃时间(心跳/扩缩容)").
			Optional(),

		// 累计计费时长 (Wall Clock Time)
		field.Int64("billing_duration_sec").
			Comment("服务累计在线时长(秒)").
			Default(0),

		// 累计使用的资源量 (CPU核数 * 秒)
		field.Int64("usage_cpu_seconds").
			Comment("累计CPU使用量(核*秒)").
			Default(0),

		// 累计使用的资源量 (GPU卡数 * 秒)
		field.Int64("usage_gpu_seconds").
			Comment("累计GPU使用量(卡*秒)").
			Default(0),

		// 累计的副本运行时间 (Replica * 秒)
		// HPA 场景下的核心计费指标
		field.Int64("usage_replica_seconds").
			Comment("累计副本运行时长(个*秒)").
			Default(0),

		field.Int64("expiration_minutes").
			Comment("自动过期时长(分钟)，0为不过期").
			Default(0),
	}
}

// Mixin of the Inference.
func (Inference) Mixin() []ent.Mixin {
	return []ent.Mixin{
		mixin.AutoIncrementId64{},
		mixin.TimeAt{},
		mixin.Remark{},
		mixin.Description{},
		mixin.Tag{},
		mixin.IsEnabled{},
	}
}

// Indexes of the Inference.
func (Inference) Indexes() []ent.Index {
	return []ent.Index{
		// 租户下名称唯一
		index.Fields("tenant_name", "name").
			Unique().
			StorageKey("uk_inference_name"),

		// 部署名唯一
		index.Fields("tenant_name", "deployment_name").
			Unique().
			StorageKey("uk_inference_deployment"),
	}
}
