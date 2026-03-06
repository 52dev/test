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

// Sandbox holds the schema definition for the Sandbox entity.
type Sandbox struct {
	ent.Schema
}

func (Sandbox) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{
			Table:     "sandboxes",
			Charset:   "utf8mb4",
			Collation: "utf8mb4_bin",
		},
		entsql.WithComments(true),
		schema.Comment("沙箱表"),
	}
}

// Fields of the User.
func (Sandbox) Fields() []ent.Field {
	return []ent.Field{
		field.String("name").
			Comment("沙箱名").
			//Unique().
			NotEmpty().
			Immutable().
			Nillable(),

		field.Enum("status").
			Comment("状态").
			Optional().
			Nillable().
			Default("UNSPECIFIED").
			NamedValues(
				"Unspecified", "UNSPECIFIED",
				"Creating", "CREATING",
				"Running", "RUNNING",
				"Stopped", "STOPPED",
				"Restarting", "RESTARTING",
				"Error", "ERROR",
				"Deleting", "DELETING",
			),
		field.String("tenant_name").
			Comment("租户名").
			NotEmpty().
			Immutable().
			Nillable(),

		field.JSON("command_mapping", map[string]string{}).
			Comment("命令映射").
			Default(map[string]string{}).
			Optional(),

		field.JSON("env", map[string]string{}).
			Comment("环境变量").
			Default(map[string]string{}).
			Optional(),

		field.String("deployment_name").
			Comment("部署名").
			NotEmpty().
			Immutable().
			Nillable(),

		field.String("url").
			Comment("域名").
			NotEmpty().
			Optional().
			Nillable(),

		field.String("password").
			Comment("访问密码").
			Optional().
			Nillable(),

		// --- 新增关联字段 ---
		field.Uint64("model_id").
			Comment("关联的模型ID").
			Optional().
			Nillable(),

		field.String("model_name").
			Comment("关联的模型名称").
			Optional().
			Nillable(),

		field.JSON("quota", &tenantV1.Quota{}).
			Comment("资源配额").
			Optional(),

		// 真正开始运行的时间 (Kueue Admitted / Pod Running)
		field.Time("started_at").
			Comment("开始运行时间").
			Optional(),

		// 结束运行的时间 (Stopped / Expired)
		field.Time("finished_at").
			Comment("结束时间").
			Optional(),

		// 运行时长 (秒或毫秒)，用于计费
		field.Int64("billing_duration_sec").
			Comment("计费时长(秒)").
			Default(0),

		// 用户购买的时长 (分钟)，0 表示无限
		field.Int64("expiration_minutes").
			Comment("自动过期时长(分钟)").
			Default(0),
	}
}

// Mixin of the Sandbox.
func (Sandbox) Mixin() []ent.Mixin {
	return []ent.Mixin{
		mixin.AutoIncrementId64{},
		mixin.TimeAt{},
		mixin.Remark{},
		mixin.Description{},
		mixin.Tag{},
		mixin.IsEnabled{},
	}
}

// Indexes of the User.
func (Sandbox) Indexes() []ent.Index {
	return []ent.Index{
		// 单列索引：按 name 快速查询（全局或模糊搜索场景）
		index.Fields("name").
			StorageKey("idx_sandbox_name"),

		// 复合唯一索引：同一个租户下，deployment_name 不能重复
		index.Fields("tenant_name", "deployment_name").
			Unique(). // 加上 Unique()
			StorageKey("uk_tenant_deployment"),
	}
}
