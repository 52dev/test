package schema

import (
	"regexp"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/google/uuid"
	"github.com/tx7do/go-crud/entgo/mixin"
)

// Model holds the schema definition for the Model entity.
type Model struct {
	ent.Schema
}

func (Model) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{
			Table:     "models",
			Charset:   "utf8mb4",
			Collation: "utf8mb4_bin",
		},
		entsql.WithComments(true),
		schema.Comment("模型表"),
	}
}

// Fields of the Model.
func (Model) Fields() []ent.Field {
	return []ent.Field{
		field.String("name").
			Comment("模型名称").
			NotEmpty().
			MaxLen(255).
			Immutable().
			Unique().
			Nillable().
			Optional(),

		field.String("slug").
			Comment("仓库组名称").
			Optional().
			NotEmpty().
			Nillable().
			Unique(),

		field.String("docker_image_ref").
			Comment("模型的镜像地址, 如 nvcr.io/nvidia/llama2:latest").
			Optional().
			Match(regexp.MustCompile(`^([a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*(:[0-9]+)?/)?[a-z0-9._-]+(/[a-z0-9._-]+)*(:[a-zA-Z0-9._-]+)?(@sha256:[a-fA-F0-9]{64})?$`)).
			Nillable(),

		field.Strings("jupyter_command").
			Comment("命令行参数").
			Default([]string{}).
			Optional(),

		field.Strings("vllm_command").
			Comment("命令行参数").
			Default([]string{}).
			Optional(),

		field.JSON("command_mapping", map[string]string{}).
			Comment("命令映射").
			Default(map[string]string{}).
			Optional(),

		field.JSON("env", map[string]string{}).
			Comment("环境变量").
			Default(map[string]string{}).
			Optional(),

		field.Ints("ports").
			Comment("暴露的端口号").
			Default([]int{}).
			Optional(),

		field.Strings("writable_paths").
			Comment("可写路径").
			Default([]string{}).
			Optional(),

		field.String("base_framework").
			Comment("基础框架，如 pytorch, tensorflow, vllm").
			Optional().
			Nillable(),

		field.Enum("type").
			Comment("模型类型").
			Optional().
			Nillable().
			Default("CUSTOM").
			NamedValues(
				"Custom", "CUSTOM",
				"Nim", "NIM",
				"Hf", "HF",
				"OnlyImage", "ONLY_IMAGE",
			),

		field.Enum("status").
			Comment("模型状态").
			Optional().
			Nillable().
			Default("NOT_CACHED").
			NamedValues(
				"NotCached", "NOT_CACHED",
				"Caching", "CACHING",
				"Ready", "READY",
			),

		field.Text("readme_text").
			Comment("Model Card 内容，支持 Markdown").
			Optional().
			Nillable(),

		field.Int64("downloads").
			Comment("下载次数").
			Optional().
			Default(0).
			NonNegative(),

		field.Int64("likes").
			Comment("点赞数").
			Optional().
			Default(0).
			NonNegative(),

		field.String("license").
			Comment("许可证，如 MIT, Apache-2.0, Llama-Community").
			Default("MIT").
			Optional().
			Nillable(),

		field.Int64("model_size").
			Comment("模型文件大小（字节）").
			Optional().
			Nillable().
			NonNegative(),

		field.UUID("uuid", uuid.UUID{}).
			Comment("UUID").
			Optional().
			Nillable().
			Default(uuid.New).
			Immutable().
			Unique(),
	}
}

// Mixin of the Model.
func (Model) Mixin() []ent.Mixin {
	return []ent.Mixin{
		mixin.AutoIncrementId64{},
		mixin.TimeAt{},
		mixin.Remark{},
		mixin.Description{},
		mixin.Tag{},
		mixin.IsEnabled{},
	}
}

// Indexes of the Model.
func (Model) Indexes() []ent.Index {
	return []ent.Index{
		// 单列索引：按 name 快速查询（全局或模糊搜索场景）
		index.Fields("name").
			StorageKey("idx_models_name"),
	}
}
