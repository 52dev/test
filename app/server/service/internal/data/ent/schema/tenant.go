package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/tx7do/go-crud/entgo/mixin"
)

// Tenant holds the schema definition for the Tenant entity.
type Tenant struct {
	ent.Schema
}

func (Tenant) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{
			Table:     "tenants",
			Charset:   "utf8mb4",
			Collation: "utf8mb4_bin",
		},
		entsql.WithComments(true),
		schema.Comment("租户表"),
	}
}

// Fields of the User.
func (Tenant) Fields() []ent.Field {
	return []ent.Field{
		field.String("name").
			Comment("租户名").
			//Unique().
			NotEmpty().
			Immutable().
			Nillable(),
	}
}

// Mixin of the Sandbox.
func (Tenant) Mixin() []ent.Mixin {
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
func (Tenant) Indexes() []ent.Index {
	return []ent.Index{
		// 单列索引：按 name 快速查询（全局或模糊搜索场景）
		index.Fields("name").
			StorageKey("idx_tenant_name"),
	}
}
