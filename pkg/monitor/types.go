package monitor

import (
	resourcev1 "go-wind-admin/api/gen/go/k8s/service/v1"
	"time"
)

type ResourceType string

const (
	ResourceTypeCPU     ResourceType = "cpu"
	ResourceTypeGPU     ResourceType = "gpu"
	ResourceTypeMemory  ResourceType = "memory"
	ResourceTypeStorage ResourceType = "storage"
)

type TokenStatus int32

const (
	TokenStatusInit           TokenStatus = 0
	TokenStatusPendingConfirm TokenStatus = 1
	TokenStatusAllocated      TokenStatus = 2
	TokenStatusReleased       TokenStatus = 3
	TokenStatusExpired        TokenStatus = 4
	TokenStatusRollbacked     TokenStatus = 5
)

func (s TokenStatus) IsFinal() bool {
	return s == TokenStatusReleased || s == TokenStatusExpired || s == TokenStatusRollbacked
}

// 核心升级：Token 包含多个 Item
type ResourceToken struct {
	TokenID        string      `json:"token_id"`
	Items          []TokenItem `json:"items"` // 列表
	TokenStatus    TokenStatus `json:"token_status"`
	ConfirmTimeout int64       `json:"confirm_timeout"`
	ExpireAt       int64       `json:"expire_at"`
	CreateAt       time.Time   `json:"create_at"`
	UpdateAt       time.Time   `json:"update_at"`
	LeaseID        int64       `json:"lease_id"`
}

type TokenItem struct {
	ResourceType ResourceType `json:"resource_type"`
	AllocateNum  int64        `json:"allocate_num"`
	UsedNum      int64        `json:"used_num"`
	LevelIDs     []string     `json:"level_ids"`
}

type ResourceLedger struct {
	Total int64 `json:"total"`
	Used  int64 `json:"used"`
}

func FromProtoType(t resourcev1.ResourceType) ResourceType {
	switch t {
	case resourcev1.ResourceType_RESOURCE_TYPE_CPU:
		return ResourceTypeCPU
	case resourcev1.ResourceType_RESOURCE_TYPE_GPU:
		return ResourceTypeGPU
	case resourcev1.ResourceType_RESOURCE_TYPE_MEMORY:
		return ResourceTypeMemory
	case resourcev1.ResourceType_RESOURCE_TYPE_STORAGE:
		return ResourceTypeStorage
	default:
		return "unknown"
	}
}
func ToProtoType(t ResourceType) resourcev1.ResourceType {
	switch t {
	case ResourceTypeCPU:
		return resourcev1.ResourceType_RESOURCE_TYPE_CPU
	case ResourceTypeGPU:
		return resourcev1.ResourceType_RESOURCE_TYPE_GPU
	case ResourceTypeMemory:
		return resourcev1.ResourceType_RESOURCE_TYPE_MEMORY
	case ResourceTypeStorage:
		return resourcev1.ResourceType_RESOURCE_TYPE_STORAGE
	default:
		return resourcev1.ResourceType_RESOURCE_TYPE_UNSPECIFIED
	}
}
