package monitor

import (
	"fmt"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"

	"sort"
)

// Generate globally unique TokenID
func GenTokenID() string {
	return uuid.New().String()
}

// --- Key Generators for Etcd ---
// 格式统一为: /wind/resource/v1/{type}/{subtype}/{id}

// KeyLedger: /wind/resource/v1/ledger/{resType}/{levelID}
func KeyLedger(resType ResourceType, levelID string) string {
	return fmt.Sprintf("/wind/resource/v1/ledger/%s/%s", resType, levelID)
}

// KeyLock: /wind/resource/v1/lock/{resType}/{levelID}
func KeyLock(resType ResourceType, levelID string) string {
	return fmt.Sprintf("/wind/resource/v1/lock/%s/%s", resType, levelID)
}

// KeyToken: /wind/resource/v1/token/{tokenID}
func KeyToken(tokenID string) string {
	return fmt.Sprintf("/wind/resource/v1/token/%s", tokenID)
}

// SortLevelIDs ensures deadlock-free locking order
func SortLevelIDs(ids []string) []string {
	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sort.Strings(sorted)
	return sorted
}

func GetWorkerNodeLSelector() labels.Selector {
	selector := labels.NewSelector()
	var reqs []labels.Requirement
	if req, err := labels.NewRequirement("node-role.kubernetes.io/master", selection.DoesNotExist, nil); err == nil {
		reqs = append(reqs, *req)
	}
	if req, err := labels.NewRequirement("node-role.kubernetes.io/control-plane", selection.DoesNotExist, nil); err == nil {
		reqs = append(reqs, *req)
	}
	selector = selector.Add(reqs...)
	return selector
}

// 辅助函数：判断是否为 Worker 节点
func IsWorkerNode(node *corev1.Node) bool {
	// 1. 检查 Label
	if _, isMaster := node.Labels["node-role.kubernetes.io/master"]; isMaster {
		return false
	}
	if _, isControlPlane := node.Labels["node-role.kubernetes.io/control-plane"]; isControlPlane {
		return false
	}

	// 2. 检查 Taint (更严格)
	// 很多 Master 节点带有 NoSchedule 的 Taint，即使没有 Role Label 也不应该调度
	for _, taint := range node.Spec.Taints {
		if taint.Key == "node-role.kubernetes.io/master" || taint.Key == "node-role.kubernetes.io/control-plane" {
			if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
				return false
			}
		}
	}

	return true
}

// KeyIndexLevel: /wind/resource/v1/index/level/{levelID}/{tokenID}
func KeyIndexLevel(levelID, tokenID string) string {
	return fmt.Sprintf("/wind/resource/v1/index/level/%s/%s", levelID, tokenID)
}
