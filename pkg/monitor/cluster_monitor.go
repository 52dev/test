package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	resourcev1 "go-wind-admin/api/gen/go/k8s/service/v1"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type ClusterMonitor struct {
	Name       string
	client     kubernetes.Interface
	manager    *MonitorManager
	stopCh     chan struct{}
	wg         sync.WaitGroup
	nodeLister listersv1.NodeLister
	synced     bool
	syncMu     sync.RWMutex

	// === 新增：健康状态追踪 ===
	lastHeartbeat time.Time
	healthMu      sync.RWMutex
	lastError     error
}

func NewClusterMonitor(name string, client kubernetes.Interface, manager *MonitorManager) *ClusterMonitor {
	return &ClusterMonitor{
		Name:    name,
		client:  client,
		manager: manager,
		stopCh:  make(chan struct{}),
	}
}

func (cm *ClusterMonitor) InitInformer() {
	factory := informers.NewSharedInformerFactory(cm.client, 10*time.Minute)
	cm.nodeLister = factory.Core().V1().Nodes().Lister()

	factory.Core().V1().Nodes().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { cm.syncTotalCapacity() },
		UpdateFunc: func(old, new interface{}) { cm.syncTotalCapacity() },
		DeleteFunc: func(obj interface{}) { cm.syncTotalCapacity() },
	})

	go factory.Start(cm.stopCh)

	// 修复：Informer 同步检查
	go func() {
		if cache.WaitForCacheSync(cm.stopCh, factory.Core().V1().Nodes().Informer().HasSynced) {
			cm.syncMu.Lock()
			cm.synced = true
			cm.syncMu.Unlock()
			// 初始同步一次物理容量
			cm.syncTotalCapacity()
		}
	}()
}

func (cm *ClusterMonitor) isSynced() bool {
	cm.syncMu.RLock()
	defer cm.syncMu.RUnlock()
	return cm.synced
}

func (cm *ClusterMonitor) Start() {
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		ticker := time.NewTicker(1 * time.Minute)
		for {
			select {
			case <-cm.stopCh:
				return
			case <-ticker.C:
				cm.syncTotalCapacity()
			}
		}
	}()
	// === 新增：启动心跳检查 ===
	cm.wg.Add(1)
	go cm.runHeartbeat()
}

func (cm *ClusterMonitor) runHeartbeat() {
	defer cm.wg.Done()
	// 频率高一点，以便及时发现断连
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			// 尝试做一个轻量级请求，例如获取 kube-system 命名空间
			// 或者直接用 client.Discovery().RESTClient().Get().AbsPath("/healthz").Do(context.TODO())
			_, err := cm.client.CoreV1().Namespaces().Get(context.Background(), "kube-system", metav1.GetOptions{})

			cm.healthMu.Lock()
			if err != nil {
				cm.lastError = err
			} else {
				cm.lastHeartbeat = time.Now()
				cm.lastError = nil
			}
			cm.healthMu.Unlock()
		}
	}
}

func (cm *ClusterMonitor) Stop() {
	close(cm.stopCh)
	cm.wg.Wait()
}

// syncTotalCapacity 计算总物理容量并更新到 Etcd Ledger (Total 字段)
func (cm *ClusterMonitor) syncTotalCapacity() {
	if !cm.isSynced() || cm.manager.etcdClient == nil {
		return
	}

	nodes, err := cm.nodeLister.List(GetWorkerNodeLSelector())
	if err != nil {
		return
	}

	var totalCPU, totalGPU int64
	for _, node := range nodes {
		// === 新增过滤 ===
		if !IsWorkerNode(node) {
			continue
		}

		cpu := node.Status.Capacity.Cpu().MilliValue()
		totalCPU += cpu / 1000
		if gpu, ok := node.Status.Capacity["nvidia.com/gpu"]; ok {
			totalGPU += gpu.Value()
		}
	}

	ctx := context.Background()
	levelID := fmt.Sprintf("physical_%s", cm.Name)
	// ... (Helper updateLedgerTotal 保持不变)
	updateLedgerTotal := func(resType ResourceType, total int64) {
		key := KeyLedger(resType, levelID)
		resp, err := cm.manager.etcdClient.Get(ctx, key)
		if err != nil {
			return
		}
		var ledger ResourceLedger
		if len(resp.Kvs) > 0 {
			json.Unmarshal(resp.Kvs[0].Value, &ledger)
		}
		if ledger.Total == total {
			return
		}
		ledger.Total = total
		valBytes, _ := json.Marshal(ledger)
		cm.manager.etcdClient.Put(ctx, key, string(valBytes))
	}
	updateLedgerTotal(ResourceTypeCPU, totalCPU)
	updateLedgerTotal(ResourceTypeGPU, totalGPU)
}

// --- 新增 Topology 实现方法 ---

func (cm *ClusterMonitor) GetSummary() *resourcev1.ListClustersResponse_ClusterSummary {
	// 1. 检查是否初始化完成
	if !cm.isSynced() {
		return &resourcev1.ListClustersResponse_ClusterSummary{ClusterId: cm.Name, Name: cm.Name, Status: "Initializing"}
	}

	// 2. === 新增：检查心跳状态 ===
	cm.healthMu.RLock()
	lastBeat := cm.lastHeartbeat
	lastErr := cm.lastError
	cm.healthMu.RUnlock()

	status := "Healthy"
	// 如果超过 30秒 没心跳，或者有报错，视为不健康
	if time.Since(lastBeat) > 30*time.Second {
		status = "Unreachable"
		if lastErr != nil {
			// 可以选择把错误信息打 log 或者拼接到 Name 里用于调试
			// status = fmt.Sprintf("Error: %v", lastErr)
		}
	}

	nodes, err := cm.nodeLister.List(GetWorkerNodeLSelector())
	if err != nil {
		return &resourcev1.ListClustersResponse_ClusterSummary{ClusterId: cm.Name, Status: "ListerError"}
	}

	summary := &resourcev1.ListClustersResponse_ClusterSummary{
		ClusterId: cm.Name,
		Name:      cm.Name,
		Status:    status, // 使用动态计算的状态
		NodeCount: int32(len(nodes)),
	}

	// 如果不健康，可能不需要返回资源数值，或者返回缓存值但标记状态
	if status == "Healthy" {
		for _, n := range nodes {
			// === 新增过滤 (你之前的代码里已经有了，这里确保加上) ===
			if !IsWorkerNode(n) {
				continue
			}

			summary.TotalCpu += n.Status.Capacity.Cpu().Value()
			if g, ok := n.Status.Capacity["nvidia.com/gpu"]; ok {
				summary.TotalGpu += g.Value()
			}
		}
	}

	return summary
}

func (cm *ClusterMonitor) ListNodes(selector string) ([]*resourcev1.ListClusterNodesResponse_NodeSummary, error) {
	if !cm.isSynced() {
		return nil, fmt.Errorf("cluster initializing")
	}

	sel := GetWorkerNodeLSelector()
	if selector != "" {
		var err error
		sel, err = labels.Parse(selector)
		if err != nil {
			return nil, err
		}
	}

	nodes, err := cm.nodeLister.List(sel)
	if err != nil {
		return nil, err
	}

	res := make([]*resourcev1.ListClusterNodesResponse_NodeSummary, 0, len(nodes))
	for _, n := range nodes {
		status := "NotReady"
		for _, cond := range n.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				status = "Ready"
				break
			}
		}
		ip := ""
		for _, addr := range n.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				ip = addr.Address
				break
			}
		}
		gpuCount := int64(0)
		if g, ok := n.Status.Capacity["nvidia.com/gpu"]; ok {
			gpuCount = g.Value()
		}

		res = append(res, &resourcev1.ListClusterNodesResponse_NodeSummary{
			NodeName: n.Name, Status: status, Ip: ip,
			CpuModel: n.Labels["feature.node.kubernetes.io/cpu-model.vendor_id"],
			GpuModel: n.Labels["nvidia.com/gpu.product"],
			TotalCpu: n.Status.Capacity.Cpu().Value(), TotalMemoryGib: n.Status.Capacity.Memory().Value() / (1 << 30), TotalGpu: gpuCount,
		})
	}
	return res, nil
}

func (cm *ClusterMonitor) GetNodeDetail(nodeName string) (*resourcev1.GetNodeDetailResponse, error) {
	if !cm.isSynced() {
		return nil, fmt.Errorf("cluster initializing")
	}

	n, err := cm.nodeLister.Get(nodeName)
	if err != nil {
		return nil, err
	}

	detail := &resourcev1.GetNodeDetailResponse{
		NodeName: n.Name, Labels: n.Labels, Annotations: n.Annotations,
		CapacityCpu: n.Status.Capacity.Cpu().Value(), AllocatableCpu: n.Status.Allocatable.Cpu().Value(),
		CapacityMemory: n.Status.Capacity.Memory().Value(), AllocatableMemory: n.Status.Allocatable.Memory().Value(),
		Gpus: make([]*resourcev1.GpuDetail, 0),
	}

	// 修复：生产级 GPU 显存映射
	if g, ok := n.Status.Capacity["nvidia.com/gpu"]; ok && g.Value() > 0 {
		count := int(g.Value())
		model := n.Labels["nvidia.com/gpu.product"]

		// 静态映射表，生产建议配置化
		memMap := map[string]int64{
			"NVIDIA A100-PCIE-40GB": 40960,
			"NVIDIA A10":            24576,
			"Tesla T4":              15360,
		}
		mem := memMap[model]
		if mem == 0 {
			mem = 1024
		} // 默认值

		for i := 0; i < count; i++ {
			detail.Gpus = append(detail.Gpus, &resourcev1.GpuDetail{
				Index: int32(i), ProductName: model, MemoryMib: mem,
			})
		}
	}
	return detail, nil
}

// FindBestNode 简单的 Bin-Packing 调度建议
func (cm *ClusterMonitor) FindBestNode(req map[ResourceType]int64) (string, error) {
	if !cm.isSynced() {
		return "", fmt.Errorf("not synced")
	}

	nodes, err := cm.nodeLister.List(GetWorkerNodeLSelector())
	if err != nil {
		return "", err
	}

	// 简单策略：First Fit (找到第一个能放下的)

	for _, n := range nodes {
		ready := false
		for _, cond := range n.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				ready = true
				break
			}
		}
		if !ready || n.Spec.Unschedulable {
			continue
		}

		// Check CPU
		cpuReq := req[ResourceTypeCPU]
		if cpuReq > 0 {
			// 提示：Allocatable 是静态值，未扣减 Pod Request。
			// 若要更精准，需遍历 Pods 计算剩余。鉴于这里是 Hint，用 Allocatable 做初步筛选尚可。
			allocatable := n.Status.Allocatable.Cpu().MilliValue() / 1000
			if allocatable < cpuReq {
				continue
			}
		}

		// Check GPU
		gpuReq := req[ResourceTypeGPU]
		if gpuReq > 0 {
			if g, ok := n.Status.Allocatable["nvidia.com/gpu"]; !ok || g.Value() < gpuReq {
				continue
			}
		}

		// Check Memory
		memReq := req[ResourceTypeMemory]
		if memReq > 0 {
			allocatableGiB := n.Status.Allocatable.Memory().Value() / (1 << 30)
			if allocatableGiB < memReq {
				continue
			}
		}

		return n.Name, nil
	}

	return "", fmt.Errorf("no suitable node found")
}
