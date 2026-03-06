package biz

import (
	"context"
	"fmt"

	pb "go-wind-admin/api/gen/go/tenant/service/v1"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	// Kueue 官方客户端
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueclientset "sigs.k8s.io/kueue/client-go/clientset/versioned"
)

type KueueController struct {
	k8sClient   kubernetes.Interface
	kueueClient kueueclientset.Interface
	log         *log.Helper
}

func NewKueueController(ctx *bootstrap.Context, k8s kubernetes.Interface, kueue kueueclientset.Interface) *KueueController {
	return &KueueController{
		k8sClient:   k8s,
		kueueClient: kueue,
		log:         ctx.NewLoggerHelper("server/biz/kueue-controller"),
	}
}

// --- 1. 全局资源 (Flavor) ---

func (c *KueueController) CreateFlavor(ctx context.Context, req *pb.CreateFlavorReq) error {
	var taints []corev1.Taint
	for k, v := range req.Taints {
		taints = append(taints, corev1.Taint{
			Key:    k,
			Value:  v,
			Effect: corev1.TaintEffectNoSchedule,
		})
	}

	flavor := &kueuev1beta2.ResourceFlavor{
		ObjectMeta: metav1.ObjectMeta{Name: req.Name},
		Spec: kueuev1beta2.ResourceFlavorSpec{
			NodeLabels: req.Labels,
			NodeTaints: taints,
		},
	}

	_, err := c.kueueClient.KueueV1beta2().ResourceFlavors().Create(ctx, flavor, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (c *KueueController) GetFlavor(ctx context.Context, name string) (*pb.FlavorInfo, error) {
	data, err := c.kueueClient.KueueV1beta2().ResourceFlavors().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	res := &pb.FlavorInfo{
		Name:   data.Name,
		Labels: data.Spec.NodeLabels,
	}
	return res, nil
}

func (c *KueueController) ListFlavors(ctx context.Context) ([]*pb.FlavorInfo, error) {
	list, err := c.kueueClient.KueueV1beta2().ResourceFlavors().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var res []*pb.FlavorInfo
	for _, item := range list.Items {
		res = append(res, &pb.FlavorInfo{
			Name:   item.Name,
			Labels: item.Spec.NodeLabels,
		})
	}
	return res, nil
}

// --- 2. 租户管理 (Tenant) ---

func (c *KueueController) CreateTenantEnv(ctx context.Context, tenantName, displayName string, quota *pb.TenantQuota) error {
	// 1. Namespace
	nsObj, err := c.k8sClient.CoreV1().Namespaces().Get(ctx, tenantName, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: tenantName,
				Labels: map[string]string{
					"wind.io/managed-by": "tenant-service",
				},
				Annotations: map[string]string{
					"wind.io/display-name": displayName,
				},
			},
		}
		if _, err := c.k8sClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create namespace failed: %v", err)
		}
	} else if err == nil {
		// Update 场景: 更新 DisplayName (如果需要)
		if displayName != "" && nsObj.Annotations["wind.io/display-name"] != displayName {
			nsObj.Annotations["wind.io/display-name"] = displayName
			_, _ = c.k8sClient.CoreV1().Namespaces().Update(ctx, nsObj, metav1.UpdateOptions{})
		}
	} else {
		return err
	}

	// 2. ClusterQueue (专属配额 + 隔离 Cohort)
	cqName := fmt.Sprintf("cq-%s", tenantName)
	cohortName := fmt.Sprintf("cohort-%s", tenantName) // 硬隔离
	if err := c.applyClusterQueue(ctx, cqName, cohortName, tenantName, quota); err != nil {
		return err
	}

	// 3. LocalQueue
	if err := c.applyLocalQueue(ctx, tenantName, "default", cqName); err != nil {
		return err
	}

	return nil
}

func (c *KueueController) DeleteTenantEnv(ctx context.Context, tenantName string) error {
	// 删除 Namespace 会级联删除 LocalQueue 和 Deployment，但 ClusterQueue 需要手动删除
	cqName := fmt.Sprintf("cq-%s", tenantName)
	if err := c.kueueClient.KueueV1beta2().ClusterQueues().Delete(ctx, cqName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete clusterqueue failed: %v", err)
	}

	// 删除 Namespace
	if err := c.k8sClient.CoreV1().Namespaces().Delete(ctx, tenantName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete namespace failed: %v", err)
	}
	return nil
}

func (c *KueueController) applyClusterQueue(ctx context.Context, name, cohort, namespace string, quota *pb.TenantQuota) error {
	// 1. 第一步：收集所有出现过的资源类型，构建全量集合
	// 同时为了避免多次遍历，先用 map 暂存 quota 数据
	resourceSet := make(map[corev1.ResourceName]bool)
	// 确保基础资源始终存在
	resourceSet[corev1.ResourceCPU] = true
	resourceSet[corev1.ResourceMemory] = true

	// map[flavorName] -> map[resourceName] -> quantity
	flavorQuotaMap := make(map[string]map[corev1.ResourceName]resource.Quantity)

	for _, fQuota := range quota.Flavors {
		if _, exists := flavorQuotaMap[fQuota.FlavorName]; !exists {
			flavorQuotaMap[fQuota.FlavorName] = make(map[corev1.ResourceName]resource.Quantity)
		}
		for _, r := range fQuota.Resources {
			rName := corev1.ResourceName(r.Name)
			q := resource.MustParse(r.NominalQuota)
			flavorQuotaMap[fQuota.FlavorName][rName] = q
			resourceSet[rName] = true
		}
	}

	// 2. 构建 coveredResources 列表 (固定顺序以保证一致性)
	var coveredResources []corev1.ResourceName
	for rName := range resourceSet {
		coveredResources = append(coveredResources, rName)
	}

	// 3. 构建 Flavors，对缺失资源进行 "补0" 操作
	var flavorQuotas []kueuev1beta2.FlavorQuotas

	// 遍历用户请求中的 Flavor 顺序
	for _, fQuota := range quota.Flavors {
		fName := fQuota.FlavorName
		rMap := flavorQuotaMap[fName]
		var flavorResources []kueuev1beta2.ResourceQuota

		// 关键修正：必须按照 coveredResources 的顺序，一个个检查
		for _, coveredRes := range coveredResources {
			var q resource.Quantity
			if val, ok := rMap[coveredRes]; ok {
				q = val
			} else {
				// 如果该 Flavor 没有定义这个资源，显式设为 0
				q = resource.MustParse("0")
			}

			flavorResources = append(flavorResources, kueuev1beta2.ResourceQuota{
				Name:         coveredRes,
				NominalQuota: q,
			})
		}

		flavorQuotas = append(flavorQuotas, kueuev1beta2.FlavorQuotas{
			Name:      kueuev1beta2.ResourceFlavorReference(fName),
			Resources: flavorResources,
		})
	}

	// 4. 构建 ResourceGroup
	resourceGroups := []kueuev1beta2.ResourceGroup{
		{
			CoveredResources: coveredResources,
			Flavors:          flavorQuotas,
		},
	}

	cqSpec := kueuev1beta2.ClusterQueueSpec{
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"kubernetes.io/metadata.name": namespace,
			},
		},
		CohortName:     kueuev1beta2.CohortReference(cohort),
		ResourceGroups: resourceGroups,
	}

	// ... (后续创建/更新逻辑不变) ...
	cq := &kueuev1beta2.ClusterQueue{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"wind.io/tenant": namespace,
			},
		},
		Spec: cqSpec,
	}

	existing, err := c.kueueClient.KueueV1beta2().ClusterQueues().Get(ctx, name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		_, err = c.kueueClient.KueueV1beta2().ClusterQueues().Create(ctx, cq, metav1.CreateOptions{})
		return err
	} else if err != nil {
		return err
	}

	// Update
	existing.Spec = cqSpec
	existing.Labels = cq.Labels
	_, err = c.kueueClient.KueueV1beta2().ClusterQueues().Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

func (c *KueueController) applyLocalQueue(ctx context.Context, ns, name, cqName string) error {
	lq := &kueuev1beta2.LocalQueue{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: kueuev1beta2.LocalQueueSpec{
			ClusterQueue: kueuev1beta2.ClusterQueueReference(cqName),
		},
	}
	_, err := c.kueueClient.KueueV1beta2().LocalQueues(ns).Create(ctx, lq, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// GetTenantDetail 获取详情
func (c *KueueController) GetTenantDetail(ctx context.Context, tenantName string) (*pb.TenantDetailResp, error) {
	nsObj, err := c.k8sClient.CoreV1().Namespaces().Get(ctx, tenantName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("tenant namespace not found: %v", err)
	}

	cqName := fmt.Sprintf("cq-%s", tenantName)
	cq, err := c.kueueClient.KueueV1beta2().ClusterQueues().Get(ctx, cqName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	resp := &pb.TenantDetailResp{
		TenantName:   tenantName,
		DisplayName:  nsObj.Annotations["wind.io/display-name"],
		Namespace:    tenantName,
		Cohort:       string(cq.Spec.CohortName),
		NominalQuota: &pb.TenantQuota{Flavors: []*pb.FlavorQuota{}},
		UsedQuota:    &pb.TenantQuota{Flavors: []*pb.FlavorQuota{}},
	}

	// Spec (Nominal)
	for _, g := range cq.Spec.ResourceGroups {
		for _, f := range g.Flavors {
			fq := &pb.FlavorQuota{FlavorName: string(f.Name)}
			for _, r := range f.Resources {
				fq.Resources = append(fq.Resources, &pb.ResourceLimit{
					Name:         string(r.Name),
					NominalQuota: r.NominalQuota.String(),
				})
			}
			resp.NominalQuota.Flavors = append(resp.NominalQuota.Flavors, fq)
		}
	}

	// Status (Used)
	for _, f := range cq.Status.FlavorsUsage {
		fq := &pb.FlavorQuota{FlavorName: string(f.Name)}
		for _, r := range f.Resources {
			fq.Resources = append(fq.Resources, &pb.ResourceLimit{
				Name:         string(r.Name),
				NominalQuota: r.Total.String(), // Total = Used
			})
		}
		resp.UsedQuota.Flavors = append(resp.UsedQuota.Flavors, fq)
	}

	return resp, nil
}

// --- 3. 核心配额预检逻辑 (Soft Check) ---

func (c *KueueController) CheckQuotaSufficient(ctx context.Context, req *pb.CheckQuotaReq) (bool, string, error) {
	// 1. 获取最新详情
	detail, err := c.GetTenantDetail(ctx, req.TenantName)
	if err != nil {
		return false, "", err
	}

	quota := req.Quota
	// 2. 解析需求
	// 防止 panic, MustParse 需确保输入合法，这里假设 Service 层已校验
	// ���者使用 ParseQuantity
	needCpu, err := resource.ParseQuantity(quota.Cpu)
	if err != nil {
		return false, "invalid cpu format", nil
	}

	needMem, err := resource.ParseQuantity(quota.Memory)
	if err != nil {
		return false, "invalid memory format", nil
	}

	needStorage, err := resource.ParseQuantity(quota.Storage)
	if err != nil {
		return false, "invalid storage format", nil
	}

	needGpu := resource.MustParse("0")
	if quota.Gpu != "" {
		if q, err := resource.ParseQuantity(quota.Gpu); err == nil {
			needGpu = q
		}
	}

	// 3. 检查所有 Flavor
	// 逻辑：如果租户配置了多个 Flavor，这里简化为"任一 Flavor 满足即可" 或者 "检查默认 Flavor"
	// 在 Kueue 中，如果未指定 Flavor，任务可能会 Pending 直到适合。
	// 这里我们遍历所有 Flavor，只要发现配额不够就报错 (严谨模式)
	// 或者：我们需要看 ClusterQueue 是否真的还有剩余。

	for _, limitFlavor := range detail.NominalQuota.Flavors {
		// 查找对应的 Used
		var usedFlavor *pb.FlavorQuota
		for _, u := range detail.UsedQuota.Flavors {
			if u.FlavorName == req.FlavorName {
				usedFlavor = u
				break
			}
		}
		if limitFlavor.FlavorName != req.FlavorName {
			continue
		}
		// 检查各项资源
		if reason := checkSingleResource(limitFlavor, usedFlavor, "cpu", needCpu); reason != "" {
			return false, reason, nil
		}
		if reason := checkSingleResource(limitFlavor, usedFlavor, "memory", needMem); reason != "" {
			return false, reason, nil
		}
		if reason := checkSingleResource(limitFlavor, usedFlavor, "ephemeral-storage", needStorage); reason != "" {
			return false, reason, nil
		}
		if !needGpu.IsZero() {
			if reason := checkSingleResource(limitFlavor, usedFlavor, "nvidia.com/gpu", needGpu); reason != "" {
				return false, reason, nil
			}
		}
	}

	return true, "sufficient", nil
}

func checkSingleResource(limit *pb.FlavorQuota, used *pb.FlavorQuota, resName string, need resource.Quantity) string {
	// 1. 获取上限
	limitQ := resource.MustParse("0")
	foundLimit := false
	for _, r := range limit.Resources {
		if r.Name == resName {
			limitQ = resource.MustParse(r.NominalQuota)
			foundLimit = true
			break
		}
	}
	if !foundLimit {
		// 如果租户没配这个资源的额度（比如没配 GPU），但用户请求了 GPU
		if !need.IsZero() {
			return fmt.Sprintf("Quota not defined for %s", resName)
		}
		return "" // 没配也没请求，通过
	}

	// 2. 获取已用
	usedQ := resource.MustParse("0")
	if used != nil {
		for _, r := range used.Resources {
			if r.Name == resName {
				usedQ = resource.MustParse(r.NominalQuota) // UsedQuota 存在这字段里
				break
			}
		}
	}

	// 3. 计算: (Used + Need) > Limit ?
	// usedQ.Add(need) 会修改 usedQ 本身
	newUsed := usedQ.DeepCopy()
	newUsed.Add(need)

	if limitQ.Cmp(newUsed) < 0 {
		return fmt.Sprintf("Insufficient %s: limit=%s, used=%s, req=%s",
			resName, limitQ.String(), usedQ.String(), need.String())
	}
	return ""
}
