package biz

import (
	"context"
	"fmt"
	pb "go-wind-admin/api/gen/go/k8s/service/v1"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2" // 引入 HPA v2
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

var (
	HigressAPIGroup         = "networking.higress.io" // Higress API Group
	DefaultIngressClassName = "higress"
)

// -------------------------- 1. 抽离常量，解耦硬编码 --------------------------
const (
	// Higress相关常量（可放到配置文件/环境变量中）
	HigressDefaultNamespace = "higress-system" // Higress默认部署的命名空间
	HigressMcpBridgeName    = "default"        // 默认McpBridge名称
	DefaultJupyterPortName  = "jupyter"        // 默认要匹配的端口名称（比如Jupyter的8888端口命名为jupyter）
	DefaultJupyterPort      = 8888

	DefaultBaseDomain = "52op.us.ci"
	DefaultDomainPort = 32473
	// Higress注解常量
	AnnotationDestination    = "higress.io/destination"
	AnnotationIgnorePathCase = "higress.io/ignore-path-case"
	AnnotationSSLRedirect    = "higress.io/ssl-redirect"

	// --- S3 CSI 常量配置 ---
	S3StorageClassName = "s3-storage"                                           // StorageClass 名称
	S3DriverName       = "ru.yandex.s3.csi"                                     // CSI Driver 名称
	S3SecretName       = "csi-s3-secret"                                        // S3 认证 Secret 名称 (需预先在 default/租户 ns 创建)
	S3SecretNamespace  = "default"                                              // Secret 所在命名空间
	S3Mounter          = "geesefs"                                              // 挂载工具 (geesefs/rclone/s3fs)
	S3MountOptions     = "--memory-limit 1000 --dir-mode 0777 --file-mode 0666" // 挂载参数
)

// PortSelector 端口选择器（解决多端口问题）
// 优先按Name匹配，Name为空则按Port匹配，都不匹配则用默认端口名称/第一个端口
type PortSelector struct {
	Name string // 端口名称（推荐用名称，比端口号更稳定）
	Port int32  // 端口号（备用）
}

type AppController struct {
	k8sClient kubernetes.Interface
	log       *log.Helper
}

type Application struct {
	Deployment *appsv1.Deployment
	Service    *corev1.Service
	Route      *networkingv1.Ingress
	Domain     *string
}

func NewAppController(ctx *bootstrap.Context, k8s kubernetes.Interface) *AppController {
	return &AppController{
		k8sClient: k8s,
		log:       ctx.NewLoggerHelper("server/biz/app-controller"),
	}
}

// Create 创建 App
func (c *AppController) Create(ctx context.Context, req *pb.CreateDeploymentReq) (*Application, error) {
	deployment, err := c.CreateDeployment(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create sandbox deployment: %w", err)
	}

	// 如果启用了 HPA，创建 HPA 对象
	if req.MinReplicas > 0 && req.MaxReplicas > 0 {
		if err := c.CreateHPA(ctx, req.TenantName, req.Name, req.MinReplicas, req.MaxReplicas); err != nil {
			// HPA 创建失败是否回滚 Deployment？暂时只 log error
			c.log.Errorf("failed to create HPA: %v", err)
		}
	}

	service, err := c.CreateService(ctx, req.TenantName, req.Name, req.Ports, deployment, req.Jupyter)
	if err != nil {
		return nil, fmt.Errorf("failed to create service for sandbox deployment: %w", err)
	}

	ingress, err := c.CreateRoute(ctx, deployment, service, DefaultBaseDomain, PortSelector{})
	if err != nil {
		return nil, fmt.Errorf("failed to create ingress route for sandbox deployment: %w", err)
	}
	domain := fmt.Sprintf("https://%s:%d", ingress.Name, DefaultDomainPort)
	app := &Application{
		Deployment: deployment,
		Service:    service,
		Route:      ingress,
		Domain:     &domain,
	}
	return app, nil
}

// CreateS3Volume 创建 S3 PV 和 PVC，并返回 Volume Source 用于挂载
func (c *AppController) CreateS3Volume(ctx context.Context, tenantName, deployName string, mountIdx int, s3Path string) (string, error) {
	// 命名规范: pv-<deployName>-<idx>, pvc-<deployName>-<idx>
	pvName := fmt.Sprintf("pv-%s-%d", deployName, mountIdx)
	pvcName := fmt.Sprintf("pvc-%s-%d", deployName, mountIdx)

	// 1. 构造 PV
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName, // PV 是集群级别的，Name 必须全局唯一
			Labels: map[string]string{
				"tenant":     tenantName,
				"deployment": deployName,
				"s3-mount":   "true",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"), // S3 无限容量，这里只是占位符
			},
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			StorageClassName: S3StorageClassName,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       S3DriverName,
					VolumeHandle: s3Path, // 核心：S3 Bucket + Prefix
					ReadOnly:     false,
					ControllerPublishSecretRef: &corev1.SecretReference{
						Name:      S3SecretName,
						Namespace: S3SecretNamespace,
					},
					NodeStageSecretRef: &corev1.SecretReference{
						Name:      S3SecretName,
						Namespace: S3SecretNamespace,
					},
					NodePublishSecretRef: &corev1.SecretReference{
						Name:      S3SecretName,
						Namespace: S3SecretNamespace,
					},
					VolumeAttributes: map[string]string{
						"mounter":  S3Mounter,
						"options":  S3MountOptions,
						"capacity": "1Gi",
					},
				},
			},
		},
	}

	// 2. 创建 PV (如果已存在则忽略)
	_, err := c.k8sClient.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return "", fmt.Errorf("failed to create PV %s: %w", pvName, err)
	}

	// 3. 构造 PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: tenantName,
			Labels: map[string]string{
				"deployment": deployName,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			StorageClassName: func() *string { s := S3StorageClassName; return &s }(),
			VolumeName:       pvName, // 显式绑定 PV
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// 4. 创建 PVC
	_, err = c.k8sClient.CoreV1().PersistentVolumeClaims(tenantName).Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return "", fmt.Errorf("failed to create PVC %s: %w", pvcName, err)
	}

	return pvcName, nil
}

func (c *AppController) CreateDeployment(ctx context.Context, req *pb.CreateDeploymentReq) (*appsv1.Deployment, error) {
	// 1. 准备资源配额对象
	reqResources := make(corev1.ResourceList)
	if req.Quota.Cpu != "" {
		q, err := resource.ParseQuantity(req.Quota.Cpu)
		if err == nil {
			reqResources[corev1.ResourceCPU] = q
		}
	}
	if req.Quota.Memory != "" {
		q, err := resource.ParseQuantity(req.Quota.Memory)
		if err == nil {
			reqResources[corev1.ResourceMemory] = q
		}
	}
	// Disk (Ephemeral Storage)
	if req.Quota.Storage != "" && req.Quota.Storage != "0" {
		if q, err := resource.ParseQuantity(req.Quota.Storage); err == nil {
			reqResources[corev1.ResourceEphemeralStorage] = q
		}
	}

	// GPU
	if req.Quota.Gpu != "" && req.Quota.Gpu != "0" {
		if q, err := resource.ParseQuantity(req.Quota.Gpu); err == nil {
			reqResources["nvidia.com/gpu"] = q
		}
	}

	// 2. 构造 Deployment
	queueName := "default"

	var ports []corev1.ContainerPort
	for _, port := range req.Ports {
		ports = append(ports, corev1.ContainerPort{
			ContainerPort: port,
		})
	}
	podLabels := map[string]string{
		"app":     "sandbox",
		"sandbox": req.Name,
	}
	if req.Jupyter {
		podLabels["jupyter"] = "true"
	}

	// 2. Deployment 标签
	deployLabels := make(map[string]string, len(podLabels)+1)
	for k, v := range podLabels {
		deployLabels[k] = v
	}
	deployLabels["kueue.x-k8s.io/queue-name"] = queueName

	// 1. 准备 Volume 和 Mount 切片
	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount

	// 1. 默认 /tmp
	volumes = append(volumes, corev1.Volume{
		Name:         "tmp-volume",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name: "tmp-volume", MountPath: "/tmp",
	})

	// 2. Writable Paths (EmptyDir)
	for i, path := range req.WritablePaths {
		volName := fmt.Sprintf("writable-vol-%d", i)
		volumes = append(volumes, corev1.Volume{
			Name:         volName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name: volName, MountPath: path,
		})
	}

	// 3. S3 Mounts (PV/PVC) - 新增逻辑
	for i, mount := range req.S3Mounts {
		// 调用 CreateS3Volume 创建基础设施
		pvcName, err := c.CreateS3Volume(ctx, req.TenantName, req.Name, i, mount.S3Path)
		if err != nil {
			return nil, err // 如果创建存储失败，直接返回错误，不继续创建 Deployment
		}

		volName := fmt.Sprintf("s3-vol-%d", i)

		// 添加 Volume (引用 PVC)
		volumes = append(volumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
					ReadOnly:  mount.ReadOnly,
				},
			},
		})

		// 添加 Mount
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      volName,
			MountPath: mount.MountPath,
			ReadOnly:  mount.ReadOnly,
			// MountPropagation: func() *corev1.MountPropagationMode { m := corev1.MountPropagationHostToContainer; return &m }(), // S3 挂载有时需要
		})
	}

	var env []corev1.EnvVar
	for key, value := range req.Env {
		env = append(env, corev1.EnvVar{
			Name:  key,
			Value: value,
		})
	}

	// 处理初始副本数
	replicas := req.Replicas
	if replicas <= 0 {
		replicas = 1
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: req.TenantName,
			Labels:    deployLabels,
		},
		Spec: appsv1.DeploymentSpec{

			Replicas: &replicas, // 使用传入的副本数
			Selector: &metav1.LabelSelector{
				MatchLabels: podLabels,
			},

			Template: corev1.PodTemplateSpec{

				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
				},
				Spec: corev1.PodSpec{
					NodeSelector: req.NodeSelector,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:    func() *int64 { i := int64(1000); return &i }(),
						RunAsGroup:   func() *int64 { i := int64(1000); return &i }(),
						RunAsNonRoot: func() *bool { b := true; return &b }(),
						FSGroup:      func() *int64 { i := int64(1000); return &i }(),
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Volumes: volumes,
					Containers: []corev1.Container{
						{
							Name:         "model-inference",
							Image:        req.Image,
							VolumeMounts: volumeMounts,
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: func() *bool { b := true; return &b }(), // S3 Fuse 有时需要特权
								Privileged:               func() *bool { b := true; return &b }(), // 注意：Fuse 挂载通常需要 Privileged: true 或特定的 Capabilities (SYS_ADMIN)
								// 如果不给 Privileged，geesefs 可能挂载失败。
								// 如果不想给特权，需要在 CSI 驱动层解决，或者使用 Sidecar 模式。
								// 这里的安全风险需评估。如果是受信任环境，Privileged OK。
								ReadOnlyRootFilesystem: func() *bool { b := true; return &b }(),
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
									Add:  []corev1.Capability{"SYS_ADMIN"}, // CSI Fuse 需要
								},
							},
							Env: env,
							Resources: corev1.ResourceRequirements{
								Requests: reqResources,
								Limits:   reqResources,
							},
							Command: req.CommandArgs,
							Ports:   ports,
						},
					},
				},
			},
		},
	}

	deployment, err := c.k8sClient.AppsV1().Deployments(req.TenantName).Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create sandbox deployment: %w", err)
	}
	return deployment, nil
}

// CreateHPA 创建 HorizontalPodAutoscaler
func (c *AppController) CreateHPA(ctx context.Context, namespace, deploymentName string, minReplicas, maxReplicas int32) error {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName, // 通常 HPA 与 Deployment 同名
			Namespace: namespace,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deploymentName,
			},
			MinReplicas: &minReplicas,
			MaxReplicas: maxReplicas,
			Metrics: []autoscalingv2.MetricSpec{
				// 默认根据 CPU 利用率 80% 扩容
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: corev1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: func() *int32 { i := int32(80); return &i }(),
						},
					},
				},
			},
		},
	}
	_, err := c.k8sClient.AutoscalingV2().HorizontalPodAutoscalers(namespace).Create(ctx, hpa, metav1.CreateOptions{})
	return err
}

func (c *AppController) CreateService(ctx context.Context, namespace string, name string, ports []int32, deployment *appsv1.Deployment, jupyter bool) (*corev1.Service, error) {
	// ... 保持原有逻辑不变 ...
	labels := deployment.Spec.Template.Labels
	var servicePorts []corev1.ServicePort
	for _, port := range ports {
		portName := fmt.Sprintf("port-%d", port)
		if jupyter && port == DefaultJupyterPort {
			portName = DefaultJupyterPortName
		}
		servicePorts = append(servicePorts, corev1.ServicePort{
			Name:       portName,
			Port:       port,
			TargetPort: intstr.FromInt32(port),
		})
	}
	service, err := c.k8sClient.CoreV1().Services(namespace).Create(ctx,
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("aims-sandbox-%s-service", name),
				Namespace: namespace,
				Labels:    labels,
			},
			Spec: corev1.ServiceSpec{
				Selector: labels,
				Ports:    servicePorts,
			},
		},
		metav1.CreateOptions{},
	)
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create service failed: %v", err)
	}
	return service, nil
}

func (c *AppController) CreateRoute(ctx context.Context, deploy *appsv1.Deployment, service *corev1.Service, baseDomain string, portSelector PortSelector) (*networkingv1.Ingress, error) {
	// ... 保持原有逻辑不变 ...
	// 省略重复代码，直接复制之前的 CreateRoute 实现
	// ...
	if deploy == nil || deploy.Name == "" {
		return nil, fmt.Errorf("deployment不能为空且名称不能为空")
	}
	if service == nil || service.Name == "" || service.Namespace == "" {
		return nil, fmt.Errorf("service不能为空且名称/命名空间不能为空")
	}
	targetPort, err := c.selectTargetPort(service, portSelector)
	if err != nil {
		return nil, err
	}
	sandboxDomain := fmt.Sprintf("%s.%s", deploy.Name, baseDomain)
	backendServiceAddr := fmt.Sprintf("%s.%s.svc.cluster.local:%d", service.Name, service.Namespace, targetPort)
	ingressAnnotations := map[string]string{
		AnnotationDestination:    backendServiceAddr,
		AnnotationIgnorePathCase: "false",
		AnnotationSSLRedirect:    "true",
	}
	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        sandboxDomain,
			Namespace:   HigressDefaultNamespace,
			Annotations: ingressAnnotations,
			Labels: map[string]string{
				"aims/type":    "sandbox",
				"aims/sandbox": deploy.Name,
				"aims/tenant":  deploy.Namespace,
			},
		},
		Spec: networkingv1.IngressSpec{
			IngressClassName: &DefaultIngressClassName,
			TLS: []networkingv1.IngressTLS{
				{
					Hosts:      []string{sandboxDomain},
					SecretName: baseDomain,
				},
			},
			Rules: []networkingv1.IngressRule{
				{
					Host: sandboxDomain,
					IngressRuleValue: networkingv1.IngressRuleValue{
						HTTP: &networkingv1.HTTPIngressRuleValue{
							Paths: []networkingv1.HTTPIngressPath{
								{
									Path:     "/",
									PathType: getPathTypePrefix(),
									Backend: networkingv1.IngressBackend{
										Resource: &corev1.TypedLocalObjectReference{
											APIGroup: &HigressAPIGroup,
											Kind:     "McpBridge",
											Name:     HigressMcpBridgeName,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	createdIngress, err := c.k8sClient.NetworkingV1().Ingresses(HigressDefaultNamespace).Create(ctx, ingress, metav1.CreateOptions{})
	return createdIngress, err
}

// Delete 删除 App (Deployment, Service, HPA, Ingress)
func (c *AppController) Delete(ctx context.Context, req *pb.DeleteDeploymentReq) error {
	// 关键步骤 1: 先删除 HPA (切断自动伸缩的源头)
	// 必须确保 HPA 被删除了，K8s 才会停止自动调整 Replicas
	deletePolicy := metav1.DeletePropagationForeground // 级联删除策略
	err := c.k8sClient.AutoscalingV2().HorizontalPodAutoscalers(req.TenantName).Delete(ctx, req.Name, metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	// 忽略 NotFound 错误，但如果其他错误则报错
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete HPA: %w", err)
	}

	// 关键步骤 2: 等待 HPA 真正消失 (可选，推荐)
	// 或者简单的 Sleep 一下让 Controller 反应过来
	time.Sleep(time.Second * 2)

	// 关键步骤 3: 将 Deployment Replicas 缩容为 0 (优雅停止)
	// 这比直接 Delete Deployment 更安全，能让 PreStop Hook 执行
	scaleReq := &pb.ScaleDeploymentReq{
		TenantName: req.TenantName,
		Name:       req.Name,
		Replicas:   0,
	}
	err = c.Scale(ctx, scaleReq)
	if err != nil {
		c.log.Errorf("failed to scale down deployment before deletion: %v", err)
		// 不返回错误，继续删除资源，避免卡死
	}
	// 1. Delete Deployment
	err = c.k8sClient.AppsV1().Deployments(req.TenantName).Delete(ctx, req.GetName(), metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete sandbox deployment: %w", err)
	}

	// 2. Delete HPA (不管是否存在，尝试删除)
	err = c.k8sClient.AutoscalingV2().HorizontalPodAutoscalers(req.TenantName).Delete(ctx, req.Name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		c.log.Errorf("failed to delete HPA: %v", err)
	}

	// 3. Delete Service
	serviceName := fmt.Sprintf("aims-sandbox-%s-service", req.Name)
	err = c.k8sClient.CoreV1().Services(req.TenantName).Delete(ctx, serviceName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		c.log.Errorf("failed to delete service: %v", err)
	}

	// 4. Delete Ingress
	sandboxDomain := fmt.Sprintf("%s.%s", req.Name, DefaultBaseDomain)
	err = c.k8sClient.NetworkingV1().Ingresses(HigressDefaultNamespace).Delete(ctx, sandboxDomain, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete sandbox ingress: %w", err)
	}

	// 5. Clean up S3 PV/PVCs (清理逻辑)
	// 因为我们命名规则是 pv-<deployName>-<idx>，我们不知道 idx 有多少个。
	// 方法 A: 使用 LabelSelector 批量删除
	// 方法 B: 在 CreateDeployment 时把 PVC 列表记下来存 DB (太重)
	// 这里使用 LabelSelector 删除 PVC (PV 会因为 ReclaimPolicy=Delete 自动删，或者手动删)

	// 删除 PVC (Namespace Scope)
	pvcList, err := c.k8sClient.CoreV1().PersistentVolumeClaims(req.TenantName).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("deployment=%s", req.Name),
	})
	if err == nil {
		for _, pvc := range pvcList.Items {
			_ = c.k8sClient.CoreV1().PersistentVolumeClaims(req.TenantName).Delete(ctx, pvc.Name, metav1.DeleteOptions{})
			// PV 清理：如果 ReclaimPolicy 不是 Delete，需要手动删 PV
			// 我们创建 PV 时指定了 Name，可以通过 Label 删
		}
	}

	// 删除 PV (Cluster Scope)
	pvList, err := c.k8sClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("deployment=%s,tenant=%s", req.Name, req.TenantName),
	})
	if err == nil {
		for _, pv := range pvList.Items {
			_ = c.k8sClient.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{})
		}
	}

	return nil
}

// GetReplicas 获取 Deployment 的真实副本数
func (c *AppController) GetReplicas(ctx context.Context, namespace, name string) (int32, error) {
	deploy, err := c.k8sClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	return deploy.Status.Replicas, nil
}

// Scale 扩缩容 (同时支持手动和 HPA)
func (c *AppController) Scale(ctx context.Context, req *pb.ScaleDeploymentReq) error {
	// 1. Update Deployment Replicas
	// 注意：如果存在 HPA，修改 Deployment Replicas 后可能会被 HPA 改回来
	// 只有在没有 HPA 或者同时修改 HPA 的 min/max 时才有效
	if req.Replicas > 0 {
		patchData := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, req.Replicas))
		_, err := c.k8sClient.AppsV1().Deployments(req.TenantName).Patch(
			ctx, req.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
		if err != nil {
			return fmt.Errorf("failed to scale deployment: %v", err)
		}
	}

	// 2. Update HPA (如果有参数)
	if req.MinReplicas != nil || req.MaxReplicas != nil {
		hpa, err := c.k8sClient.AutoscalingV2().HorizontalPodAutoscalers(req.TenantName).Get(ctx, req.Name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) && req.MinReplicas != nil && req.MaxReplicas != nil {
				// HPA 不存在，且参数完整 -> 创建 HPA
				return c.CreateHPA(ctx, req.TenantName, req.Name, *req.MinReplicas, *req.MaxReplicas)
			}
			return err
		}

		// HPA 存在 -> Update
		if req.MinReplicas != nil {
			hpa.Spec.MinReplicas = req.MinReplicas
		}
		if req.MaxReplicas != nil {
			hpa.Spec.MaxReplicas = *req.MaxReplicas
		}
		_, err = c.k8sClient.AutoscalingV2().HorizontalPodAutoscalers(req.TenantName).Update(ctx, hpa, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update HPA: %v", err)
		}
	} else {
		// 如果只设置了 replicas 且没有 min/max���且 deployment 已经有 HPA，
		// 那么 HPA 可能会干扰 Scale。
		// 策略：如果 req.MinReplicas == 0 (显式关闭 HPA)，则删除 HPA
		// 这里暂不实现复杂的 HPA 禁用逻辑，假设调用方逻辑清晰
	}

	return nil
}

// Start, Stop, Rollout 保持不变，可以复用
// ... (与你之前的代码一致，不再重复) ...

func (c *AppController) Rollout(ctx context.Context, req *pb.RolloutDeploymentReq) error {
	restartTime := time.Now().Format(time.RFC3339)
	patchData := []byte(fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"%s"}}}}}`, restartTime))
	_, err := c.k8sClient.AppsV1().Deployments(req.TenantName).Patch(
		ctx, req.Name, types.StrategicMergePatchType, patchData, metav1.PatchOptions{})
	return err
}

func (c *AppController) Stop(ctx context.Context, req *pb.StopDeploymentReq) error {
	// Stop 意味着 Scale to 0
	// 同时需要禁用 HPA (否则 HPA 会把副本数拉起来)
	// 1. Delete HPA
	_ = c.k8sClient.AutoscalingV2().HorizontalPodAutoscalers(req.TenantName).Delete(ctx, req.Name, metav1.DeleteOptions{})

	// 2. Scale Deployment to 0
	patchData := []byte(`{"spec":{"replicas":0}}`)
	_, err := c.k8sClient.AppsV1().Deployments(req.TenantName).Patch(
		ctx, req.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
	return err
}

func (c *AppController) Start(ctx context.Context, req *pb.StartDeploymentReq) error {
	// Start: Scale to Replicas (默认为1)
	replicas := int32(1)
	if req.Replicas != nil && *req.Replicas > 0 {
		replicas = *req.Replicas
	}

	patchData := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas))
	_, err := c.k8sClient.AppsV1().Deployments(req.TenantName).Patch(
		ctx, req.Name, types.MergePatchType, patchData, metav1.PatchOptions{})

	// 注意：Start 后如果需要恢复 HPA，需要调用 Scale 接口重新配置 HPA
	// 这里仅恢复副本数
	return err
}

// selectTargetPort 和 getPathTypePrefix 保持不变
func (c *AppController) selectTargetPort(service *corev1.Service, selector PortSelector) (int32, error) {
	if selector.Name != "" {
		for _, port := range service.Spec.Ports {
			if port.Name == selector.Name {
				return port.Port, nil
			}
		}
		return 0, fmt.Errorf("service %s/%s 未找到名称为%s的端口", service.Namespace, service.Name, selector.Name)
	}
	if selector.Port > 0 {
		for _, port := range service.Spec.Ports {
			if port.Port == selector.Port {
				return port.Port, nil
			}
		}
		return 0, fmt.Errorf("service %s/%s 未找到端口号为%d的端口", service.Namespace, service.Name, selector.Port)
	}
	for _, port := range service.Spec.Ports {
		if port.Name == DefaultJupyterPortName {
			return port.Port, nil
		}
	}
	return service.Spec.Ports[0].Port, nil
}

func getPathTypePrefix() *networkingv1.PathType {
	pt := networkingv1.PathTypePrefix
	return &pt
}
