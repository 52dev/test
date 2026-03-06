package service

// 服务名称命名规则
//
// Consul：字母、数字、破折号；
// Etcd:
// Nacos:

const (
	Project = "aims"

	WorkerService = "worker"         // 工作台BFF
	AdminService  = "admin-gateway"  // 后台BFF
	ServerService = "server-gateway" // 后台BFF
)

// NewDiscoveryName 构建服务发现名称
func NewDiscoveryName(serviceName string) string {
	return Project + "/" + serviceName
}
