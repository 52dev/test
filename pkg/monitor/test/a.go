package main

import (
	"fmt"
	"go-wind-admin/pkg/monitor" // 你的包路径
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	// 1. 初始化管理器
	configA, _ := clientcmd.BuildConfigFromFlags("", "C:\\Users\\wang.qiyuan\\.kube\\config")
	clientA, _ := kubernetes.NewForConfig(configA)
	manager := monitor.NewMonitorManagerWithLocalCluster("Beijing-1", clientA)

	// 2. 模拟注册集群 A (从 kubeconfig 读取)

	//manager.AddCluster("Beijing-2", clientA)

	// 3. 模拟注册集群 B
	// configB, _ := clientcmd.BuildConfigFromFlags("", "/root/.kube/config-cluster-b")
	// clientB, _ := kubernetes.NewForConfig(configB)
	// manager.AddCluster("Shanghai-1", clientB)

	// 模拟 API 调用循环
	for {
		fmt.Println("\n-------- 全局多集群资源池扫描 --------")

		// 获取聚合后的数据
		// 如果某个集群断连，这里拿到的数据会自动剔除该集群的资源
		pool := manager.GetGlobalPool()
		pool.Print()

		time.Sleep(5 * time.Second)
	}
}
