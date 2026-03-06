package monitor

import (
	"github.com/go-kratos/kratos/v2/log"
	conf "go-wind-admin/api/gen/go/conf/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// UpdateByConfig 负责 K8s 客户端的初始化和 ClusterMonitor 的启动
func UpdateByConfig(manager *MonitorManager, cfg *conf.Bootstrap) error {
	if cfg == nil || cfg.Kubernetes == nil {
		return nil
	}

	for _, c := range cfg.Kubernetes.Config {
		if c.Name == "" {
			continue
		}

		var k8sCfg *rest.Config
		var err error
		if c.InCluster != nil && *c.InCluster {
			k8sCfg, err = rest.InClusterConfig()
		} else if c.KubeconfigPath != nil && *c.KubeconfigPath != "" {
			k8sCfg, err = clientcmd.BuildConfigFromFlags("", *c.KubeconfigPath)
		} else {
			k8sCfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		}
		if err != nil {
			log.Errorf("Failed to build config for %s: %v", c.Name, err)
			continue
		}

		client, err := kubernetes.NewForConfig(k8sCfg)
		if err != nil {
			log.Errorf("Failed to create client for %s: %v", c.Name, err)
			continue
		}

		// 修复：显式传递 manager，让 cm 能够回调同步账本
		cm := NewClusterMonitor(c.Name, client, manager)
		cm.InitInformer()
		manager.AddCluster(cm)
		log.Infof("Loaded cluster: %s", c.Name)
	}
	return nil
}
