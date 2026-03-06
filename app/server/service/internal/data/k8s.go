package data

import (
	"errors"
	"fmt"
	conf "go-wind-admin/api/gen/go/conf/v1"

	//higressclientset "github.com/alibaba/higress/v2/client/pkg/clientset/versioned"
	"github.com/go-kratos/kratos/v2/log"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	kueueclientset "sigs.k8s.io/kueue/client-go/clientset/versioned"
)

func NewKubeConfig(cfg *conf.Bootstrap) (*rest.Config, error) {
	if cfg == nil || cfg.Kubernetes == nil {
		return nil, nil
	}
	c := cfg.Kubernetes.GetConfig()
	if c.Name == "" {
		return nil, nil
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
		return nil, err
	}
	return k8sCfg, nil
}

func NewKubeClient(cfg *conf.Bootstrap) (kubernetes.Interface, error) {
	k8sCfg, err := NewKubeConfig(cfg)
	if err != nil {
		return nil, err
	}
	if k8sCfg == nil {
		fmt.Println(cfg)
		return nil, errors.New("no kubernetes config found")
	}
	client, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		log.Errorf("Failed to create client: %v", err)
		panic(err)
	}

	log.Info("Loaded kubernetes client")
	return client, nil
}

func NewKueueClient(cfg *conf.Bootstrap) (kueueclientset.Interface, error) {
	k8sCfg, err := NewKubeConfig(cfg)
	if err != nil {
		return nil, err
	}
	client, err := kueueclientset.NewForConfig(k8sCfg)
	if err != nil {
		log.Errorf("Failed to create kueue client: %v", err)
		panic(err)
	}

	log.Info("Loaded kueue client")
	return client, nil
}

//func NewHigressClient(cfg *conf.Bootstrap) (higressclientset., error) {
//	k8sCfg, err := NewKubeConfig(cfg)
//	if err != nil {
//		return nil, err
//	}
//	client, err := higressclientset.NewForConfig(k8sCfg)
//	if err != nil {
//		log.Errorf("Failed to create kueue client: %v", err)
//		panic(err)
//	}
//
//	log.Info("Loaded kueue client")
//	return client, nil
//}
