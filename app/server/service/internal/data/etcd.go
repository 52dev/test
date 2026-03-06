package data

import (
	conf "go-wind-admin/api/gen/go/conf/v1"
	"time"

	"github.com/tx7do/kratos-bootstrap/bootstrap"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewEtcdClient(ctx *bootstrap.Context, cfg *conf.Bootstrap) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: 5 * time.Second,
		Username:    cfg.Etcd.Username,
		Password:    cfg.Etcd.Password,
	})
	return cli, err
}
