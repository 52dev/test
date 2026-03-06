package oss

import (
	"context"

	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/minio/minio-go/v7"

	conf "github.com/tx7do/kratos-bootstrap/api/gen/go/conf/v1"
	ossMinio "github.com/tx7do/kratos-bootstrap/oss/minio"

	serverV1 "go-wind-admin/api/gen/go/server/service/v1"
)

const (
	defaultExpiryTime = time.Minute * 60 // 默认的预签名时间，默认为：1小时
)

// MinIOClient MinIO 客户端封装
type MinIOClient struct {
	mc         *minio.Client
	conf       *conf.OSS
	log        *log.Helper
	hmacSecret []byte
}

func NewMinIoClient(cfg *conf.Bootstrap, logger log.Logger) *MinIOClient {
	l := log.NewHelper(log.With(logger, "module", "minio/data/admin-service"))
	return &MinIOClient{
		log:        l,
		conf:       cfg.Oss,
		mc:         ossMinio.NewClient(cfg.Oss),
		hmacSecret: staticHMACSecret,
	}
}

// GetClient returns the underlying MinIO client
func (c *MinIOClient) GetClient() *minio.Client {
	return c.mc
}

// DeleteFile 删除一个文件
func (c *MinIOClient) DeleteFile(ctx context.Context, bucketName, objectName string) error {
	if bucketName == "" {
		return serverV1.ErrorBadRequest("bucket name is required")
	}
	if objectName == "" {
		return serverV1.ErrorBadRequest("object name is required")
	}

	err := c.mc.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		c.log.Errorf("Failed to delete file: %v", err)
		return serverV1.ErrorExpectationFailed("failed to delete file")
	}

	return nil
}

func (c *MinIOClient) DeletePath(ctx context.Context, bucketName, path string) error {
	if bucketName == "" {
		return serverV1.ErrorBadRequest("bucket name is required")
	}
	if path == "" {
		return serverV1.ErrorBadRequest("object name is required")
	}

	objectsCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objectsCh)
		// 递归列出所有对象
		for object := range c.mc.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: path, Recursive: true}) {
			if object.Err != nil {
				c.log.Errorf("Error listing objects: %v", object.Err)
				return
			}
			objectsCh <- object
		}
	}()

	// 批量删除对象
	errorCh := c.mc.RemoveObjects(ctx, bucketName, objectsCh, minio.RemoveObjectsOptions{})

	// 检查删除操作的错误
	for e := range errorCh {
		c.log.Errorf("Failed to remove object %s, error: %s", e.ObjectName, e.Err)
		return serverV1.ErrorExpectationFailed("failed to delete path")
	}

	return nil
}
