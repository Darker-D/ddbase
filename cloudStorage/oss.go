package cloudStorage

import (
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"math/rand"
	"path"
	"time"
)

type Config struct {
	Endpoint        string // 节点
	AccessKeyId     string // 访问id
	AccessKeySecret string // 访问秘钥
	BucketName      string // 桶名称
	PartSize        int64  // 断点续传块大小
	Url             string // 文件url prefix
}

func New(c *Config) *Oss {
	cli, err := oss.New(c.Endpoint, c.AccessKeyId, c.AccessKeySecret)
	if err != nil {
		panic(err)
	}
	return &Oss{
		c,
		cli,
	}
}

type Oss struct {
	c *Config
	*oss.Client
}

// UploadFile oss upload file return cloudFileUrl
// prefix 文件名前缀 用于文件说明
func (o *Oss) UploadFile(prefix, filePath string) (cloudFileUrl string, err error) {
	bucket, err := o.Bucket(o.c.BucketName)
	if err != nil {
		return
	}
	// objectKey
	fileExt := path.Ext(filePath)
	cloudFileUrl = fmt.Sprintf("%s%s", o.genObjectKey(prefix), fileExt)
	err = bucket.UploadFile(cloudFileUrl, filePath, o.c.PartSize)
	return
}

// UploadFile oss upload file return cloudFileUrl
// prefix 文件名前缀 用于文件说明
// folder 文件目录
func (o *Oss) UploadFileWithFolder(folder, prefix, filePath string) (cloudFileUrl string, err error) {
	bucket, err := o.Bucket(o.c.BucketName)
	if err != nil {
		return
	}
	// objectKey
	fileExt := path.Ext(filePath)
	cloudFileUrl = path.Join(folder, fmt.Sprintf("%s%s", o.genObjectKey(prefix), fileExt))
	err = bucket.UploadFile(cloudFileUrl, filePath, o.c.PartSize)
	return
}

func (o *Oss) genObjectKey(filenamePrefix string) string {
	return fmt.Sprintf("%s-%s-%d", filenamePrefix, randStr(8), time.Now().Unix())
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStr(n int) string {
	b := make([]rune, n)
	randMarker := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range b {
		b[i] = letters[randMarker.Intn(len(letters))]
	}
	return string(b)
}
