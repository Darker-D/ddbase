package sign

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"sort"
	"strings"
)

// key secret
const (
	_appID     = "front-pkg"
	_appSecret = "350ffca461ad77bc444f41e18a4be327"
	_algorithm = "sha256"
)

// Config .
type Config struct {
	AppID     string
	AppSecret string
	Algorithm string
}

var defaultConfig = &Config{
	AppID:     _appID,
	AppSecret: _appSecret,
	Algorithm: _algorithm,
}

// New Sign .
func New(c *Config) *Sign {
	if c != nil {
		return &Sign{c: c}
	}
	return &Sign{c: defaultConfig}
}

type Sign struct {
	c *Config
}

func (s *Sign) GenSign(params map[string]string) string {
	// 创建切片
	var keys = make([]string, 0, len(params))
	// 遍历签名参数
	for k := range params {
		if k != "sign" { // 排除sign字段
			keys = append(keys, k)
		}
	}

	// 由于切片的元素顺序是不固定，所以这里强制给切片元素加个顺序
	sort.Strings(keys)

	//创建字符缓冲
	var buf bytes.Buffer
	for _, k := range keys {
		if len(params[k]) > 0 {
			buf.WriteString(k)
			buf.WriteString(`=`)
			buf.WriteString(params[k])
			buf.WriteString(`&`)
		}
	}
	// 加入apiKey作加密密钥
	buf.WriteString(`key=`)
	buf.WriteString(s.c.AppSecret)

	var h hash.Hash
	if s.c.Algorithm == _algorithm {
		h = hmac.New(sha256.New, []byte(s.c.AppSecret))
	} else {
		h = hmac.New(md5.New, []byte(s.c.AppSecret))
	}

	h.Write(buf.Bytes())

	return strings.ToLower(hex.EncodeToString(h.Sum(nil)[:]))
}
