package gorm

import (
	"context"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestNewMySQL(t *testing.T) {
	type args struct {
		c *Config
	}
	tests := []struct {
		name   string
		args   args
		wantDb *gorm.DB
	}{
		{
			name: "测试连接是否可用",
			args: args{
				c: &Config{
					DSN:         "YGCD:YGcd_*@1518test@tcp(rm-2zey3uc4b631y4128318.mysql.rds.aliyuncs.com:3306)/ai_nlp?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true&loc=Local&charset=utf8,utf8mb4",
					Active:      1,
					Idle:        10,
					IdleTimeout: 30 * time.Second,
				},
			},
			wantDb: NewMySQL(&Config{
				DSN:         "YGCD:YGcd_*@1518test@tcp(rm-2zey3uc4b631y4128318.mysql.rds.aliyuncs.com:3306)/ai_nlp?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true&loc=Local&charset=utf8,utf8mb4",
				Active:      1,
				Idle:        10,
				IdleTimeout: 30 * time.Second,
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// assert.
			t.Log(tt.wantDb.WithContext(context.TODO()).Name())
		})
	}
}
