package pool

import (
	"sync"

	"github.com/panjf2000/ants"
)

// customPool 通用协程池
var customPool *ants.Pool

var once sync.Once

// NewPool 初始化通用协程池
func NewPool(size int) {
	once.Do(func() {
		customPool, _ = ants.NewPool(size)
	})
}

// GetPool 获取通用协程池
func GetPool() *ants.Pool {
	return customPool
}
