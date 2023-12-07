package kafka

import (
	"context"
	"github.com/Darker-D/ddbase/log"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type Config struct {
	Addr  []string
	Topic string
	*sarama.Config
}

func NewAsyncProducer(c *Config) (sarama.AsyncProducer, error) {
	if c.Config == nil {
		c.Config = sarama.NewConfig()
		c.Config.Producer.RequiredAcks = sarama.WaitForAll
		// 开启异步回调
		c.Producer.Return.Successes = true
		c.Producer.Return.Errors = true

	}
	producer, err := sarama.NewAsyncProducer(c.Addr, c.Config)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	// 异步回调
	go func() {
	EXITED:
		for {
			select {
			case message := <-producer.Successes():
				log.Logger().Info("AsyncProducer", zap.Any("data", message))
			case err := <-producer.Errors():
				log.Logger().Info("AsyncProducer", zap.Any("err", err))
			case <-ctx.Done():
				log.Logger().Info("AsyncProducer", zap.Any("err", ctx.Err()))
				break EXITED
			}
		}
	}()

	return producer, err
}

func NewSyncProducer(c *Config) (sarama.SyncProducer, error) {
	if c.Config == nil {
		c.Config = sarama.NewConfig()
	}
	producer, err := sarama.NewSyncProducer(c.Addr, c.Config)
	if err != nil {
		return nil, err
	}

	return producer, err
}
