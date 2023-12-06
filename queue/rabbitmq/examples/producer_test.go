package examples

import (
	"github.com/Darker-D/ddbase/log"
	"github.com/Darker-D/ddbase/queue/rabbitmq"
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

func TestProducer(t *testing.T) {
	log.Init(&log.ZLogConfig{
		Source:     "pkg",
		Dir:        "./logs/",
		Filename:   "pkg",
		Level:      "warn",
		Stdout:     true,
		MaxAge:     1,
		MaxSize:    1,
		MaxBackups: 1,
	})

	exchange := rabbitmq.Exchange{
		Name:       "topic.order.call.records.result", // 交换机名称
		Type:       "fanout",                          // 交换机类型
		Durable:    true,                              // 重启服务是否自动删除
		AutoDelete: false,                             //
		Internal:   false,                             //
		NoWait:     false,                             //
		Args:       nil,                               //
	}

	queue := rabbitmq.Queue{
		Name:       "queue.order.call.records.result", // 队列名
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}

	publishingOptions := rabbitmq.PublishingOptions{
		Tag:        "",
		RoutingKey: "",
		Mandatory:  false,
		Immediate:  false,
	}

	rmq := rabbitmq.New(
		&rabbitmq.Config{
			Host:     "172.17.8.61",
			Port:     5672,
			Username: "jryg-dev",
			Password: "hEzjAdfqqKIsrvPl",
			Vhost:    "",
			Session: rabbitmq.Session{
				Exchange:          exchange,
				Queue:             queue,
				PublishingOptions: publishingOptions,
			},
		},
	)

	publisher := rmq.NewProducer()
	defer publisher.Shutdown()
	publisher.RegisterSignalHandler()

	// may be we should autoconvert to byte array?
	msg := amqp.Publishing{
		Body: []byte("24444"),
	}

	publisher.NotifyReturn(func(message amqp.Return) {
		fmt.Println(message)
	})

	go func() {
		time.Sleep(10 * time.Second)
		publisher.GetChannel().Close()
	}()

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second * time.Duration(i))
		fmt.Println("发布")
		err := publisher.Publish(msg)
		if err != nil {
			fmt.Println(err, i)
		}
	}
}
