package examples

import (
	"github.com/Darker-D/ddbase/log"
	"github.com/Darker-D/ddbase/queue/rabbitmq"
	"fmt"
	"github.com/streadway/amqp"
	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
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
		Name:       "induce_cancel", // 交换机名称
		Type:       "fanout",        // 交换机类型
		Durable:    false,           // 重启服务是否自动删除
		AutoDelete: false,           //
		Internal:   false,           //
		NoWait:     false,           //
		Args:       nil,             //
	}

	queue := rabbitmq.Queue{
		Name:       "asr_queue", // 队列名
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
	binding := rabbitmq.BindingOptions{
		RoutingKey: "induceCancelAsrQueue", // 交换机与队列绑定
		NoWait:     false,
		Args:       nil,
	}

	consumerOptions := rabbitmq.ConsumerOptions{
		Tag:       "induceCancelAsrParse", //
		AutoAck:   false,                  // 是否自动提交
		Exclusive: false,                  //
		NoLocal:   false,                  //
		NoWait:    false,                  //
		Args:      nil,                    //
	}
	rmq := rabbitmq.New(
		&rabbitmq.Config{
			Host:     "172.17.11.97", // host
			Port:     5672,           // 端口
			Username: "jryg-test",
			Password: "ByM9g&Xd0Hcq",
			Vhost:    "/aiCenter", //
			Session: rabbitmq.Session{
				Exchange:        exchange,
				Queue:           queue,
				BindingOptions:  binding,
				ConsumerOptions: consumerOptions,
			},
		},
	)

	consumer := rmq.NewConsumer(handler)

	defer consumer.Shutdown()
	err := consumer.QOS(3)
	if err != nil {
		panic(err)
	}
	fmt.Println("Elasticsearch Feeder worker started")
	//consumer.RegisterSignalHandler()

	go func() {

		time.Sleep(3 * time.Second)
		consumer.GetChannel().Close()
	}()
	consumer.Consume(handler)

}

var handler = func(delivery amqp.Delivery) {
	message := string(delivery.Body)
	fmt.Println(message)
	delivery.Ack(false)
}
