package rabbitmq

import (
	"github.com/Darker-D/ddbase/log"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"math"
	"sync/atomic"
	"time"
)

type Consumer struct {
	// Base struct for RabbitMQ
	*RabbitMQ

	// The communication channel over connection
	channel *amqp.Channel

	// All deliveries from server will send to this channel
	deliveries <-chan amqp.Delivery

	// This handler will be called when a
	handler func(amqp.Delivery)

	// A notifiyng channel for publishings
	// will be used for sync. between close channel and consume handler
	done chan error

	// Current producer connection settings
	session Session

	// chanInterval is the next reChannel times
	chanInterval float64

	// chant reChannel ticker
	chant *time.Ticker
}

// Deliveries get current Delivery .
func (c *Consumer) Deliveries() <-chan amqp.Delivery {
	return c.deliveries
}

// Consume accepts a handler function for every message streamed from RabbitMq
// will be called within this handler func
func (c *Consumer) Consume(handler func(delivery amqp.Delivery)) error {
	co := c.session.ConsumerOptions
	q := c.session.Queue
	// Exchange bound to Queue, starting Consume
	deliveries, err := c.channel.Consume(
		// consume from real queue
		q.Name,       // name
		co.Tag,       // consumerTag,
		co.AutoAck,   // autoAck
		co.Exclusive, // exclusive
		co.NoLocal,   // noLocal
		co.NoWait,    // noWait
		co.Args,      // arguments
	)
	if err != nil {
		return err
	}

	// should we stop streaming, in order not to consume from server?
	c.deliveries = deliveries
	if handler != nil {
		c.handler = handler
	}

	// log.Info("handle: deliveries channel starting")

	// handle all consumer errors, if required re-connect
	// there are problems with reconnection logic for now
	for delivery := range c.deliveries {
		c.handler(delivery)
	}

	// log.Info("handle: deliveries channel closed")
	c.done <- nil
	return nil
}

// initChannel internally declares the exchanges and queues
func (c *Consumer) initChannel() error {
	e := c.session.Exchange
	q := c.session.Queue
	bo := c.session.BindingOptions

	var err error

	// global==true 时 表示在当前channel上所有的 consumer 都生效，否则只对设置了之后新建的consumer(channel)生效
	err = c.channel.Qos(c.config.ConsumerOptions.PrefetchCount, c.config.ConsumerOptions.PrefetchSize, c.config.ConsumerOptions.Global)
	if err != nil {
		panic(err)
	}

	// declaring Exchange
	if err = c.channel.ExchangeDeclare(
		e.Name,       // name of the exchange
		e.Type,       // type
		e.Durable,    // durable
		e.AutoDelete, // delete when complete
		e.Internal,   // internal
		e.NoWait,     // noWait
		e.Args,       // arguments
	); err != nil {
		return err
	}

	// declaring Queue
	_, err = c.channel.QueueDeclare(
		q.Name,       // name of the queue
		q.Durable,    // durable
		q.AutoDelete, // delete when usused
		q.Exclusive,  // exclusive
		q.NoWait,     // noWait
		q.Args,       // arguments
	)
	if err != nil {
		return err
	}

	// binding Exchange to Queue
	if err = c.channel.QueueBind(
		// bind to real queue
		q.Name,        // name of the queue
		bo.RoutingKey, // bindingKey
		e.Name,        // sourceExchange
		bo.NoWait,     // noWait
		bo.Args,       // arguments
	); err != nil {
		return err
	}

	return nil
}

// NotifyClose registers a listener for close events either initiated by an error
// accompaning a channel.close method or by a normal shutdown.
// On normal shutdowns, the chan will be closed.
// To reconnect after a transport or protocol error, we should register a listener here and
// re-connect to server
func (c *Consumer) handleErrors() {
	go func() {
		// handle channel error , retry get new channel
	RE:
		nc := c.channel.NotifyClose(make(chan *amqp.Error))
		for {
			select {
			case e := <-nc:
				switch {
				case e != nil && e.Code == 501:
					_ = c.Shutdown()
					return
				}
				if (c.conn != nil && c.conn.IsClosed() && c.channel != nil) || atomic.LoadInt32(&c.isConnClosed) == 1 {
					goto RE
				}
				c.reChannel()
				err := c.initChannel()
				if err != nil {
					continue
				}
				go c.Consume(nil)
				goto RE
			}
		}
	}()

	go func() {
		// handle tcp connection error, retry get new connection
		reconn := c.connChan
		for {
			select {
			case conn := <-reconn:
				log.Logger().Error("rabbitmq", zap.String("consumer", "tcp recreating ..."))
				c.conn = conn
				c.reChannel()
				err := c.initChannel()
				if err != nil {
					panic(err)
				}
				atomic.SwapInt32(&c.isConnClosed, 0)
				go c.Consume(nil)
			}
		}
	}()
}

const (
	// maxRetryIntervalTime
	maxRetryIntervalTime float64 = 10
)

// reChannel retry get channel from connection .
func (c *Consumer) reChannel() {
	if c.conn == nil || c.conn.IsClosed() {
		return
	}
	_ = c.channel.Close()
RE:
	sleep := time.Duration(int64(math.Min(maxRetryIntervalTime, math.Pow(2, c.chanInterval)))) * time.Second
	log.Logger().Warn("rabbitmq", zap.String("consumer", "channel recreating ..."), zap.Float64("sleep", sleep.Seconds()))
	c.chant.Reset(sleep)
	select {
	case <-c.chant.C:
	}
	channel, err := c.conn.Channel()
	if err != nil {
		log.Logger().Warn("rabbitmq", zap.String("consumer", "channel recreated failed"), zap.Error(err))
		c.chanInterval += 1
		goto RE
	}
	c.channel = channel
	c.chanInterval = 0
	c.chant.Stop()
	log.Logger().Warn("rabbitmq", zap.String("consumer", "channel recreated success"))
}

// QOS controls how many messages the server will try to keep on the network for
// consumers before receiving delivery acks.  The intent of Qos is to make sure
// the network buffers stay full between the server and client.
func (c *Consumer) QOS(messageCount int) error {
	return c.channel.Qos(messageCount, 0, false)
}

// ConsumeMessage accepts a handler function and only consumes one message
// stream from RabbitMq
func (c *Consumer) Get(handler func(delivery amqp.Delivery)) error {
	co := c.session.ConsumerOptions
	q := c.session.Queue
	message, ok, err := c.channel.Get(q.Name, co.AutoAck)
	if err != nil {
		return err
	}

	if ok {
		if nil != handler {
			handler(message)
		} else if nil != c.handler {
			c.handler(message)
		}
	}

	// maybe we should return ok too?
	return nil
}

// Shutdown gracefully closes all connections and waits
// for handler to finish its messages
func (c *Consumer) Shutdown() error {
	co := c.session.ConsumerOptions
	if err := shutdownChannel(c.channel, co.Tag); err != nil {
		return err
	}

	// defer log.Info("Consumer shutdown OK")
	// log.Info("Waiting for Consumer handler to exit")

	// if we have not called the Consume yet, we can return here
	if c.deliveries == nil {
		close(c.done)
	}

	// this channel is here for finishing the consumer's ranges of
	// delivery chans.  We need every delivery to be processed, here make
	// sure to wait for all consumers goroutines to finish before exiting our
	// process.
	return <-c.done
}

// GetChannel get current channel .
func (c *Consumer) GetChannel() *amqp.Channel {
	return c.channel
}
