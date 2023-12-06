package rabbitmq

import (
	"github.com/Darker-D/ddbase/log"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"math"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/streadway/amqp"
)

// Config .
type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	Vhost    string
	Session
}

var _defaultBasicQos = &BasicQos{
	PrefetchCount: 100,
	PrefetchSize:  0,
	Global:        false,
}

// New .
func New(c *Config) *RabbitMQ {
	if c.ConsumerOptions.BasicQos == nil {
		c.ConsumerOptions.BasicQos = _defaultBasicQos
	}
	return &RabbitMQ{
		config:   c,
		connChan: make(chan *amqp.Connection, 1),
		connt:    time.NewTicker(time.Duration(1)),
	}
}

// RabbitMQ .
type RabbitMQ struct {
	// The connection between client and the server
	conn *amqp.Connection

	// config stores the current configuration based on the given profile
	config *Config

	// interval is the next reconnection times
	connInterval float64

	// reConn ticker
	connt *time.Ticker

	// Is the current connection reconnected
	reconnected bool

	// connChan synchronize a new connection to channel
	connChan chan *amqp.Connection

	// isConnClosed connection is no closed .
	isConnClosed int32
}

type Exchange struct {
	// Exchange name
	Name string

	// Exchange type
	Type string

	// Durable exchanges will survive server restarts
	Durable bool

	// Will remain declared when there are no remaining bindings.
	AutoDelete bool

	// Exchanges declared as `internal` do not accept accept publishings.Internal
	// exchanges are useful for when you wish to implement inter-exchange topologies
	// that should not be exposed to users of the broker.
	Internal bool

	// When noWait is true, declare without waiting for a confirmation from the server.
	NoWait bool

	// amqp.Table of arguments that are specific to the server's implementation of
	// the exchange can be sent for exchange types that require extra parameters.
	Args amqp.Table
}

type Queue struct {
	// The queue name may be empty, in which the server will generate a unique name
	// which will be returned in the Name field of Queue struct.
	Name string

	// Check Exchange comments for durable
	Durable bool

	// Check Exchange comments for autodelete
	AutoDelete bool

	// Exclusive queues are only accessible by the connection that declares them and
	// will be deleted when the connection closes.  Channels on other connections
	// will receive an error when attempting declare, bind, consume, purge or delete a
	// queue with the same name.
	Exclusive bool

	// When noWait is true, the queue will assume to be declared on the server.  A
	// channel exception will arrive if the conditions are met for existing queues
	// or attempting to modify an existing queue from a different connection.
	NoWait bool

	// Check Exchange comments for Args
	Args amqp.Table
}

type ConsumerOptions struct {
	// The consumer is identified by a string that is unique and scoped for all
	// consumers on this channel.
	Tag string

	// When autoAck (also known as noAck) is true, the server will acknowledge
	// deliveries to this consumer prior to writing the delivery to the network.  When
	// autoAck is true, the consumer should not call Delivery.Ack
	AutoAck bool // autoAck

	// 是否独自从服务器上消费，true则独自消费改队列，false则公平的分发到多个消费者中消费
	Exclusive bool // exclusive

	// When noLocal is true, the server will not deliver publishing sent from the same
	// connection to this consumer. (Do not use Publish and Consume from same channel)
	NoLocal bool // noLocal

	// Check Queue struct documentation
	NoWait bool // noWait

	// BasicQos .
	*BasicQos

	// Check Exchange comments for Args
	Args amqp.Table // arguments
}

// BasicQos .
type BasicQos struct {
	// PrefetchCount .
	PrefetchCount int

	// PrefetchSize .
	PrefetchSize int

	// Global .
	Global bool
}

type BindingOptions struct {
	// Publishings messages to given Queue with matching -RoutingKey-
	// Every Queue has a default binding to Default Exchange with their Qeueu name
	// So you can send messages to a queue over default exchange
	RoutingKey string

	// Do not wait for a consumer
	NoWait bool

	// App specific data
	Args amqp.Table
}

// NewConsumer is a constructor for consumer creation
// Accepts Exchange, Queue, BindingOptions and ConsumerOptions
func (r *RabbitMQ) NewConsumer(handler func(delivery amqp.Delivery)) *Consumer {
	rmq := r.Connect()

	// getting a channel
	channel, err := r.conn.Channel()
	if err != nil {
		panic(err)
	}

	c := &Consumer{
		RabbitMQ: rmq,
		channel:  channel,
		done:     make(chan error),
		session:  r.config.Session,
		handler:  handler,
		chant:    time.NewTicker(1),
	}

	err = c.initChannel()
	if err != nil {
		panic(err)
	}

	// handle channel error
	c.handleErrors()

	return c
}

// NewProducer is a constructor function for producer creation Accepts Exchange,
// Queue, PublishingOptions. On the other hand we are not declaring our topology
// on both the publisher and consumer to be able to change the settings only in
// one place. We can declare those settings on both place to ensure they are
// same. But this package will not support it.
func (r *RabbitMQ) NewProducer() *Producer {
	rmq := r.Connect()

	// getting a channel
	channel, err := r.conn.Channel()
	if err != nil {
		panic(err)
	}

	producer := &Producer{
		RabbitMQ: rmq,
		channel:  channel,
		session:  r.config.Session,
		chant:    time.NewTicker(1),
	}

	// handle connection and channel error
	producer.handleErrors()

	return producer
}

// Returns RMQ connection
func (r *RabbitMQ) Conn() *amqp.Connection {
	return r.conn
}

// Dial dials the RMQ server
func (r *RabbitMQ) Dial() error {
	// if config is nil do not continue
	if r.config == nil {
		return errors.New("config is nil")
	}

	conf := amqp.URI{
		Scheme:   "amqp",
		Host:     r.config.Host,
		Port:     r.config.Port,
		Username: r.config.Username,
		Password: r.config.Password,
		Vhost:    r.config.Vhost,
	}.String()

	var err error
	// Connects opens an AMQP connection from the credentials in the URL.
	r.conn, err = amqp.Dial(conf)
	if err != nil {
		return err
	}

	return nil
}

// Connect opens a connection to RabbitMq. This function is idempotent
//
// this should not return RabbitMQ struct - cihangir,arslan config changes
func (r *RabbitMQ) Connect() *RabbitMQ {
	// force close conn
	if r.conn != nil && !r.conn.IsClosed() {
		_ = r.conn.Close()
	}

RE:
	//if r.conn != nil {
	sleep := time.Duration(int64(math.Min(maxRetryIntervalTime, math.Pow(2, r.connInterval)))) * time.Second
	if r.reconnected {
		log.Logger().Error("RabbitMQ.reConnect", zap.Float64("sleep", sleep.Seconds()))
	}
	r.connt.Reset(sleep)
	select {
	case <-r.connt.C:
	}
	//}

	if err := r.Dial(); err != nil {

		log.Logger().Error("RabbitMQ.Connect", zap.Error(err))
		if !r.reconnected {
			panic(err)
		}

		r.connInterval += 1
		goto RE
	}

	// retry connection sync
	if r.reconnected {
		r.connChan <- r.conn
	}

	if !r.reconnected {
		r.handleErrors()
	}

	r.reconnected = true
	r.connInterval = 0
	r.connt.Stop()
	return r
}

// Session is holding the current Exchange, Queue,
// Binding Consuming and Publishing settings for enclosed
// rabbitmq connection
type Session struct {
	// Exchange declaration settings
	Exchange Exchange

	// Queue declaration settings
	Queue Queue

	// Binding options for current exchange to queue binding
	BindingOptions BindingOptions

	// Consumer options for a queue or exchange
	ConsumerOptions ConsumerOptions

	// Publishing options for a queue or exchange
	PublishingOptions PublishingOptions
}

// NotifyClose registers a listener for close events either initiated by an error
// accompaning a connection.close method or by a normal shutdown.
// On normal shutdowns, the chan will be closed.
// To reconnect after a transport or protocol error, we should register a listener here and
// re-connect to server
// Reconnection is -not- working by now
func (r *RabbitMQ) handleErrors() {
	go func() {
	RE:
		connClose := r.conn.NotifyClose(make(chan *amqp.Error))
		blocked := r.conn.NotifyBlocked(make(chan amqp.Blocking))
		for {
			select {
			case amqpErr := <-connClose:
				log.Logger().Error("conn.NotifyClose", zap.Error(amqpErr))
				atomic.SwapInt32(&r.isConnClosed, 1)
				_ = r.Connect()
				goto RE
			case b := <-blocked:
				if b.Active {
					log.Logger().Error("TCP blocked", zap.String("reason", b.Reason))
				}
				//default:

			}
		}
	}()
}

// Shutdown closes the RabbitMQ connection
func (r *RabbitMQ) Shutdown() error {
	return shutdown(r.conn)
}

// RegisterSignalHandler watchs for interrupt signals
// and gracefully closes connection
func (r *RabbitMQ) RegisterSignalHandler() {
	registerSignalHandler(r)
}

// Closer interface is for handling reconnection logic in a sane way
// Every reconnection supported struct should implement those methods
// in order to work properly
type Closer interface {
	RegisterSignalHandler()
	Shutdown() error
}

// shutdown is a general closer function for handling close gracefully
// Mostly here for both consumers and producers
// After a reconnection scenerio we are gonna call shutdown before connection
func shutdown(conn *amqp.Connection) error {
	if err := conn.Close(); err != nil {
		if amqpError, isAmqpError := err.(*amqp.Error); isAmqpError && amqpError.Code != 504 {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}

	return nil
}

// shutdownChannel is a general closer function for channels
func shutdownChannel(channel *amqp.Channel, tag string) error {
	// This waits for a server acknowledgment which means the sockets will have
	// flushed all outbound publishings prior to returning.  It's important to
	// block on Close to not lose any publishings.
	if err := channel.Cancel(tag, true); err != nil {
		if amqpError, isAmqpError := err.(*amqp.Error); isAmqpError && amqpError.Code != 504 {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}

	if err := channel.Close(); err != nil {
		return err
	}

	return nil
}

// registerSignalHandler helper function for stopping consumer or producer from
// operating further
// Watchs for SIGINT, SIGTERM, SIGQUIT, SIGSTOP and closes connection
func registerSignalHandler(c Closer) {
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals)
		for {
			_signal := <-signals
			switch _signal {
			case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGSTOP:
				err := c.Shutdown()
				if err != nil {
					panic(err)
				}
				os.Exit(1)
			}
		}
	}()
}
