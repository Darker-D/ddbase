package rabbitmq

import (
	"github.com/Darker-D/ddbase/log"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"math"
	"sync/atomic"
	"time"
)

type Producer struct {
	// Base struct for Producer
	*RabbitMQ

	// The communication channel over connection
	channel *amqp.Channel

	// A notifiyng channel for publishings
	done chan error

	// Current producer connection settings
	session Session

	// chanInterval is the next reChannel times
	chanInterval float64

	// chant reChannel ticker
	chant *time.Ticker
}

type PublishingOptions struct {
	// The key that when publishing a message to a exchange/queue will be only delivered to
	// given routing key listeners
	RoutingKey string

	// Publishing tag
	Tag string

	// Queue should be on the server/broker
	Mandatory bool

	// Consumer should be bound to server
	Immediate bool
}

// NotifyClose registers a listener for close events either initiated by an error
// accompaning a channel.close method or by a normal shutdown.
// On normal shutdowns, the chan will be closed.
// To reconnect after a transport or protocol error, we should register a listener here and
// re-connect to server
// reChannel is -not- working by now
func (p *Producer) handleErrors() {
	go func() {
	RE:
		nc := p.channel.NotifyClose(make(chan *amqp.Error))
		for {
			select {
			case <-nc:
				if (p.conn != nil && p.conn.IsClosed() && p.channel != nil) || atomic.LoadInt32(&p.isConnClosed) == 1 {
					goto RE
				}
				p.reChannel()
				goto RE
			}
		}
	}()

	go func() {
	RE:
		reconn := p.connChan
		for {
			select {
			case conn := <-reconn:
				p.conn = conn
				p.reChannel()
				atomic.SwapInt32(&p.isConnClosed, 0)
				goto RE
			}
		}
	}()
}

//
func (c *Producer) reChannel() {
	if c.conn == nil || c.conn.IsClosed() {
		return
	}
	_ = c.channel.Close()
RE:
	sleep := time.Duration(int64(math.Min(maxRetryIntervalTime, math.Pow(2, c.chanInterval)))) * time.Second
	log.Logger().Warn("rabbitmq", zap.String("producer", "channel recreating ..."), zap.Float64("sleep", sleep.Seconds()))
	c.chant.Reset(sleep)
	select {
	case <-c.chant.C:
	}
	channel, err := c.conn.Channel()
	if err != nil {
		log.Logger().Warn("rabbitmq", zap.String("producer", "channel recreated failed"))
		c.chanInterval += 1
		goto RE
	}
	c.channel = channel
	c.chanInterval = 0
	c.chant.Stop()
	log.Logger().Warn("rabbitmq", zap.String("producer", "channel recreated success"))
}

// Publish sends a Publishing from the client to an exchange on the server.
func (p *Producer) Publish(publishing amqp.Publishing) error {
	e := p.session.Exchange
	q := p.session.Queue
	po := p.session.PublishingOptions

	routingKey := po.RoutingKey
	// if exchange name is empty, this means we are gonna publish
	// this mesage to a queue, every queue has a binding to default exchange
	if e.Name == "" {
		routingKey = q.Name
	}

	err := p.channel.Publish(
		e.Name,       // publish to an exchange(it can be default exchange)
		routingKey,   // routing to 0 or more queues
		po.Mandatory, // mandatory, if no queue than err
		po.Immediate, // immediate, if no consumer than err
		publishing,
		// amqp.Publishing {
		//        // Application or exchange specific fields,
		//        // the headers exchange will inspect this field.
		//        Headers Table

		//        // Properties
		//        ContentType     string    // MIME content type
		//        ContentEncoding string    // MIME content encoding
		//        DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
		//        Priority        uint8     // 0 to 9
		//        CorrelationId   string    // correlation identifier
		//        ReplyTo         string    // address to to reply to (ex: RPC)
		//        Expiration      string    // message expiration spec
		//        MessageId       string    // message identifier
		//        Timestamp       time.Time // message timestamp
		//        Type            string    // message type name
		//        UserId          string    // creating user id - ex: "guest"
		//        AppId           string    // creating application id

		//        // The application specific payload of the message
		//        Body []byte
		// }
	)

	return err
}

// NotifyReturn captures a message when a Publishing is unable to be
// delivered either due to the `mandatory` flag set
// and no route found, or `immediate` flag set and no free consumer.
func (p *Producer) NotifyReturn(notifier func(message amqp.Return)) {
	go func() {
		for res := range p.channel.NotifyReturn(make(chan amqp.Return)) {
			notifier(res)
		}
	}()

}

// Shutdown gracefully closes all connections
func (p *Producer) Shutdown() error {
	co := p.session.ConsumerOptions
	if err := shutdownChannel(p.channel, co.Tag); err != nil {
		return err
	}

	// Since publishing is asynchronous this can happen
	// instantly without waiting for a done message.
	// defer log.Info("Producer shutdown OK")
	return nil
}

func (c *Producer) GetChannel() *amqp.Channel {
	return c.channel
}
