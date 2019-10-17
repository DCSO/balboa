// balboa
// Copyright (c) 2018, DCSO GmbH

package feeder

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"balboa/format"
	"balboa/observation"

	"github.com/NeowayLabs/wabbit"
	amqp "github.com/NeowayLabs/wabbit/amqp"
	log "github.com/sirupsen/logrus"
	origamqp "github.com/streadway/amqp"
)

// Consumer reads and processes messages from a fake RabbitMQ server.
type Consumer struct {
	conn                wabbit.Conn
	channel             wabbit.Channel
	tag                 string
	exchanges           []string
	URL                 string
	done                chan error
	stop                chan bool
	deliveries          <-chan wabbit.Delivery
	Callback            func(wabbit.Delivery)
	StopReconnection    chan bool
	ChanMutex           sync.Mutex
	ConnMutex           sync.Mutex
	OutChan             chan observation.InputObservation
	MakeObservationFunc format.MakeObservationFunc
	ErrorChan           chan wabbit.Error
	Reconnector         func(string) (wabbit.Conn, string, error)
	Connector           func(*Consumer) error
}

func decompressBody(d wabbit.Delivery) ([]byte, error) {
	var compressedBuffer bytes.Buffer
	compressedBuffer.Write(d.Body())
	compressedReader, err := gzip.NewReader(&compressedBuffer)
	if err != nil {
		return nil, err
	}
	decompressedWriter := new(bytes.Buffer)
	io.Copy(decompressedWriter, compressedReader)
	compressedReader.Close()
	body := decompressedWriter.Bytes()
	return body, nil
}

const amqpReconnDelay = 2 * time.Second

func reconnectOnFailure(s *Consumer) {
	for {
		select {
		case <-s.StopReconnection:
			return
		case rabbitErr := <-s.ErrorChan:
			if rabbitErr != nil {
				log.Warnf("RabbitMQ connection failed: %s", rabbitErr.Reason())
				close(s.stop)
				for {
					time.Sleep(amqpReconnDelay)
					connErr := s.Connector(s)
					if connErr != nil {
						log.Warnf("RabbitMQ error: %s", connErr)
					} else {
						log.Infof("Reestablished connection to %s", s.URL)
						s.ConnMutex.Lock()
						s.conn.NotifyClose(s.ErrorChan)
						s.ConnMutex.Unlock()
						go handle(s.deliveries, s.stop, s.done, s.OutChan, s.MakeObservationFunc)
						break
					}
				}
			}
		}
	}
}

// NewConsumerWithReconnector creates a new consumer with the given properties. The callback
// function is called for each delivery accepted from a consumer channel.
func (f *AMQPFeeder) NewConsumerWithReconnector(amqpURI string, exchanges []string, exchangeType,
	queueName, key, ctag string, out chan observation.InputObservation,
	reconnector func(string) (wabbit.Conn, string, error)) (*Consumer, error) {
	var err error
	c := &Consumer{
		conn:                nil,
		channel:             nil,
		exchanges:           exchanges,
		URL:                 amqpURI,
		tag:                 ctag,
		done:                make(chan error),
		stop:                make(chan bool),
		OutChan:             out,
		MakeObservationFunc: f.MakeObservationFunc,
		Reconnector:         reconnector,
		StopReconnection:    make(chan bool),
	}

	c.Connector = func(s *Consumer) error {
		var err error
		var exchangeType string

		s.ConnMutex.Lock()
		s.conn, exchangeType, err = s.Reconnector(s.URL)
		s.ConnMutex.Unlock()
		if err != nil {
			return err
		}
		s.ChanMutex.Lock()
		s.channel, err = s.conn.Channel()
		s.ChanMutex.Unlock()
		if err != nil {
			s.ConnMutex.Lock()
			s.conn.Close()
			s.ConnMutex.Unlock()
			return err
		}

		for _, exchange := range exchanges {
			// We do not want to declare an exchange on non-default connection methods,
			// as they may not support all exchange types. For instance amqptest does
			// not support 'fanout'.
			log.Debug("declaring exchange ", exchange)
			err = s.channel.ExchangeDeclare(
				exchange,
				exchangeType,
				wabbit.Option{
					"durable":    true,
					"autoDelete": false,
					"internal":   false,
					"noWait":     false,
				},
			)
			if err != nil {
				log.Error(err)
				s.ChanMutex.Lock()
				s.channel.Close()
				s.ChanMutex.Unlock()
				s.ConnMutex.Lock()
				s.conn.Close()
				s.ConnMutex.Unlock()
				return err
			}
		}
		queueName := fmt.Sprintf("%s.%s", strings.Join(exchanges, "."), queueName)
		queue, err := c.channel.QueueDeclare(
			queueName,
			wabbit.Option{
				"durable":    false,
				"autoDelete": true,
				"exclusive":  true,
				"noWait":     false,
				"args": origamqp.Table{
					"x-message-ttl":      int32(300000),
					"x-max-length-bytes": int32(100 * 1024 * 1024),
				},
			},
		)
		if err != nil {
			return fmt.Errorf("error queue declare: %s", err)
		}
		log.Debugf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
			queue.Name(), queue.Messages(), queue.Consumers(), key)

		for _, exchange := range exchanges {
			log.Debug("binding to exchange ", exchange)
			if err = c.channel.QueueBind(
				queue.Name(),
				key,
				exchange,
				wabbit.Option{
					"noWait": false,
				},
			); err != nil {
				return fmt.Errorf("error queue bind: %s", err)
			}
		}

		log.Debugf("queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
		c.deliveries, err = c.channel.Consume(
			queue.Name(),
			c.tag,
			wabbit.Option{
				"autoAck":   false,
				"exclusive": false,
				"noLocal":   false,
				"noWait":    false,
			},
		)
		if err != nil {
			return fmt.Errorf("error queue consume: %s", err)
		}

		log.Debugf("consumer established connection to %s", s.URL)
		s.stop = make(chan bool)
		c.ErrorChan = make(chan wabbit.Error)

		return nil
	}

	c.ErrorChan = make(chan wabbit.Error)
	err = c.Connector(c)
	if err != nil {
		return nil, err
	}
	c.conn.NotifyClose(c.ErrorChan)

	go reconnectOnFailure(c)
	go handle(c.deliveries, c.stop, c.done, out, f.MakeObservationFunc)

	return c, nil
}

func defaultReconnector(amqpURI string) (wabbit.Conn, string, error) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		return nil, "fanout", err
	}
	return conn, "fanout", err
}

// NewConsumer returns a new Consumer.
func (f *AMQPFeeder) NewConsumer(amqpURI string, exchanges []string, exchangeType, queueName, key,
	ctag string, out chan observation.InputObservation) (*Consumer, error) {
	return f.NewConsumerWithReconnector(amqpURI, exchanges, exchangeType, queueName, key,
		ctag, out, defaultReconnector)
}

// Shutdown shuts down a consumer, closing down its channels and connections.
func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Close(); err != nil {
		return fmt.Errorf("channel close failed: %s", err)
	}
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}
	defer log.Debugf("AMQP shutdown OK")
	// wait for handle() to exit
	return <-c.done
}

func handle(deliveries <-chan wabbit.Delivery, stop chan bool, done chan error,
	out chan observation.InputObservation, fn format.MakeObservationFunc) {
	for {
		select {
		case <-stop:
			done <- nil
			return
		case d := <-deliveries:
			if d == nil {
				done <- nil
				return
			}
			log.Infof("got %d bytes via AMQP", len(d.Body()))
			raw := d.Body()
			if _, ok := d.Headers()["compressed"]; ok {
				var err error
				raw, err = decompressBody(d)
				if err != nil {
					log.Warn(err)
					continue
				}
			}
			var sensorID string
			if _, ok := d.Headers()["sensor_id"]; ok {
				sensorID = d.Headers()["sensor_id"].(string)
			}
			err := fn(raw, sensorID, out, stop)
			if err != nil {
				log.Warn(err)
			}
			d.Ack(true)
		}
	}

}

// AMQPFeeder is a Feeder that accepts input via AMQP queues.
type AMQPFeeder struct {
	StopChan            chan bool
	StoppedChan         chan bool
	IsRunning           bool
	Consumer            *Consumer
	URL                 string
	Exchanges           []string
	Queue               string
	MakeObservationFunc format.MakeObservationFunc
}

// MakeAMQPFeeder returns a new AMQPFeeder, connecting to the AMQP server at
// the given URL, creating a new queue with the given name bound to the
// provided exchanges.
func MakeAMQPFeeder(url string, exchanges []string, queue string) *AMQPFeeder {
	return &AMQPFeeder{
		IsRunning:           false,
		StopChan:            make(chan bool),
		Exchanges:           exchanges,
		URL:                 url,
		Queue:               queue,
		MakeObservationFunc: format.MakeFeverAggregateInputObservations,
	}
}

// SetInputDecoder states that the given MakeObservationFunc should be used to
// parse and decode data delivered to this feeder.
func (f *AMQPFeeder) SetInputDecoder(fn format.MakeObservationFunc) {
	f.MakeObservationFunc = fn
}

// Run starts the feeder.
func (f *AMQPFeeder) Run(out chan observation.InputObservation) error {
	var err error
	f.Consumer, err = f.NewConsumer(f.URL, f.Exchanges, "fanout", f.Queue, "", "balboa", out)
	if err != nil {
		return err
	}
	f.IsRunning = true
	return nil
}

// Stop causes the feeder to stop accepting deliveries and close all
// associated channels, including the passed notification channel.
func (f *AMQPFeeder) Stop(stopChan chan bool) {
	close(stopChan)
	if f.IsRunning {
		f.Consumer.Shutdown()
	}
}
