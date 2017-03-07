package consumer

import (
	"fmt"
	"os"

	cluster "github.com/bsm/sarama-cluster"
)

type Event struct {
	Topic   string
	Payload string
}

// Config info need to consume kafka
type Config struct {
	brokers []string
	topics  []string
	group   string
}

// Consumer interface of consumers
type Consumer interface {
	Consume()
}

// FnProcess function to process consumer's events
type FnProcess func(*Event) error

// UploadConsumer use to consume upload events
type UploadConsumer struct {
	consumer *cluster.Consumer
	process  fnProcess
}

// Consume upload events
func (c *UploadConsumer) Consume() {
	for {
		select {
		case msg := <-c.consumer.Messages():
			fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
			err := c.process(&Event{
				Topic:   msg.Topic,
				Payload: msg.Value,
			})
			if err == nil {
				c.consumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-signals:
			return
		}
	}
}

// NewUploadConsumer create consumer of upload event
func NewUploadConsumer(c *Config, fn FnProcess) Consumer {

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	cons, err := cluster.NewConsumer(c.brokers, c.group, c.topics, config)
	if err != nil {
		panic(err)
	}
	return &UploadConsumer{
		consumer: cons,
		process:  fn,
	}
}
