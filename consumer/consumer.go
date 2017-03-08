package consumer

import (
	"fmt"

	cluster "github.com/bsm/sarama-cluster"
)

type Event struct {
	Topic   string
	Payload string
}

// Config info need to consume kafka
type Config struct {
	Brokers []string
	Topics  []string
	Group   string
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
	process  FnProcess
}

const UploadEventPrefix = "imaginary-upload-"

// Consume upload events
func (c *UploadConsumer) Consume() {
	for {
		select {
		case msg := <-c.consumer.Messages():
			err := c.process(&Event{
				Topic:   UploadEventPrefix + msg.Topic,
				Payload: string(msg.Value),
			})
			if err != nil {
				fmt.Println(err)
			}
			c.consumer.MarkOffset(msg, "") // mark message as processed
		}
	}
}

// NewUploadConsumer create consumer of upload event
func NewUploadConsumer(c *Config, fn FnProcess) Consumer {

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	cons, err := cluster.NewConsumer(c.Brokers, c.Group, c.Topics, config)
	if err != nil {
		panic(err)
	}
	return &UploadConsumer{
		consumer: cons,
		process:  fn,
	}
}
