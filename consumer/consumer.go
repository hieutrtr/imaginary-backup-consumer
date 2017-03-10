package consumer

import (
	"fmt"
	"strings"

	cluster "github.com/bsm/sarama-cluster"
)

const (
	// OffsetNewest stands for the log head offset, i.e. the offset that will be
	// assigned to the next message that will be produced to the partition. You
	// can send this to a client's GetOffset method to get this offset, or when
	// calling ConsumePartition to start consuming new messages.
	OffsetNewest int64 = -1
	// OffsetOldest stands for the oldest offset available on the broker for a
	// partition. You can send this to a client's GetOffset method to get this
	// offset, or when calling ConsumePartition to start consuming from the
	// oldest offset that is still available on the broker.
	OffsetOldest int64 = -2
)

type Event struct {
	Topic   string
	Payload string
}

// Config info need to consume kafka
type Config struct {
	Brokers    []string
	Topics     []string
	Group      string
	OffsetInit int64
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

const uploadEventPrefix = "imaginary-upload-"

// Consume upload events
func (c *UploadConsumer) Consume() {
	for {
		select {
		case msg := <-c.consumer.Messages():
			err := c.process(&Event{
				Topic:   strings.Replace(msg.Topic, uploadEventPrefix, "", -1),
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
func NewUploadConsumer(c *Config, fn FnProcess) (Consumer, error) {

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = c.OffsetInit
	config.Group.Return.Notifications = true

	cons, err := cluster.NewConsumer(c.Brokers, c.Group, c.Topics, config)
	if err != nil {
		return nil, err
	}
	cons.csmr.ConsumePartition
	return &UploadConsumer{
		consumer: cons,
		process:  fn,
	}, nil
}
