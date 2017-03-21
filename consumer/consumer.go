package consumer

import (
	"fmt"
	"os"
	"strings"

	cluster "github.com/bsm/sarama-cluster"
)

const (
	OffsetNewest int64 = -1
	OffsetOldest int64 = -2
)

var (
	kafkaBrokers   = os.Getenv("KAFKA_BROKERS")
	kafkaTopics    = os.Getenv("KAFKA_TOPICS")
	kafkaConsGroup = os.Getenv("KAFKA_CONSUMER_GROUP")
)

type Event struct {
	Topic   string
	Payload string
}

// FnProcess function to process consumer's events
type FnProcess func(*Event) error

// Consumer use to consume upload events
type Consumer struct {
	consumer *cluster.Consumer
	process  FnProcess
}

const uploadEventPrefix = "imaginary-upload-"

// Consume upload events
func (c *Consumer) Consume() {
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
func NewUploadConsumer(offsetInit int64, fn FnProcess) *Consumer {
	if kafkaBrokers == "" {
		exitWithError("Missing KAFKA_BROKERS env")
	}
	if kafkaTopics == "" {
		exitWithError("Missing KAFKA_TOPICS env")
	}
	if kafkaConsGroup == "" {
		exitWithError("Missing KAFKA_CONSUMER_GROUP env")
	}

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = offsetInit
	config.Group.Return.Notifications = true

	cons, err := cluster.NewConsumer(strings.Split(kafkaBrokers, ","), kafkaConsGroup, strings.Split(kafkaTopics, ","), config)
	if err != nil {
		exitWithError("Cannot start consumer with error " + fmt.Sprint(err))
	}
	return &Consumer{
		consumer: cons,
		process:  fn,
	}
}

func exitWithError(mess string) {
	panic("kafka_consumer: " + mess)
}
