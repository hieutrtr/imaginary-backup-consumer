package main

import (
	"flag"
	"strings"

	"github.com/hieutrtr/imaginary-backup-consumer/block"
	"github.com/hieutrtr/imaginary-backup-consumer/consumer"
)

var (
	aBrokers = flag.String("brokers", "", "Kafka Brokers")
	aTopics  = flag.String("topics", "imaginary-upload-profile_avatar,imaginary-upload-ads,imaginary-upload-property_project", "Kafka topics")
	aGroup   = flag.String("group", "imaginary-restore", "Consumer group name")
)

func main() {
	flag.Parse()
	if *aBrokers == "" || *aTopics == "" || *aGroup == "" {
		panic("Missing params")
	}
	config := &consumer.Config{
		Brokers: strings.Split(*aBrokers, ","),
		Topics:  strings.Split(*aTopics, ","),
		Group:   *aGroup,
	}
	cons, err := consumer.NewUploadConsumer(config, func(e *consumer.Event) error {
		err := block.Restore(e.Topic, e.Payload)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	cons.Consume()
}
