package main

import (
	"flag"
	"strings"

	"github.com/hieutrtr/imaginary-backup-consumer/block"
	"github.com/hieutrtr/imaginary-backup-consumer/consumer"
	"github.com/hieutrtr/imaginary-backup-consumer/s3"
)

var (
	aBrokers    = flag.String("brokers", "", "Kafka Brokers")
	aTopics     = flag.String("topics", "imaginary-upload-profile_avatar,imaginary-upload-ads,imaginary-upload-property_project", "Kafka topics")
	aGroup      = flag.String("group", "imaginary-backup", "Consumer group name")
	aType       = flag.String("type", "backup", "Backup or Restore ?")
	aService    = flag.String("service", "s3", "S3 or Ceph")
	aOffsetInit = flag.Int64("offset-init", consumer.OffsetNewest, "Newest : -1, Oldest : -2")
)

func main() {
	flag.Parse()
	if *aBrokers == "" || *aTopics == "" || *aGroup == "" {
		panic("Missing params")
	}
	config := &consumer.Config{
		Brokers:    strings.Split(*aBrokers, ","),
		Topics:     strings.Split(*aTopics, ","),
		Group:      *aGroup,
		OffsetInit: *aOffsetInit,
	}
	cons, err := consumer.NewUploadConsumer(config, func(e *consumer.Event) error {
		var err error
		if *aType == "backup" {
			err = block.Transfer(e.Topic, e.Payload)
		} else {
			if *aService == "s3" {
				err = s3.Restore(e.Topic, e.Payload)
			} else {
				err = block.Restore(e.Topic, e.Payload)
			}
		}
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
