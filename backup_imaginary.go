package main

import (
	"github.com/hieutrtr/imaginary-backup-consumer/ceph/obj2block"
	"github.com/hieutrtr/imaginary-backup-consumer/kafka/consumer"
)

func main() {
	config := &consumer.Config{
		brokers: []string{"10.60.3.49:9092", "10.60.3.50:9092"},
		topics:  []string{"imaginary-upload-profile_avatar"},
		group:   "backup-imaginary",
	}
	cons := consumer.NewUploadConsumer(config, func(e *consumer.Event) error {
		err := obj2block.Transfer(e.Topic, e.Payload)
		if err != nil {
			return err
		}
		return nil
	})
	cons.Consume()
}
