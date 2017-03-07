package main

import (
	"github.com/hieutrtr/imaginary-backup-consumer/block"
	"github.com/hieutrtr/imaginary-backup-consumer/consumer"
)

func main() {
	config := &consumer.Config{
		Brokers: []string{"10.60.3.49:9092", "10.60.3.50:9092"},
		Topics:  []string{"profile_avatar"},
		Group:   "backup-imaginary",
	}
	cons := consumer.NewUploadConsumer(config, func(e *consumer.Event) error {
		err := block.Transfer(e.Topic, e.Payload)
		if err != nil {
			return err
		}
		return nil
	})
	cons.Consume()
}
