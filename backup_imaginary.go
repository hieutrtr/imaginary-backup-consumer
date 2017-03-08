package main

import (
	"github.com/hieutrtr/imaginary-backup-consumer/block"
	"github.com/hieutrtr/imaginary-backup-consumer/consumer"
)

func main() {
	config := &consumer.Config{
		Brokers: []string{"10.60.3.493:9092", "10.60.33.50:9092"},
		Topics:  []string{"imaginary-upload-profile_avatar", "imaginary-upload-ads", "imaginary-upload-property_project"},
		Group:   "backup-imaginary",
	}
	cons, err := consumer.NewUploadConsumer(config, func(e *consumer.Event) error {
		err := block.Transfer(e.Topic, e.Payload)
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
