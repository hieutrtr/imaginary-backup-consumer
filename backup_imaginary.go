package main

import "github.com/hieutrtr/imaginary-backup-consumer/consumer"
import "github.com/hieutrtr/imaginary-backup-consumer/ceph_block"

func main() {
	config := &consumer.Config{
		brokers: []string{"10.60.3.49:9092", "10.60.3.50:9092"},
		topics:  []string{"imaginary-upload-profile_avatar"},
		group:   "backup-imaginary",
	}
	cons := consumer.NewUploadConsumer(config, func(e *consumer.Event) error {
		err := ceph_block.Upload(e.Topic, e.Payload)
		if err != nil {
			return err
		}
		return nil
	})
	cons.Consume()
}
