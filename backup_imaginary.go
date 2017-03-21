package main

import (
	"flag"

	"github.com/hieutrtr/imaginary-backup-consumer/block"
	"github.com/hieutrtr/imaginary-backup-consumer/consumer"
)

var (
	aType       = flag.String("type", "backup", "Backup or Restore ?")
	aService    = flag.String("service", "ceph", "S3 or Ceph")
	aOffsetInit = flag.Int64("offset-init", consumer.OffsetNewest, "Newest : -1, Oldest : -2")
)

func main() {
	flag.Parse()
	var err error
	cons := consumer.NewUploadConsumer(*aOffsetInit, func(e *consumer.Event) error {
		if *aType == "backup" {
			err = block.Transfer(e.Topic, e.Payload)
		} else {
			err = block.Restore(e.Topic, e.Payload)
		}
		if err != nil {
			return err
		}
		return nil
	})
	cons.Consume()
}
