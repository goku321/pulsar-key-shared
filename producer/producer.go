package producer

import (
	"context"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

func noop(id pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
	if err != nil {
		log.Println("message produced!")
	}
}

func Produce(c pulsar.Client, topic, name, key string, count int) {
	p, err := c.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
		Name: name,
		DisableBatching: true,
	})
	if err != nil {
		log.Fatalf("failed to create producer: %s", err)
	}

	for i := 0; i < count; i++ {
		// p.SendAsync(context.Background(), &pulsar.ProducerMessage{
		// 	Payload: []byte("hello pulsar"),
		// 	Key: key,
		// }, noop)

		if _, err := p.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("hello pulsar"),
			Key: key,
		}); err != nil {
			log.Fatal("failed to produce message")
		}
	}
}