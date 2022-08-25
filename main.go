package main

import (
	"flag"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pulsar-key-shared/producer"
)

func consume(c pulsar.Client, topic, subscription, name, key string) {
	cons, err := c.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscription,
		Type:             pulsar.KeyShared,
		Name:             name,
		KeySharedPolicy: &pulsar.KeySharedPolicy{
			Mode:                    pulsar.KeySharedPolicyModeAutoSplit,
			AllowOutOfOrderDelivery: false,
		},
		// DLQ: &pulsar.DLQPolicy{
		// 	MaxDeliveries: 1,
		// 	DeadLetterTopic: "dlq",
		// },
	})
	if err != nil {
		log.Fatalf("failed to subscribe to topic stream: %v", err)
	}

	defer cons.Close()

	for msg := range cons.Chan() {
		time.Sleep(time.Second * 0)
		log.Printf("message with key: %s", msg.Key())
		// if strings.Contains(msg.Key(), name) {
		msg.Nack(msg)
		log.Printf("Nacked message with key: %s", msg.Key())
		// }
		// time.Sleep(time.Second * 1)
	}
}

func main() {
	sub := flag.String("sub", "test-sub", "subscription name")
	topic := flag.String("topic", "my-topic", "topic name")
	key := flag.String("key", "test-key", "key name")
	name := flag.String("name", "test-consumer", "consumer/producer name")
	mode := flag.String("mode", "consumer", "consumer/producer")
	count := flag.Int("count", 100, "message count")
	flag.Parse()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to init pulsar client: %s", err)
	}

	if *mode == "consumer" {
		consume(client, *topic, *sub, *name, *key)
	} else {
		producer.Produce(client, *topic, *name, *key, *count)
	}
}
