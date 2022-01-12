package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	StartConsumers()
}

func StartConsumers() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "fullcyclekafka_kafka_1:9092",
		"client.id":         "app-consumer",
		"group.id":          "app-group",
		"auto.offset.reset": "earliest",
	}

	c, err := kafka.NewConsumer(configMap)

	if err != nil {
		log.Fatal("Deu merlin")
	}

	topics := []string{"teste"}

	c.SubscribeTopics(topics, nil)

	for {
		msg, err := c.ReadMessage(-1)

		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		} else {
			log.Fatal("Deu merlin")
		}
	}
}
