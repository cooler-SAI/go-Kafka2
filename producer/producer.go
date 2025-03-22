package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "test-topic"
	brokerAddress := "localhost:9092"

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer func(writer *kafka.Writer) {
		err := writer.Close()
		if err != nil {

		}
	}(writer)

	msg := kafka.Message{
		Key:   []byte("key"),
		Value: []byte("Hello! How are you?"),
	}

	err := writer.WriteMessages(context.Background(), msg)
	if err != nil {
		log.Fatalf("Failed to write message: %s", err)
	}

	fmt.Println("Message sent successfully!")
}
