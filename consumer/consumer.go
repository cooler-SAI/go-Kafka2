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

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})

	defer func(reader *kafka.Reader) {
		err := reader.Close()
		if err != nil {
			fmt.Println("Error closing reader:")

		}
	}(reader)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("Failed to read message: %s", err)
		}

		fmt.Printf("Received message: %s\n", string(msg.Value))
	}
}
