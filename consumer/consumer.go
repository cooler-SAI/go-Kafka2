package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "test-topic"
	brokerAddress := "localhost:9092"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Failed to close reader: %s", err)
		}
		log.Println("Consumer stopped gracefully!")
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down consumer...")
		cancel()
	}()

	log.Println("Consumer started. Waiting for messages...")
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("Failed to read message: %s", err)
			continue
		}

		log.Printf("Received: %s (Partition: %d, Offset: %d)",
			string(msg.Value),
			msg.Partition,
			msg.Offset,
		)
	}
}
