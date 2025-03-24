package main

import (
	"context"
	"fmt"
	"log"
	"time"

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
			fmt.Println("Error closing writer:")
		}
	}(writer)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			msg := kafka.Message{
				Key:   []byte("check"),
				Value: []byte(fmt.Sprintf("Checking... [%s]", time.Now().Format("2006-01-02 15:04:05"))),
			}

			err := writer.WriteMessages(context.Background(), msg)
			if err != nil {
				log.Printf("Failed to send message: %s", err)
			} else {
				log.Printf("Sent: %s", msg.Value)
			}
		}
	}
}
