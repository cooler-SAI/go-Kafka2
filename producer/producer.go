package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
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
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Failed to close writer: %s", err)
		}
		log.Println("Producer stopped gracefully!")
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down producer...")
		cancel()
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			msg := kafka.Message{
				Key: []byte("check"),
				Value: []byte(fmt.Sprintf("Checking... [%s]",
					time.Now().Format("2006-01-02 15:04:05"))),
			}

			err := writer.WriteMessages(ctx, msg)
			if err != nil {
				log.Printf("Failed to send message: %s", err)
			} else {
				log.Printf("Sent: %s", msg.Value)
			}

		case <-ctx.Done():
			return
		}
	}
}
