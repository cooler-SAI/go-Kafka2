package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

func main() {

	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()

	topic := "test-topic"
	brokerAddress := "localhost:9092"

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close Kafka writer")
		}
		log.Info().Msg("Producer stopped gracefully 🛑")
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Warn().Msg("Shutting down producer...")
		cancel()
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Info().Msg("Producer started! Sending messages every 5 sec...")

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
				log.Error().Err(err).Msg("Failed to send message")
			} else {
				log.Info().
					Str("message", string(msg.Value)).
					Msg("Message sent ✅")
			}

		case <-ctx.Done():
			return
		}
	}
}
