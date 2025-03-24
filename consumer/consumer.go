package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

func main() {
	// Настройка вывода
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()

	topic := "test-topic"
	brokerAddress := "localhost:9092"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close Kafka reader")
		}
		log.Info().Msg("Consumer stopped gracefully 🛑")
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Warn().Msg("Shutting down consumer...")
		cancel()
	}()

	log.Info().Msg("Consumer started! Waiting for messages...")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Error().Err(err).Msg("Failed to read message")
			continue
		}

		log.Info().
			Str("value", string(msg.Value)).
			Int("partition", msg.Partition).
			Int64("offset", msg.Offset).
			Msg("Received message 📩")
	}
}
