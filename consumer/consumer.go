package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

var (
	messagesReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_consumer_messages_received_total",
			Help: "Total number of received messages",
		},
	)
	readErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_consumer_errors_total",
			Help: "Total number of read errors",
		},
	)
)

func init() {
	prometheus.MustRegister(messagesReceived, readErrors)
}

func main() {
	// Настройка логгера
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()

	// HTTP-сервер для метрик
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Info().Msg("Prometheus metrics server started on :2113")
		if err := http.ListenAndServe(":2113", nil); err != nil {
			log.Fatal().Err(err).Msg("Metrics server failed")
		}
	}()

	// Инициализация консьюмера Kafka
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "test-topic",
		GroupID:  "test-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close reader")
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Warn().Msg("Shutdown signal received")
		cancel()
	}()

	log.Info().Msg("Consumer started")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				break
			}
			readErrors.Inc()
			log.Error().Err(err).Msg("Read failed")
			continue
		}

		messagesReceived.Inc()
		log.Info().
			Str("value", string(msg.Value)).
			Int("partition", msg.Partition).
			Int64("offset", msg.Offset).
			Msg("Message processed")

		// Реальная обработка сообщения
		processMessage(msg.Value)
	}

	log.Info().Msg("Consumer stopped")
}

func processMessage(value []byte) {
	// Ваша бизнес-логика здесь
	log.Debug().Str("content", string(value)).Msg("Processing message")
}
