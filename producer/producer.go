package main

import (
	"context"
	"fmt"
	"math/rand"
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
	messagesSent = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_producer_messages_sent_total",
			Help: "Total number of sent messages",
		},
	)
	sendErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_producer_errors_total",
			Help: "Total number of send errors",
		},
	)
	messageRate = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_producer_message_rate",
			Help: "Current message generation rate (messages/sec)",
		},
	)
)

func init() {
	prometheus.MustRegister(messagesSent, sendErrors, messageRate)
}

func main() {
	// Настройка логгера
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()

	// HTTP-сервер для метрик
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Info().Msg("Starting metrics server on :2112")
		http.ListenAndServe(":2112", nil)
	}()

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	rand.Seed(time.Now().UnixNano())
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Info().Msg("Starting dynamic message producer")

	for {
		select {
		case <-ticker.C:
			// Динамическое изменение частоты сообщений
			rate := 3 + rand.Intn(7) // Случайное значение 3-9 сообщений/сек
			messageRate.Set(float64(rate))

			for i := 0; i < rate; i++ {
				msg := kafka.Message{
					Value: []byte(fmt.Sprintf("Message %d-%d", time.Now().Unix(), i)),
				}

				if err := writer.WriteMessages(ctx, msg); err != nil {
					sendErrors.Inc()
					log.Error().Err(err).Msg("Send failed")
				} else {
					messagesSent.Inc()
					log.Info().
						Str("message", string(msg.Value)).
						Int("rate", rate).
						Msg("Message sent")
				}
			}

		case <-ctx.Done():
			log.Info().Msg("Producer stopped")
			return
		}
	}
}
