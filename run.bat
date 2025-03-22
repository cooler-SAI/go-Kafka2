@echo off
start "producer" go run producer/producer.go
start "consumer" go run consumer/consumer.go