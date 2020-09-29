package main

import (
	"context"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

func startKafka() {

	config := kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		GroupID:     "test-topic-id-2",
		Topic:       "CARD_PGT",
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.LastOffset,
	}

	reader := kafka.NewReader(config)

	for {
		m, err := reader.ReadMessage(context.Background())

		if err != nil {
			fmt.Println("erro", err)
			continue
		}

		fmt.Println("Message is: ", string(m.Value))

	}
}

func main() {
	fmt.Println("start consuming ... !!")
	startKafka()
}
