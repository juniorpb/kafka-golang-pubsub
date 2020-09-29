package main

import (
	"context"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}

func main() {

	kafkaURL := "localhost:9092"
	topic := "CARD_PGT"
	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()
	fmt.Println("start producing ... !!")

	for i := 0; ; i++ {
		msg := kafka.Message{
			//Key:   []byte(fmt.Sprintf("Key-%d", i)),
			Value: []byte(fmt.Sprint("aaaaa")),
		}

		err := writer.WriteMessages(context.Background(), msg)

		if err != nil {
			fmt.Println("Erro ao escrever mensagem", err)
		}

		time.Sleep(1 * time.Second)
	}
}
