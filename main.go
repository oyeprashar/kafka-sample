package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	kafkaBroker = "localhost:9093"
	topic       = "example-topic"
)

func main() {
	log.Println("---> Kafka Producer and Consumer Started")

	// Create a WaitGroup to manage goroutines
	var wg sync.WaitGroup

	// Kafka Configuration
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Create a new producer
	// TODO : this is giving error
	producer, err := sarama.NewAsyncProducer([]string{kafkaBroker}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup([]string{kafkaBroker}, "example-consumer-group", config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start producer routine
	wg.Add(1)
	go func() {
		defer wg.Done()
		produceMessages(producer, topic)
	}()

	// Start consumer routine
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeMessages(ctx, consumerGroup, topic)
	}()

	// Handle graceful shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("---> Initiating graceful shutdown")
	cancel()
	wg.Wait()
	log.Println("---> Shutdown complete")
}

// Custom consumer handler
type consumerHandler struct {
	ready chan bool
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Message received: topic=%q, partition=%d, offset=%d, key=%s, value=%s\n",
			message.Topic, message.Partition, message.Offset,
			string(message.Key), string(message.Value))

		// Mark message as processed
		session.MarkMessage(message, "")
	}
	return nil
}

func produceMessages(producer sarama.AsyncProducer, topic string) {
	messages := []struct{ key, value string }{
		{"user1", "Hello Kafka 423"},
		{"user2", "Go Kafka Example"},
		{"user3", "Distributed Systems"},
	}

	for _, msg := range messages {
		producerMessage := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(msg.key),
			Value: sarama.StringEncoder(msg.value),
		}

		// Print before sending
		log.Printf("Attempting to produce message: key=%s, value=%s", msg.key, msg.value)

		producer.Input() <- producerMessage

		select {
		case success := <-producer.Successes():
			log.Printf("Message produced successfully: topic=%s, key=%s, offset=%d, timestamp=%v",
				success.Topic, msg.key, success.Offset, success.Timestamp)
		case err := <-producer.Errors():
			log.Printf("Failed to produce message: %v", err)
		}
	}

	// Wait to allow producer to send messages
	time.Sleep(2 * time.Second)
}

func consumeMessages(ctx context.Context, consumerGroup sarama.ConsumerGroup, topic string) {
	handler := &consumerHandler{ready: make(chan bool)}

	log.Printf("Starting consumer for topic: %s", topic)

	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer context cancelled")
			return
		default:
			err := consumerGroup.Consume(ctx, []string{topic}, handler)
			if err != nil {
				log.Printf("Error consuming messages: %v", err)
				return
			}
		}
	}
}
