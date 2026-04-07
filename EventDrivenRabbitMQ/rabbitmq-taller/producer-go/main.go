package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchangeName = "events"
	routingKey   = "user.action"
)

type Event struct {
	Producer  string  `json:"producer"`
	Event     string  `json:"event"`
	UserID    string  `json:"userId"`
	Action    string  `json:"action"`
	Score     float64 `json:"score"`
	Timestamp string  `json:"timestamp"`
	Seq       int     `json:"seq"`
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func connectWithRetry(url string, retries int, delay time.Duration) (*amqp.Connection, error) {
	for i := 1; i <= retries; i++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		log.Printf("[Go Producer] Attempt %d/%d failed: %v. Retrying in %v...", i, retries, err, delay)
		time.Sleep(delay)
	}
	return nil, fmt.Errorf("could not connect after %d retries", retries)
}

func main() {
	host := getenv("RABBITMQ_HOST", "rabbitmq")
	port := getenv("RABBITMQ_PORT", "5672")
	user := getenv("RABBITMQ_USER", "guest")
	pass := getenv("RABBITMQ_PASS", "guest")

	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, pass, host, port)

	log.Println("[Go Producer] Connecting to RabbitMQ...")
	conn, err := connectWithRetry(url, 10, 3*time.Second)
	if err != nil {
		log.Fatalf("[Go Producer] %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("[Go Producer] Failed to open channel: %v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("[Go Producer] Failed to declare exchange: %v", err)
	}
	log.Printf("[Go Producer] Connected. Publishing to exchange='%s' key='%s'", exchangeName, routingKey)

	actions := []string{"login", "purchase", "view", "logout", "search"}
	counter := 1

	for {
		// Score: float con 2 decimales sin necesidad de math.Round
		score := float64(rand.Intn(10000)) / 100.0

		event := Event{
			Producer:  "go",
			Event:     "user.action",
			UserID:    fmt.Sprintf("USR-%04d", rand.Intn(9000)+1000),
			Action:    actions[rand.Intn(len(actions))],
			Score:     score,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Seq:       counter,
		}

		body, err := json.Marshal(event)
		if err != nil {
			log.Printf("[Go Producer] Marshal error: %v", err)
			continue
		}

		err = ch.Publish(
			exchangeName,
			routingKey,
			false, false,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				Body:         body,
			},
		)
		if err != nil {
			log.Printf("[Go Producer] Publish error: %v", err)
		} else {
			log.Printf("[Go Producer] Sent #%d: %s", counter, string(body))
		}

		counter++
		time.Sleep(2 * time.Second)
	}
}
