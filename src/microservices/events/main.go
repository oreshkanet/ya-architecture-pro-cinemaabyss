package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

// Models
type MovieEvent struct {
	MovieId int    `json:"movie_id"`
	Title   string `json:"title"`
	Action  string `json:"action"`
	UserId  int    `json:"user_id"`
}

type UserEvent struct {
	UserId    int    `json:"user_id"`
	UserName  string `json:"username"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentId  int     `json:"payment_id"`
	UserId     int     `json:"user_id"`
	Amount     float32 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type"`
}

func main() {

	kafkaBrokers := []string{getEnv("KAFKA_BROKERS", "localhost:9092")}

	topicUser := "events-user"
	topicMovie := "events-movie"
	topicPayment := "events-payment"

	handleConsumerMovies(kafkaBrokers, topicMovie, "movies-service-group")
	handleConsumerUser(kafkaBrokers, topicUser, "user-service-group")
	handleConsumerPayment(kafkaBrokers, topicPayment, "payment-service-group")

	// Set up HTTP routes
	http.HandleFunc("/api/events/movie", func(w http.ResponseWriter, r *http.Request) {
		handleEventMovie(w, r, kafkaProducer(kafkaBrokers, topicMovie))
	})
	http.HandleFunc("/api/events/user", func(w http.ResponseWriter, r *http.Request) {
		handleEventUser(w, r, kafkaProducer(kafkaBrokers, topicUser))
	})
	http.HandleFunc("/api/events/payment", func(w http.ResponseWriter, r *http.Request) {
		handleEventPayment(w, r, kafkaProducer(kafkaBrokers, topicPayment))
	})
	http.HandleFunc("/api/events/health", handleHealth)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082" // Note: Using a different port than the monolith
	}
	log.Printf("Starting movies microservice on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func handleConsumerMovies(kafkaBrokers []string, topic, group string) {
	consumer := kafkaConsumer(kafkaBrokers, topic, group)
	go func() {
		for rawMsg := range consumer {
			var event MovieEvent
			if err := json.Unmarshal(rawMsg, &event); err != nil {
				log.Printf("Ошибка парсинга JSON из Kafka: %v | данные: %s", err, string(rawMsg))
				continue
			}
			// Логируем структурированное событие
			log.Printf("Получено событие из Kafka: movie_id=%d, title=%q, action=%q, user_id=%d",
				event.MovieId, event.Title, event.Action, event.UserId)
		}
	}()
}

func handleConsumerUser(kafkaBrokers []string, topic, group string) {
	consumer := kafkaConsumer(kafkaBrokers, topic, group)
	go func() {
		for rawMsg := range consumer {
			var event UserEvent
			if err := json.Unmarshal(rawMsg, &event); err != nil {
				log.Printf("Ошибка парсинга JSON из Kafka: %v | данные: %s", err, string(rawMsg))
				continue
			}
			// Логируем структурированное событие
			log.Printf("Получено событие из Kafka: user_id=%d, user_name=%q, action=%q, timestamp=%d",
				event.UserId, event.UserName, event.Action, event.Timestamp)
		}
	}()
}

func handleConsumerPayment(kafkaBrokers []string, topic, group string) {
	consumer := kafkaConsumer(kafkaBrokers, topic, group)
	go func() {
		for rawMsg := range consumer {
			var event PaymentEvent
			if err := json.Unmarshal(rawMsg, &event); err != nil {
				log.Printf("Ошибка парсинга JSON из Kafka: %v | данные: %s", err, string(rawMsg))
				continue
			}
			// Логируем структурированное событие
			log.Printf("Получено событие из Kafka: payment_id=%d, user_id=%d, amount=%q, status=%q, timestamp=%q, method_type=%q",
				event.PaymentId, event.UserId, event.Amount, event.Status, event.Timestamp, event.MethodType)
		}
	}()
}

func kafkaProducer(brokers []string, topic string) func(context.Context, []byte) error {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false, // синхронная отправка для надёжности
	}
	return func(ctx context.Context, msg []byte) error {
		return writer.WriteMessages(ctx, kafka.Message{
			Value: msg,
		})
	}
}

func kafkaConsumer(brokers []string, topic string, groupID string) <-chan []byte {
	messages := make(chan []byte, 100)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	go func() {
		defer close(messages)
		defer reader.Close()
		for {
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Ошибка при чтении из Kafka: %v", err)
				return
			}
			messages <- msg.Value
		}
	}()

	return messages
}

// Handlers

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleEventMovie(w http.ResponseWriter, r *http.Request, sendToKafka func(context.Context, []byte) error) {
	var m MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	payload, err := json.Marshal(m)
	if err != nil {
		http.Error(w, "Failed to serialize event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Отправляем в Kafka
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sendToKafka(ctx, payload); err != nil {
		log.Printf("Ошибка отправки в Kafka: %v", err)
		http.Error(w, "Failed to publish event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(m)
}

func handleEventUser(w http.ResponseWriter, r *http.Request, sendToKafka func(context.Context, []byte) error) {
	var m UserEvent
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	payload, err := json.Marshal(m)
	if err != nil {
		http.Error(w, "Failed to serialize event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Отправляем в Kafka
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sendToKafka(ctx, payload); err != nil {
		log.Printf("Ошибка отправки в Kafka: %v", err)
		http.Error(w, "Failed to publish event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(m)
}

func handleEventPayment(w http.ResponseWriter, r *http.Request, sendToKafka func(context.Context, []byte) error) {
	var m PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	payload, err := json.Marshal(m)
	if err != nil {
		http.Error(w, "Failed to serialize event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Отправляем в Kafka
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sendToKafka(ctx, payload); err != nil {
		log.Printf("Ошибка отправки в Kafka: %v", err)
		http.Error(w, "Failed to publish event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(m)
}
