package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"
)

var (
	monolithURL                  string
	moviesURL                    string
	eventsURL                    string
	gradualMigration             bool
	moviesMigrationPercent       int
	usersMigrationPercent        int
	paymentsMigrationPercent     int
	subscriptionMigrationPercent int
)

func main() {
	var err error
	monolithURL = os.Getenv("MONOLITH_URL")
	moviesURL = os.Getenv("MOVIES_SERVICE_URL")
	eventsURL = os.Getenv("EVENTS_SERVICE_URL")
	gradualMigration, err = strconv.ParseBool(os.Getenv("GRADUAL_MIGRATION"))
	if err != nil {
		log.Fatal("не удалось распарсить GRADUAL_MIGRATION:", err)
	}
	moviesMigrationPercent, err = strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
	if err != nil {
		log.Fatal("не удалось распарсить MOVIES_MIGRATION_PERCENT:", err)
	}

	rand.Seed(time.Now().UnixNano())

	// Set up HTTP routes
	http.HandleFunc("/api/movies", handleMovies)
	http.HandleFunc("/api/movies/health", handleHealth)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/api/users", handleUsers)
	http.HandleFunc("/api/payments", handlePayments)
	http.HandleFunc("/api/subscriptions", handleSubscriptions)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Printf("Starting movies microservice on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func ProxyRequest(targetUrl string, w http.ResponseWriter, r *http.Request) {
	target, err := url.Parse(targetUrl)
	if err != nil {
		http.Error(w, "Некорректный URL целевого сервиса", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ServeHTTP(w, r)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

// Movie handlers
func handleMovies(w http.ResponseWriter, r *http.Request) {
	var targetURL string
	if gradualMigration &&
		moviesMigrationPercent > 0 &&
		rand.Intn(100) < moviesMigrationPercent {
		targetURL = moviesURL
	} else {
		targetURL = monolithURL
	}
	ProxyRequest(targetURL, w, r)
}

func handleUsers(w http.ResponseWriter, r *http.Request) {
	var targetURL string
	if gradualMigration &&
		usersMigrationPercent > 0 &&
		rand.Intn(100) < usersMigrationPercent {
		targetURL = moviesURL
	} else {
		targetURL = monolithURL
	}
	ProxyRequest(targetURL, w, r)
}

func handlePayments(w http.ResponseWriter, r *http.Request) {
	var targetURL string
	if gradualMigration &&
		paymentsMigrationPercent > 0 &&
		rand.Intn(100) < paymentsMigrationPercent {
		targetURL = moviesURL
	} else {
		targetURL = monolithURL
	}
	ProxyRequest(targetURL, w, r)
}

func handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	var targetURL string
	if gradualMigration &&
		subscriptionMigrationPercent > 0 &&
		rand.Intn(100) < subscriptionMigrationPercent {
		targetURL = moviesURL
	} else {
		targetURL = monolithURL
	}
	ProxyRequest(targetURL, w, r)
}
