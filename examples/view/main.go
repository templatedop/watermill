package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"
)

// This example demonstrates using Views to query stateful data
// Views provide read-only access to group tables, perfect for APIs

type OrderStats struct {
	CustomerID   string  `json:"customer_id"`
	TotalOrders  int     `json:"total_orders"`
	TotalSpent   float64 `json:"total_spent"`
	LastOrderAt  string  `json:"last_order_at"`
	AverageOrder float64 `json:"average_order"`
}

var statsView *kafka.TypedView[OrderStats]

func main() {
	logger := watermill.NewStdLogger(true, true)

	// Create Kafka client
	config := kafka.EcommerceConfig(
		[]string{"localhost:9092"},
		"stats-view-consumer",
	)
	config.Logger = logger

	client, err := kafka.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create view for order stats table
	statsView, err = kafka.NewTypedView[OrderStats](
		client,
		"order-stats-table", // Same table as the processor
		kafka.NewJSONCodec(),
		kafka.NewMemoryStorage(),
	)
	if err != nil {
		log.Fatalf("Failed to create view: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start view
	if err := statsView.Start(ctx); err != nil {
		log.Fatalf("Failed to start view: %v", err)
	}

	log.Println("Waiting for view to sync...")
	statsView.WaitReady()
	log.Println("View is ready!")

	// Start HTTP server to query stats
	http.HandleFunc("/stats/", handleGetStats)
	http.HandleFunc("/stats", handleListStats)

	server := &http.Server{Addr: ":8080"}

	go func() {
		log.Println("HTTP server listening on :8080")
		log.Println("Try: curl http://localhost:8080/stats/CUST-1")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v", err)
		}
	}()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)

	statsView.Stop()
}

// handleGetStats returns stats for a specific customer
func handleGetStats(w http.ResponseWriter, r *http.Request) {
	// Extract customer ID from path
	customerID := r.URL.Path[len("/stats/"):]
	if customerID == "" {
		http.Error(w, "Customer ID required", http.StatusBadRequest)
		return
	}

	// Query view
	stats, err := statsView.Get(customerID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
		return
	}

	if stats == nil {
		http.Error(w, "Customer not found", http.StatusNotFound)
		return
	}

	// Return JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleListStats returns all customer stats
func handleListStats(w http.ResponseWriter, r *http.Request) {
	iterator, err := statsView.Iterator()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
		return
	}
	defer iterator.Close()

	allStats := make(map[string]*OrderStats)

	for iterator.Next() {
		key := iterator.Key()

		var stats OrderStats
		if err := kafka.NewJSONCodec().Decode(iterator.Value(), &stats); err != nil {
			log.Printf("Failed to decode stats for %s: %v", key, err)
			continue
		}

		allStats[key] = &stats
	}

	if err := iterator.Error(); err != nil {
		http.Error(w, fmt.Sprintf("Iterator error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(allStats)
}
