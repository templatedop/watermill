// Franz-go View Example
// Demonstrates querying stateful data via HTTP API using franz-go

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gitlab.cept.gov.in/it-2.0-common/watermill/pkg/franzgo"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CustomerStats represents customer statistics from the state store
type CustomerStats struct {
	CustomerID   string    `json:"customer_id"`
	TotalOrders  int       `json:"total_orders"`
	TotalSpent   float64   `json:"total_spent"`
	LastOrderAt  time.Time `json:"last_order_at"`
	AverageOrder float64   `json:"average_order"`
}

var stateStore franzgo.Storage

func main() {
	log.Println("🚀 Starting Franz-go View Example...")

	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	// Create configuration
	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("stats-view").
		WithClientID("view-consumer").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	// Initialize state store
	stateStore, err = franzgo.NewMemoryStorage()
	if err != nil {
		log.Fatalf("❌ Failed to create storage: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consuming the changelog topic to populate the view
	go populateView(ctx, client)

	// Give view time to populate
	time.Sleep(2 * time.Second)

	// Start HTTP server
	http.HandleFunc("/stats/", handleGetStats)
	http.HandleFunc("/stats", handleListStats)

	server := &http.Server{Addr: ":8080"}

	go func() {
		log.Println("🌐 HTTP server listening on :8080")
		log.Println("📊 Try: curl http://localhost:8080/stats/CUST-1")
		log.Println("📊 Try: curl http://localhost:8080/stats")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("❌ Server error: %v", err)
		}
	}()

	log.Println("✅ View service running. Press Ctrl+C to stop")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("🛑 Shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)
	cancel()
	log.Println("✅ Shutdown complete")
}

// populateView consumes the changelog topic and populates the local state store
func populateView(ctx context.Context, client *franzgo.Client) {
	consumer := franzgo.NewConsumer(client)

	log.Println("📥 Populating view from changelog...")

	err := consumer.Consume(ctx, []string{"customer-stats-changelog"}, func(record *kgo.Record) error {
		// The changelog contains customer stats keyed by customer ID
		customerID := string(record.Key)

		if len(record.Value) == 0 {
			// Tombstone record (deletion)
			stateStore.Delete(customerID)
			log.Printf("🗑️  Deleted stats for customer %s", customerID)
			return nil
		}

		// Store the stats
		if err := stateStore.Put(customerID, record.Value); err != nil {
			return fmt.Errorf("failed to store stats: %w", err)
		}

		var stats CustomerStats
		if err := json.Unmarshal(record.Value, &stats); err == nil {
			log.Printf("📊 Updated view: %s - %d orders, $%.2f total",
				customerID, stats.TotalOrders, stats.TotalSpent)
		}

		return nil
	})

	if err != nil {
		log.Printf("❌ Consumer error: %v", err)
	}
}

// handleGetStats returns stats for a specific customer
func handleGetStats(w http.ResponseWriter, r *http.Request) {
	// Extract customer ID from path: /stats/CUST-1
	path := r.URL.Path
	customerID := strings.TrimPrefix(path, "/stats/")

	if customerID == "" {
		http.Error(w, "Customer ID required", http.StatusBadRequest)
		return
	}

	// Get stats from state store
	data, err := stateStore.Get(customerID)
	if err != nil {
		http.Error(w, "Customer not found", http.StatusNotFound)
		return
	}

	var stats CustomerStats
	if err := json.Unmarshal(data, &stats); err != nil {
		http.Error(w, "Failed to parse stats", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)

	log.Printf("📊 API: Retrieved stats for customer %s", customerID)
}

// handleListStats returns all customer stats
func handleListStats(w http.ResponseWriter, r *http.Request) {
	allStats := make([]CustomerStats, 0)

	// Iterate through all keys in the state store
	iter := stateStore.Iterator()
	defer iter.Close()

	for iter.Next() {
		key, value := iter.KeyValue()

		var stats CustomerStats
		if err := json.Unmarshal(value, &stats); err != nil {
			log.Printf("⚠️  Failed to unmarshal stats for %s: %v", string(key), err)
			continue
		}

		allStats = append(allStats, stats)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total":     len(allStats),
		"customers": allStats,
	})

	log.Printf("📊 API: Retrieved stats for %d customers", len(allStats))
}
