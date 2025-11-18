package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/templatedop/watermill/pkg/franzgo"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Phase 3 Examples: Stream Transformations, Exactly-Once Semantics, and Windowing

func runStreamTransformationsExample(client *franzgo.Client) {
	fmt.Println("\n=== Stream Transformations Example ===")

	ctx := context.Background()
	producer := franzgo.NewProducer(client)

	// Produce sample messages
	fmt.Println("Producing sample messages...")
	for i := 1; i <= 5; i++ {
		data := map[string]interface{}{
			"id":     i,
			"value":  i * 10,
			"status": "active",
		}
		payload, _ := json.Marshal(data)

		err := producer.ProduceWithHeaders(ctx, "transform-input", []byte(fmt.Sprintf("key-%d", i)), payload, map[string]string{
			"source": "example",
			"type":   "test",
		})
		if err != nil {
			log.Printf("Failed to produce: %v", err)
		}
	}

	fmt.Println("Messages produced successfully")

	// Example 1: Simple Map Transformation
	runMapTransformation(client)

	// Example 2: Filter Transformation
	runFilterTransformation(client)

	// Example 3: FlatMap Transformation
	runFlatMapTransformation(client)

	// Example 4: Chained Transformations
	runChainedTransformation(client)
}

func runMapTransformation(client *franzgo.Client) {
	fmt.Println("\n--- Map Transformation ---")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transformer := franzgo.NewStreamTransformer(client, "transform-input", "transform-map-output")

	// Transform: multiply value by 2
	go func() {
		err := transformer.Map(ctx, func(record *kgo.Record) (*kgo.Record, error) {
			var data map[string]interface{}
			if err := json.Unmarshal(record.Value, &data); err != nil {
				return nil, err
			}

			// Transform the value
			if val, ok := data["value"].(float64); ok {
				data["value"] = val * 2
				data["transformed"] = true
			}

			newValue, _ := json.Marshal(data)
			record.Value = newValue

			fmt.Printf("Transformed: %s\n", string(newValue))
			return record, nil
		})
		if err != nil && err != context.DeadlineExceeded {
			log.Printf("Map transformation error: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)
	fmt.Println("Map transformation completed")
}

func runFilterTransformation(client *franzgo.Client) {
	fmt.Println("\n--- Filter Transformation ---")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transformer := franzgo.NewStreamTransformer(client, "transform-input", "transform-filter-output")

	// Filter: only pass records with value > 20
	go func() {
		err := transformer.Filter(ctx, func(record *kgo.Record) bool {
			var data map[string]interface{}
			if err := json.Unmarshal(record.Value, &data); err != nil {
				return false
			}

			if val, ok := data["value"].(float64); ok {
				passes := val > 20
				fmt.Printf("Record value=%.0f, passes filter: %v\n", val, passes)
				return passes
			}

			return false
		})
		if err != nil && err != context.DeadlineExceeded {
			log.Printf("Filter transformation error: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)
	fmt.Println("Filter transformation completed")
}

func runFlatMapTransformation(client *franzgo.Client) {
	fmt.Println("\n--- FlatMap Transformation ---")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// First produce a message with an array
	producer := franzgo.NewProducer(client)
	arrayData := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{"id": 1, "name": "item1"},
			map[string]interface{}{"id": 2, "name": "item2"},
			map[string]interface{}{"id": 3, "name": "item3"},
		},
	}
	payload, _ := json.Marshal(arrayData)
	producer.Produce(ctx, "flatmap-input", []byte("array-key"), payload)

	transformer := franzgo.NewStreamTransformer(client, "flatmap-input", "flatmap-output")

	// FlatMap: expand array into multiple records
	go func() {
		common := &franzgo.CommonTransformations{}
		err := transformer.FlatMap(ctx, common.ExpandArray("items"))
		if err != nil && err != context.DeadlineExceeded {
			log.Printf("FlatMap transformation error: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)
	fmt.Println("FlatMap transformation completed")
}

func runChainedTransformation(client *franzgo.Client) {
	fmt.Println("\n--- Chained Transformation ---")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transformer := franzgo.NewChainableTransformer(client, "transform-input", "transform-chained-output")

	// Chain: Filter -> Map -> Transform
	transformer.
		Filter(func(record *kgo.Record) bool {
			var data map[string]interface{}
			json.Unmarshal(record.Value, &data)
			if val, ok := data["value"].(float64); ok {
				return val > 15
			}
			return false
		}).
		Map(func(record *kgo.Record) (*kgo.Record, error) {
			var data map[string]interface{}
			json.Unmarshal(record.Value, &data)
			data["doubled"] = true
			if val, ok := data["value"].(float64); ok {
				data["value"] = val * 2
			}
			newValue, _ := json.Marshal(data)
			record.Value = newValue
			return record, nil
		})

	go func() {
		err := transformer.Run(ctx)
		if err != nil && err != context.DeadlineExceeded {
			log.Printf("Chained transformation error: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)
	fmt.Println("Chained transformation completed")
}

func runExactlyOnceExample(client *franzgo.Client) {
	fmt.Println("\n=== Exactly-Once Semantics Example ===")

	ctx := context.Background()

	// Example 1: Transactional Producer
	runTransactionalProducerExample(client)

	// Example 2: Exactly-Once Processor
	runExactlyOnceProcessorExample(ctx, client)

	// Example 3: Idempotent Producer
	runIdempotentProducerExample(ctx, client)
}

func runTransactionalProducerExample(client *franzgo.Client) {
	fmt.Println("\n--- Transactional Producer ---")

	// Create config with transaction support
	config := franzgo.DefaultConfig()
	config.Brokers = []string{"localhost:9092"}

	txnConfig := franzgo.DefaultTransactionalConfig("txn-producer-1")
	txnProducer, err := franzgo.NewTransactionalProducer(config, txnConfig)
	if err != nil {
		log.Printf("Failed to create transactional producer: %v", err)
		return
	}
	defer txnProducer.Close()

	ctx := context.Background()

	// Execute multiple operations in a single transaction
	err = txnProducer.ExecuteTransaction(ctx, func(txnCtx context.Context) error {
		// Produce multiple messages atomically
		fmt.Println("Producing messages in transaction...")

		if err := txnProducer.Produce(txnCtx, "orders", []byte("order-1"), []byte(`{"id": 1, "amount": 100}`)); err != nil {
			return err
		}

		if err := txnProducer.Produce(txnCtx, "payments", []byte("payment-1"), []byte(`{"order_id": 1, "status": "paid"}`)); err != nil {
			return err
		}

		if err := txnProducer.Produce(txnCtx, "inventory", []byte("inv-1"), []byte(`{"product_id": 1, "reserved": true}`)); err != nil {
			return err
		}

		fmt.Println("All messages produced successfully in transaction")
		return nil // Commit on success
	})

	if err != nil {
		fmt.Printf("Transaction failed: %v\n", err)
	} else {
		fmt.Println("Transaction committed successfully!")
	}
}

func runExactlyOnceProcessorExample(ctx context.Context, client *franzgo.Client) {
	fmt.Println("\n--- Exactly-Once Processor ---")

	config := franzgo.DefaultConfig()
	config.Brokers = []string{"localhost:9092"}

	eoConfig := &franzgo.ExactlyOnceConfig{
		ConsumerGroup:       "exactly-once-group",
		TransactionalID:     "eo-processor-1",
		ProcessingTimeout:   30 * time.Second,
		DeduplicationWindow: 5 * time.Minute,
	}

	// Create processor with exactly-once guarantees
	processor, err := franzgo.NewExactlyOnceProcessor(config, eoConfig, func(ctx context.Context, record *kgo.Record) ([]*franzgo.ProduceMessage, error) {
		// Process the record
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			return nil, err
		}

		// Transform and produce to multiple topics
		messages := []*franzgo.ProduceMessage{
			{
				Topic: "processed-orders",
				Key:   record.Key,
				Value: record.Value,
				Headers: map[string]string{
					"processed_at": time.Now().Format(time.RFC3339),
					"processor":    "exactly-once-processor",
				},
			},
		}

		fmt.Printf("Processed record: %s\n", string(record.Key))
		return messages, nil
	})

	if err != nil {
		log.Printf("Failed to create exactly-once processor: %v", err)
		return
	}
	defer processor.Close()

	fmt.Println("Exactly-once processor created successfully")
	fmt.Println("This processor guarantees that each message is processed exactly once")
}

func runIdempotentProducerExample(ctx context.Context, client *franzgo.Client) {
	fmt.Println("\n--- Idempotent Producer ---")

	producer := franzgo.NewProducer(client)
	idempotentProducer := franzgo.NewIdempotentProducer(producer, 5*time.Minute)

	// Produce the same message multiple times - only sent once
	for i := 0; i < 3; i++ {
		err := idempotentProducer.Produce(ctx, "idempotent-test", []byte("same-key"), []byte(`{"msg": "duplicate test"}`))
		if err != nil {
			log.Printf("Failed to produce: %v", err)
		}
		fmt.Printf("Attempt %d: Message sent (deduplicated internally)\n", i+1)
	}

	fmt.Println("Idempotent producer ensures no duplicate messages")
}

func runWindowingExample(client *franzgo.Client) {
	fmt.Println("\n=== Windowing Operations Example ===")

	ctx := context.Background()

	// Produce sample data for windowing
	produceSampleDataForWindowing(client, ctx)

	// Example 1: Tumbling Window
	runTumblingWindowExample(client)

	// Example 2: Sliding Window
	runSlidingWindowExample(client)

	// Example 3: Session Window
	runSessionWindowExample(client)

	// Example 4: Window Aggregations
	runWindowAggregationsExample(client)
}

func produceSampleDataForWindowing(client *franzgo.Client, ctx context.Context) {
	fmt.Println("\n--- Producing Sample Data for Windowing ---")

	producer := franzgo.NewProducer(client)

	// Produce events over time
	for i := 1; i <= 10; i++ {
		data := map[string]interface{}{
			"user_id":   fmt.Sprintf("user-%d", (i%3)+1),
			"value":     float64(i * 10),
			"timestamp": time.Now().Unix(),
		}
		payload, _ := json.Marshal(data)

		err := producer.Produce(ctx, "window-input", []byte(fmt.Sprintf("event-%d", i)), payload)
		if err != nil {
			log.Printf("Failed to produce: %v", err)
		}

		time.Sleep(100 * time.Millisecond) // Spread events over time
	}

	fmt.Println("Sample data produced for windowing")
}

func runTumblingWindowExample(client *franzgo.Client) {
	fmt.Println("\n--- Tumbling Window (5 second windows) ---")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create tumbling window that aggregates every 5 seconds
	window := franzgo.NewTumblingWindow(client, 5*time.Second, func(wc *franzgo.WindowContext) error {
		aggregator := &franzgo.WindowAggregator{}

		count, _ := aggregator.Count(wc)
		sum, _ := aggregator.Sum(wc, "value")
		avg, _ := aggregator.Average(wc, "value")

		fmt.Printf("Tumbling Window [%s - %s] for key '%s':\n",
			wc.Window.Start.Format("15:04:05"),
			wc.Window.End.Format("15:04:05"),
			wc.Window.Key,
		)
		fmt.Printf("  Count: %d, Sum: %.0f, Average: %.2f\n", count, sum, avg)

		// Emit aggregated result
		result := map[string]interface{}{
			"key":       wc.Window.Key,
			"count":     count,
			"sum":       sum,
			"average":   avg,
			"window_start": wc.Window.Start.Unix(),
			"window_end":   wc.Window.End.Unix(),
		}

		return wc.Emit("window-tumbling-output", []byte(wc.Window.Key), result)
	})

	window.SetOutputTopic("window-tumbling-output")

	// Process records in tumbling windows
	consumer := franzgo.NewConsumer(client)
	go func() {
		consumer.Consume(ctx, []string{"window-input"}, func(ctx context.Context, record *kgo.Record) error {
			var data map[string]interface{}
			json.Unmarshal(record.Value, &data)
			key := data["user_id"].(string)

			return window.Process(ctx, key, record)
		})
	}()

	time.Sleep(12 * time.Second)
	fmt.Println("Tumbling window processing completed")
}

func runSlidingWindowExample(client *franzgo.Client) {
	fmt.Println("\n--- Sliding Window (10 second window, 3 second slide) ---")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create sliding window
	window := franzgo.NewSlidingWindow(client, 10*time.Second, 3*time.Second, func(wc *franzgo.WindowContext) error {
		aggregator := &franzgo.WindowAggregator{}

		count, _ := aggregator.Count(wc)
		if count > 0 {
			sum, _ := aggregator.Sum(wc, "value")

			fmt.Printf("Sliding Window [%s - %s] for key '%s': Count=%d, Sum=%.0f\n",
				wc.Window.Start.Format("15:04:05"),
				wc.Window.End.Format("15:04:05"),
				wc.Window.Key,
				count,
				sum,
			)
		}

		return nil
	})

	window.SetOutputTopic("window-sliding-output")

	// Process records in sliding windows
	consumer := franzgo.NewConsumer(client)
	go func() {
		consumer.Consume(ctx, []string{"window-input"}, func(ctx context.Context, record *kgo.Record) error {
			var data map[string]interface{}
			json.Unmarshal(record.Value, &data)
			key := data["user_id"].(string)

			return window.Process(ctx, key, record)
		})
	}()

	time.Sleep(12 * time.Second)
	window.Stop()
	fmt.Println("Sliding window processing completed")
}

func runSessionWindowExample(client *franzgo.Client) {
	fmt.Println("\n--- Session Window (2 second inactivity gap) ---")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create session window
	window := franzgo.NewSessionWindow(client, 2*time.Second, func(wc *franzgo.WindowContext) error {
		aggregator := &franzgo.WindowAggregator{}

		count, _ := aggregator.Count(wc)
		duration := wc.Window.End.Sub(wc.Window.Start)

		fmt.Printf("Session Window for key '%s':\n", wc.Window.Key)
		fmt.Printf("  Duration: %v, Events: %d\n", duration, count)
		fmt.Printf("  Start: %s, End: %s\n",
			wc.Window.Start.Format("15:04:05"),
			wc.Window.End.Format("15:04:05"),
		)

		return nil
	})

	window.SetOutputTopic("window-session-output")

	// Process records in session windows
	consumer := franzgo.NewConsumer(client)
	go func() {
		consumer.Consume(ctx, []string{"window-input"}, func(ctx context.Context, record *kgo.Record) error {
			var data map[string]interface{}
			json.Unmarshal(record.Value, &data)
			key := data["user_id"].(string)

			return window.Process(ctx, key, record)
		})
	}()

	time.Sleep(12 * time.Second)
	fmt.Println("Session window processing completed")
}

func runWindowAggregationsExample(client *franzgo.Client) {
	fmt.Println("\n--- Window Aggregations ---")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a window with comprehensive aggregations
	aggregation, err := franzgo.NewWindowedAggregation(client, franzgo.WindowTypeTumbling, 5*time.Second, func(wc *franzgo.WindowContext) error {
		aggregator := &franzgo.WindowAggregator{}

		count, _ := aggregator.Count(wc)
		sum, _ := aggregator.Sum(wc, "value")
		avg, _ := aggregator.Average(wc, "value")
		min, _ := aggregator.Min(wc, "value")
		max, _ := aggregator.Max(wc, "value")

		fmt.Printf("\nWindow Aggregations for key '%s':\n", wc.Window.Key)
		fmt.Printf("  Count: %d\n", count)
		fmt.Printf("  Sum: %.0f\n", sum)
		fmt.Printf("  Average: %.2f\n", avg)
		fmt.Printf("  Min: %.0f\n", min)
		fmt.Printf("  Max: %.0f\n", max)

		// Emit comprehensive aggregation result
		result := map[string]interface{}{
			"key":     wc.Window.Key,
			"count":   count,
			"sum":     sum,
			"average": avg,
			"min":     min,
			"max":     max,
		}

		return wc.Emit("window-aggregations-output", []byte(wc.Window.Key), result)
	})

	if err != nil {
		log.Printf("Failed to create windowed aggregation: %v", err)
		return
	}

	aggregation.SetOutputTopic("window-aggregations-output")

	// Run the aggregation
	go func() {
		err := aggregation.Run(ctx, "window-input", func(record *kgo.Record) string {
			var data map[string]interface{}
			json.Unmarshal(record.Value, &data)
			return data["user_id"].(string)
		})
		if err != nil && err != context.DeadlineExceeded {
			log.Printf("Aggregation error: %v", err)
		}
	}()

	time.Sleep(8 * time.Second)
	aggregation.Stop()
	fmt.Println("Window aggregations completed")
}

func runPhase3Examples() {
	fmt.Println("==============================================")
	fmt.Println("Franz-go Phase 3: Advanced Features Examples")
	fmt.Println("==============================================")

	// Create client
	config, err := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("franzgo-phase3-examples").
		Build()

	if err != nil {
		log.Fatalf("Failed to build config: %v", err)
	}

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Run examples
	runStreamTransformationsExample(client)
	runExactlyOnceExample(client)
	runWindowingExample(client)

	fmt.Println("\n==============================================")
	fmt.Println("All Phase 3 examples completed!")
	fmt.Println("==============================================")
}
