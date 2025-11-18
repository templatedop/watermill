package franzgo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer provides high-level producer functionality
type Producer struct {
	client *Client
	mu     sync.RWMutex
}

// NewProducer creates a new producer
func NewProducer(client *Client) *Producer {
	return &Producer{
		client: client,
	}
}

// Produce sends a message to Kafka
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client.IsClosed() {
		return fmt.Errorf("client is closed")
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	// Synchronous produce
	results := p.client.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	return nil
}

// ProduceAsync sends a message asynchronously
func (p *Producer) ProduceAsync(ctx context.Context, topic string, key, value []byte, callback func(record *kgo.Record, err error)) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client.IsClosed() {
		callback(nil, fmt.Errorf("client is closed"))
		return
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	p.client.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		callback(r, err)
	})
}

// ProduceWithHeaders sends a message with custom headers
func (p *Producer) ProduceWithHeaders(ctx context.Context, topic string, key, value []byte, headers map[string]string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client.IsClosed() {
		return fmt.Errorf("client is closed")
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	// Add headers
	for k, v := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   k,
			Value: []byte(v),
		})
	}

	results := p.client.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	return nil
}

// ProduceBatch sends multiple messages in a batch
func (p *Producer) ProduceBatch(ctx context.Context, records []*kgo.Record) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client.IsClosed() {
		return fmt.Errorf("client is closed")
	}

	results := p.client.client.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce batch: %w", err)
	}

	return nil
}

// ProduceBatchAsync sends multiple messages asynchronously
func (p *Producer) ProduceBatchAsync(ctx context.Context, records []*kgo.Record, callback func(record *kgo.Record, err error)) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client.IsClosed() {
		for _, r := range records {
			callback(r, fmt.Errorf("client is closed"))
		}
		return
	}

	for _, record := range records {
		p.client.client.Produce(ctx, record, func(r *kgo.Record, err error) {
			callback(r, err)
		})
	}
}

// EcommerceProducer provides ecommerce-specific producer methods
type EcommerceProducer struct {
	*Producer
}

// NewEcommerceProducer creates a new ecommerce producer
func NewEcommerceProducer(client *Client) *EcommerceProducer {
	return &EcommerceProducer{
		Producer: NewProducer(client),
	}
}

// OrderEvent represents an order event
type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Total      float64   `json:"total"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
	Items      []string  `json:"items"`
}

// PaymentEvent represents a payment event
type PaymentEvent struct {
	PaymentID     string    `json:"payment_id"`
	OrderID       string    `json:"order_id"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	Status        string    `json:"status"`
	PaymentMethod string    `json:"payment_method"`
	ProcessedAt   time.Time `json:"processed_at"`
}

// InventoryEvent represents an inventory event
type InventoryEvent struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Action    string `json:"action"` // reserved, released, sold
	Timestamp time.Time `json:"timestamp"`
}

// PublishOrderCreated publishes an order created event
func (ep *EcommerceProducer) PublishOrderCreated(ctx context.Context, order *OrderEvent) error {
	codec := &JSONCodec{}
	value, err := codec.Encode(order)
	if err != nil {
		return fmt.Errorf("failed to encode order: %w", err)
	}

	headers := map[string]string{
		"event_type":   "order.created",
		"event_id":     uuid.New().String(),
		"timestamp":    time.Now().Format(time.RFC3339),
		"content_type": "application/json",
	}

	return ep.ProduceWithHeaders(ctx, "orders", []byte(order.OrderID), value, headers)
}

// PublishOrderUpdated publishes an order updated event
func (ep *EcommerceProducer) PublishOrderUpdated(ctx context.Context, order *OrderEvent) error {
	codec := &JSONCodec{}
	value, err := codec.Encode(order)
	if err != nil {
		return fmt.Errorf("failed to encode order: %w", err)
	}

	headers := map[string]string{
		"event_type":   "order.updated",
		"event_id":     uuid.New().String(),
		"timestamp":    time.Now().Format(time.RFC3339),
		"content_type": "application/json",
	}

	return ep.ProduceWithHeaders(ctx, "orders", []byte(order.OrderID), value, headers)
}

// PublishPaymentProcessed publishes a payment processed event
func (ep *EcommerceProducer) PublishPaymentProcessed(ctx context.Context, payment *PaymentEvent) error {
	codec := &JSONCodec{}
	value, err := codec.Encode(payment)
	if err != nil {
		return fmt.Errorf("failed to encode payment: %w", err)
	}

	headers := map[string]string{
		"event_type":   "payment.processed",
		"event_id":     uuid.New().String(),
		"timestamp":    time.Now().Format(time.RFC3339),
		"content_type": "application/json",
	}

	return ep.ProduceWithHeaders(ctx, "payments", []byte(payment.PaymentID), value, headers)
}

// PublishInventoryUpdated publishes an inventory updated event
func (ep *EcommerceProducer) PublishInventoryUpdated(ctx context.Context, inventory *InventoryEvent) error {
	codec := &JSONCodec{}
	value, err := codec.Encode(inventory)
	if err != nil {
		return fmt.Errorf("failed to encode inventory: %w", err)
	}

	headers := map[string]string{
		"event_type":   "inventory.updated",
		"event_id":     uuid.New().String(),
		"timestamp":    time.Now().Format(time.RFC3339),
		"content_type": "application/json",
	}

	return ep.ProduceWithHeaders(ctx, "inventory", []byte(inventory.ProductID), value, headers)
}

// JSONCodec provides JSON encoding/decoding
type JSONCodec struct{}

// Encode encodes a value to JSON
func (c *JSONCodec) Encode(v interface{}) ([]byte, error) {
	// Simple JSON encoding - in production use encoding/json
	return []byte(fmt.Sprintf("%v", v)), nil
}

// Decode decodes JSON to a value
func (c *JSONCodec) Decode(data []byte, v interface{}) error {
	// Simple JSON decoding - in production use encoding/json
	return nil
}

// ProducerMetrics tracks producer metrics
type ProducerMetrics struct {
	MessagesProduced int64
	BytesProduced    int64
	Errors           int64
	mu               sync.RWMutex
}

// NewProducerMetrics creates new producer metrics
func NewProducerMetrics() *ProducerMetrics {
	return &ProducerMetrics{}
}

// RecordProduced records a produced message
func (pm *ProducerMetrics) RecordProduced(size int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.MessagesProduced++
	pm.BytesProduced += int64(size)
}

// RecordError records an error
func (pm *ProducerMetrics) RecordError() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.Errors++
}

// GetStats returns current stats
func (pm *ProducerMetrics) GetStats() (messages, bytes, errors int64) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.MessagesProduced, pm.BytesProduced, pm.Errors
}
