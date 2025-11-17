package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

// Producer provides high-level methods for publishing messages
type Producer struct {
	client *Client
}

// NewProducer creates a new Producer
func NewProducer(client *Client) *Producer {
	return &Producer{
		client: client,
	}
}

// PublishEvent publishes an event with automatic UUID and timestamp
func (p *Producer) PublishEvent(ctx context.Context, topic string, payload interface{}) error {
	msg, err := p.createMessage(payload)
	if err != nil {
		return err
	}

	msg.Metadata.Set("event_type", "event")
	msg.Metadata.Set("topic", topic)

	return p.client.Publish(ctx, topic, msg)
}

// PublishCommand publishes a command with automatic UUID and timestamp
func (p *Producer) PublishCommand(ctx context.Context, topic string, payload interface{}) error {
	msg, err := p.createMessage(payload)
	if err != nil {
		return err
	}

	msg.Metadata.Set("event_type", "command")
	msg.Metadata.Set("topic", topic)

	return p.client.Publish(ctx, topic, msg)
}

// PublishBatch publishes multiple messages in a batch
func (p *Producer) PublishBatch(ctx context.Context, topic string, payloads []interface{}) error {
	messages := make([]*message.Message, 0, len(payloads))

	for _, payload := range payloads {
		msg, err := p.createMessage(payload)
		if err != nil {
			return err
		}
		msg.Metadata.Set("topic", topic)
		messages = append(messages, msg)
	}

	return p.client.Publish(ctx, topic, messages...)
}

// PublishWithKey publishes a message with a specific partition key
func (p *Producer) PublishWithKey(ctx context.Context, topic string, key string, payload interface{}) error {
	msg, err := p.createMessage(payload)
	if err != nil {
		return err
	}

	msg.Metadata.Set("topic", topic)
	msg.Metadata.Set("partition_key", key)

	return p.client.Publish(ctx, topic, msg)
}

// PublishDelayed publishes a message with a delay (requires additional infrastructure)
func (p *Producer) PublishDelayed(ctx context.Context, topic string, payload interface{}, delay time.Duration) error {
	msg, err := p.createMessage(payload)
	if err != nil {
		return err
	}

	msg.Metadata.Set("topic", topic)
	msg.Metadata.Set("delay", delay.String())
	msg.Metadata.Set("scheduled_at", time.Now().Add(delay).Format(time.RFC3339))

	return p.client.Publish(ctx, topic, msg)
}

// createMessage creates a new message with payload
func (p *Producer) createMessage(payload interface{}) (*message.Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(uuid.New().String(), data)
	msg.Metadata.Set("timestamp", time.Now().Format(time.RFC3339))
	msg.Metadata.Set("producer_id", p.client.config.ClientID)

	return msg, nil
}

// EcommerceProducer provides ecommerce-specific publishing methods
type EcommerceProducer struct {
	*Producer
}

// NewEcommerceProducer creates a new EcommerceProducer
func NewEcommerceProducer(client *Client) *EcommerceProducer {
	return &EcommerceProducer{
		Producer: NewProducer(client),
	}
}

// OrderEvent represents an order event
type OrderEvent struct {
	OrderID    string                 `json:"order_id"`
	CustomerID string                 `json:"customer_id"`
	Status     string                 `json:"status"`
	Items      []OrderItem            `json:"items"`
	Total      float64                `json:"total"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// OrderItem represents an item in an order
type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

// PublishOrderCreated publishes an order created event
func (p *EcommerceProducer) PublishOrderCreated(ctx context.Context, order OrderEvent) error {
	return p.PublishWithKey(ctx, "orders.created", order.OrderID, order)
}

// PublishOrderUpdated publishes an order updated event
func (p *EcommerceProducer) PublishOrderUpdated(ctx context.Context, order OrderEvent) error {
	return p.PublishWithKey(ctx, "orders.updated", order.OrderID, order)
}

// PublishOrderCancelled publishes an order cancelled event
func (p *EcommerceProducer) PublishOrderCancelled(ctx context.Context, orderID string, reason string) error {
	event := map[string]string{
		"order_id": orderID,
		"reason":   reason,
	}
	return p.PublishWithKey(ctx, "orders.cancelled", orderID, event)
}

// InventoryEvent represents an inventory event
type InventoryEvent struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Operation string `json:"operation"` // "reserve", "release", "adjust"
}

// PublishInventoryReserved publishes an inventory reserved event
func (p *EcommerceProducer) PublishInventoryReserved(ctx context.Context, productID string, quantity int) error {
	event := InventoryEvent{
		ProductID: productID,
		Quantity:  quantity,
		Operation: "reserve",
	}
	return p.PublishWithKey(ctx, "inventory.reserved", productID, event)
}

// PublishInventoryReleased publishes an inventory released event
func (p *EcommerceProducer) PublishInventoryReleased(ctx context.Context, productID string, quantity int) error {
	event := InventoryEvent{
		ProductID: productID,
		Quantity:  quantity,
		Operation: "release",
	}
	return p.PublishWithKey(ctx, "inventory.released", productID, event)
}

// PaymentEvent represents a payment event
type PaymentEvent struct {
	PaymentID  string  `json:"payment_id"`
	OrderID    string  `json:"order_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Method     string  `json:"method"`
	CustomerID string  `json:"customer_id"`
}

// PublishPaymentProcessed publishes a payment processed event
func (p *EcommerceProducer) PublishPaymentProcessed(ctx context.Context, payment PaymentEvent) error {
	return p.PublishWithKey(ctx, "payments.processed", payment.OrderID, payment)
}

// PublishPaymentFailed publishes a payment failed event
func (p *EcommerceProducer) PublishPaymentFailed(ctx context.Context, payment PaymentEvent, reason string) error {
	event := map[string]interface{}{
		"payment": payment,
		"reason":  reason,
	}
	return p.PublishWithKey(ctx, "payments.failed", payment.OrderID, event)
}

// ShipmentEvent represents a shipment event
type ShipmentEvent struct {
	ShipmentID   string `json:"shipment_id"`
	OrderID      string `json:"order_id"`
	TrackingCode string `json:"tracking_code"`
	Carrier      string `json:"carrier"`
	Status       string `json:"status"`
}

// PublishShipmentCreated publishes a shipment created event
func (p *EcommerceProducer) PublishShipmentCreated(ctx context.Context, shipment ShipmentEvent) error {
	return p.PublishWithKey(ctx, "shipments.created", shipment.OrderID, shipment)
}

// PublishShipmentDelivered publishes a shipment delivered event
func (p *EcommerceProducer) PublishShipmentDelivered(ctx context.Context, shipment ShipmentEvent) error {
	return p.PublishWithKey(ctx, "shipments.delivered", shipment.OrderID, shipment)
}
