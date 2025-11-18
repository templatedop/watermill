package franzgo

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Client is the main Kafka client
type Client struct {
	config *Config
	client *kgo.Client
	mu     sync.RWMutex
	closed bool
}

// NewClient creates a new Kafka client
func NewClient(config *Config) (*Client, error) {
	if config == nil {
		config = DefaultConfig()
	}

	opts, err := config.ToKgoOpts()
	if err != nil {
		return nil, fmt.Errorf("failed to create client options: %w", err)
	}

	kgoClient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create franz-go client: %w", err)
	}

	return &Client{
		config: config,
		client: kgoClient,
		closed: false,
	}, nil
}

// GetKgoClient returns the underlying franz-go client
func (c *Client) GetKgoClient() *kgo.Client {
	return c.client
}

// Config returns the client configuration
func (c *Client) Config() *Config {
	return c.config
}

// Ping checks connectivity to Kafka brokers
func (c *Client) Ping(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	return c.client.Ping(ctx)
}

// Close closes the client and releases resources
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.client.Close()
	return nil
}

// IsClosed returns true if the client is closed
func (c *Client) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}
