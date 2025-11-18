package schema

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// SchemaType defines the type of schema
type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
	SchemaTypeJSON     SchemaType = "JSON"
)

// SchemaRegistry interface for schema registry operations
type SchemaRegistry interface {
	// GetSchemaByID retrieves a schema by its ID
	GetSchemaByID(id int) (*Schema, error)
	// GetLatestSchema retrieves the latest schema for a subject
	GetLatestSchema(subject string) (*Schema, error)
	// RegisterSchema registers a new schema
	RegisterSchema(subject string, schema string, schemaType SchemaType) (int, error)
	// CheckCompatibility checks if a schema is compatible
	CheckCompatibility(subject string, schema string) (bool, error)
}

// Schema represents a schema
type Schema struct {
	ID      int
	Subject string
	Version int
	Schema  string
	Type    SchemaType
}

// ConfluentSchemaRegistry implements Confluent Schema Registry client
type ConfluentSchemaRegistry struct {
	baseURL    string
	httpClient *http.Client
	cache      map[int]*Schema
	mu         sync.RWMutex
}

// NewConfluentSchemaRegistry creates a new Confluent Schema Registry client
func NewConfluentSchemaRegistry(baseURL string) *ConfluentSchemaRegistry {
	return &ConfluentSchemaRegistry{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		cache: make(map[int]*Schema),
	}
}

// GetSchemaByID retrieves a schema by its ID
func (r *ConfluentSchemaRegistry) GetSchemaByID(id int) (*Schema, error) {
	// Check cache first
	r.mu.RLock()
	if schema, exists := r.cache[id]; exists {
		r.mu.RUnlock()
		return schema, nil
	}
	r.mu.RUnlock()

	// Fetch from registry
	url := fmt.Sprintf("%s/schemas/ids/%d", r.baseURL, id)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry error: %s", string(body))
	}

	var result struct {
		Schema string `json:"schema"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	schema := &Schema{
		ID:     id,
		Schema: result.Schema,
	}

	// Cache the schema
	r.mu.Lock()
	r.cache[id] = schema
	r.mu.Unlock()

	return schema, nil
}

// GetLatestSchema retrieves the latest schema for a subject
func (r *ConfluentSchemaRegistry) GetLatestSchema(subject string) (*Schema, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", r.baseURL, subject)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch latest schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry error: %s", string(body))
	}

	var result struct {
		ID      int    `json:"id"`
		Version int    `json:"version"`
		Schema  string `json:"schema"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	schema := &Schema{
		ID:      result.ID,
		Subject: subject,
		Version: result.Version,
		Schema:  result.Schema,
	}

	// Cache the schema
	r.mu.Lock()
	r.cache[result.ID] = schema
	r.mu.Unlock()

	return schema, nil
}

// RegisterSchema registers a new schema
func (r *ConfluentSchemaRegistry) RegisterSchema(subject string, schema string, schemaType SchemaType) (int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", r.baseURL, subject)

	payload := map[string]interface{}{
		"schema":     schema,
		"schemaType": string(schemaType),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal payload: %w", err)
	}

	resp, err := r.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, fmt.Errorf("failed to register schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registry error: %s", string(body))
	}

	var result struct {
		ID int `json:"id"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.ID, nil
}

// CheckCompatibility checks if a schema is compatible
func (r *ConfluentSchemaRegistry) CheckCompatibility(subject string, schema string) (bool, error) {
	url := fmt.Sprintf("%s/compatibility/subjects/%s/versions/latest", r.baseURL, subject)

	payload := map[string]string{
		"schema": schema,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal payload: %w", err)
	}

	resp, err := r.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return false, fmt.Errorf("failed to check compatibility: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("schema registry error: %s", string(body))
	}

	var result struct {
		IsCompatible bool `json:"is_compatible"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.IsCompatible, nil
}

// Codec interface for encoding/decoding messages with schemas
type Codec interface {
	// Encode encodes a message with schema
	Encode(schemaID int, data interface{}) ([]byte, error)
	// Decode decodes a message with schema
	Decode(data []byte) (interface{}, int, error)
}

// BaseCodec provides common functionality for all codecs
type BaseCodec struct {
	registry SchemaRegistry
}

// NewBaseCodec creates a new base codec
func NewBaseCodec(registry SchemaRegistry) *BaseCodec {
	return &BaseCodec{
		registry: registry,
	}
}

// EncodeWithMagicByte encodes data with Confluent magic byte format
// Format: [magic_byte(1)][schema_id(4)][data]
func (c *BaseCodec) EncodeWithMagicByte(schemaID int, data []byte) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write magic byte (0x00)
	if err := buf.WriteByte(0x00); err != nil {
		return nil, err
	}

	// Write schema ID (big-endian)
	if err := binary.Write(buf, binary.BigEndian, int32(schemaID)); err != nil {
		return nil, err
	}

	// Write data
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecodeWithMagicByte decodes data with Confluent magic byte format
func (c *BaseCodec) DecodeWithMagicByte(data []byte) ([]byte, int, error) {
	if len(data) < 5 {
		return nil, 0, fmt.Errorf("data too short")
	}

	// Check magic byte
	if data[0] != 0x00 {
		return nil, 0, fmt.Errorf("invalid magic byte: expected 0x00, got 0x%02x", data[0])
	}

	// Read schema ID
	schemaID := int(binary.BigEndian.Uint32(data[1:5]))

	// Return payload (after magic byte and schema ID)
	return data[5:], schemaID, nil
}

// CachedSchemaRegistry wraps a schema registry with caching
type CachedSchemaRegistry struct {
	inner       SchemaRegistry
	schemaCache map[int]*Schema
	subjectCache map[string]*Schema
	mu          sync.RWMutex
	ttl         time.Duration
}

// NewCachedSchemaRegistry creates a new cached schema registry
func NewCachedSchemaRegistry(inner SchemaRegistry, ttl time.Duration) *CachedSchemaRegistry {
	return &CachedSchemaRegistry{
		inner:        inner,
		schemaCache:  make(map[int]*Schema),
		subjectCache: make(map[string]*Schema),
		ttl:          ttl,
	}
}

// GetSchemaByID retrieves a schema by ID with caching
func (r *CachedSchemaRegistry) GetSchemaByID(id int) (*Schema, error) {
	r.mu.RLock()
	if schema, exists := r.schemaCache[id]; exists {
		r.mu.RUnlock()
		return schema, nil
	}
	r.mu.RUnlock()

	schema, err := r.inner.GetSchemaByID(id)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.schemaCache[id] = schema
	r.mu.Unlock()

	return schema, nil
}

// GetLatestSchema retrieves the latest schema with caching
func (r *CachedSchemaRegistry) GetLatestSchema(subject string) (*Schema, error) {
	r.mu.RLock()
	if schema, exists := r.subjectCache[subject]; exists {
		r.mu.RUnlock()
		return schema, nil
	}
	r.mu.RUnlock()

	schema, err := r.inner.GetLatestSchema(subject)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.subjectCache[subject] = schema
	r.schemaCache[schema.ID] = schema
	r.mu.Unlock()

	return schema, nil
}

// RegisterSchema forwards to inner registry
func (r *CachedSchemaRegistry) RegisterSchema(subject string, schema string, schemaType SchemaType) (int, error) {
	return r.inner.RegisterSchema(subject, schema, schemaType)
}

// CheckCompatibility forwards to inner registry
func (r *CachedSchemaRegistry) CheckCompatibility(subject string, schema string) (bool, error) {
	return r.inner.CheckCompatibility(subject, schema)
}

// InvalidateCache invalidates the cache
func (r *CachedSchemaRegistry) InvalidateCache() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.schemaCache = make(map[int]*Schema)
	r.subjectCache = make(map[string]*Schema)
}
