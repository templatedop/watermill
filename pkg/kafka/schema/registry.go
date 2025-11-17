package schema

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// SchemaType defines the type of schema
type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
	SchemaTypeJSON     SchemaType = "JSON"
)

// RegistryClient is a client for Confluent Schema Registry
type RegistryClient struct {
	baseURL    string
	httpClient *http.Client
	cache      *schemaCache
	mu         sync.RWMutex
}

// SchemaRegistryConfig holds configuration for schema registry
type SchemaRegistryConfig struct {
	URL      string
	Username string
	Password string

	// Cache settings
	CacheSize int
}

// DefaultSchemaRegistryConfig returns default schema registry configuration
func DefaultSchemaRegistryConfig(url string) *SchemaRegistryConfig {
	return &SchemaRegistryConfig{
		URL:       url,
		CacheSize: 1000,
	}
}

// NewRegistryClient creates a new schema registry client
func NewRegistryClient(config *SchemaRegistryConfig) *RegistryClient {
	if config == nil {
		config = DefaultSchemaRegistryConfig("http://localhost:8081")
	}

	return &RegistryClient{
		baseURL:    config.URL,
		httpClient: &http.Client{},
		cache:      newSchemaCache(config.CacheSize),
	}
}

// Schema represents a schema in the registry
type Schema struct {
	ID      int        `json:"id"`
	Version int        `json:"version"`
	Schema  string     `json:"schema"`
	Type    SchemaType `json:"schemaType"`
	Subject string     `json:"subject"`
}

// RegisterSchema registers a new schema for a subject
func (rc *RegistryClient) RegisterSchema(subject string, schema string, schemaType SchemaType) (*Schema, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	url := fmt.Sprintf("%s/subjects/%s/versions", rc.baseURL, subject)

	payload := map[string]interface{}{
		"schema":     schema,
		"schemaType": schemaType,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := rc.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to register schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registration failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		ID int `json:"id"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	s := &Schema{
		ID:      result.ID,
		Schema:  schema,
		Type:    schemaType,
		Subject: subject,
	}

	rc.cache.put(result.ID, s)

	return s, nil
}

// GetSchemaByID retrieves a schema by ID
func (rc *RegistryClient) GetSchemaByID(id int) (*Schema, error) {
	// Check cache first
	if schema := rc.cache.get(id); schema != nil {
		return schema, nil
	}

	rc.mu.RLock()
	defer rc.mu.RUnlock()

	url := fmt.Sprintf("%s/schemas/ids/%d", rc.baseURL, id)

	resp, err := rc.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get schema with status %d", resp.StatusCode)
	}

	var result Schema
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	result.ID = id
	rc.cache.put(id, &result)

	return &result, nil
}

// GetLatestSchema retrieves the latest schema for a subject
func (rc *RegistryClient) GetLatestSchema(subject string) (*Schema, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	url := fmt.Sprintf("%s/subjects/%s/versions/latest", rc.baseURL, subject)

	resp, err := rc.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get latest schema with status %d", resp.StatusCode)
	}

	var result Schema
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	result.Subject = subject
	rc.cache.put(result.ID, &result)

	return &result, nil
}

// GetSchemaByVersion retrieves a specific version of a schema
func (rc *RegistryClient) GetSchemaByVersion(subject string, version int) (*Schema, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	url := fmt.Sprintf("%s/subjects/%s/versions/%d", rc.baseURL, subject, version)

	resp, err := rc.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get schema with status %d", resp.StatusCode)
	}

	var result Schema
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	result.Subject = subject
	rc.cache.put(result.ID, &result)

	return &result, nil
}

// ListSubjects lists all subjects in the registry
func (rc *RegistryClient) ListSubjects() ([]string, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	url := fmt.Sprintf("%s/subjects", rc.baseURL)

	resp, err := rc.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to list subjects: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to list subjects with status %d", resp.StatusCode)
	}

	var subjects []string
	if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return subjects, nil
}

// DeleteSubject deletes all versions of a subject
func (rc *RegistryClient) DeleteSubject(subject string) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	url := fmt.Sprintf("%s/subjects/%s", rc.baseURL, subject)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := rc.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete subject: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete subject with status %d", resp.StatusCode)
	}

	return nil
}

// schemaCache caches schemas by ID
type schemaCache struct {
	cache map[int]*Schema
	size  int
	mu    sync.RWMutex
}

func newSchemaCache(size int) *schemaCache {
	return &schemaCache{
		cache: make(map[int]*Schema),
		size:  size,
	}
}

func (sc *schemaCache) get(id int) *Schema {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return sc.cache[id]
}

func (sc *schemaCache) put(id int, schema *Schema) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Simple cache eviction: if full, clear oldest half
	if len(sc.cache) >= sc.size {
		count := 0
		for k := range sc.cache {
			delete(sc.cache, k)
			count++
			if count >= sc.size/2 {
				break
			}
		}
	}

	sc.cache[id] = schema
}

// Encoder encodes data with schema registry integration
type Encoder struct {
	registry *RegistryClient
}

// NewEncoder creates a new schema registry encoder
func NewEncoder(registry *RegistryClient) *Encoder {
	return &Encoder{
		registry: registry,
	}
}

// Encode encodes data with schema ID prefix
// Format: [magic_byte(1)][schema_id(4)][data]
func (e *Encoder) Encode(schemaID int, data []byte) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Magic byte (always 0)
	if err := buf.WriteByte(0); err != nil {
		return nil, fmt.Errorf("failed to write magic byte: %w", err)
	}

	// Schema ID (4 bytes, big-endian)
	if err := binary.Write(buf, binary.BigEndian, int32(schemaID)); err != nil {
		return nil, fmt.Errorf("failed to write schema ID: %w", err)
	}

	// Data
	if _, err := buf.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	return buf.Bytes(), nil
}

// Decoder decodes data with schema registry integration
type Decoder struct {
	registry *RegistryClient
}

// NewDecoder creates a new schema registry decoder
func NewDecoder(registry *RegistryClient) *Decoder {
	return &Decoder{
		registry: registry,
	}
}

// Decode decodes data and retrieves schema
// Format: [magic_byte(1)][schema_id(4)][data]
func (d *Decoder) Decode(encoded []byte) (schemaID int, data []byte, err error) {
	if len(encoded) < 5 {
		return 0, nil, fmt.Errorf("encoded data too short")
	}

	reader := bytes.NewReader(encoded)

	// Magic byte
	magicByte, err := reader.ReadByte()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read magic byte: %w", err)
	}
	if magicByte != 0 {
		return 0, nil, fmt.Errorf("invalid magic byte: %d", magicByte)
	}

	// Schema ID
	var id int32
	if err := binary.Read(reader, binary.BigEndian, &id); err != nil {
		return 0, nil, fmt.Errorf("failed to read schema ID: %w", err)
	}

	// Data
	data = make([]byte, reader.Len())
	if _, err := reader.Read(data); err != nil && err != io.EOF {
		return 0, nil, fmt.Errorf("failed to read data: %w", err)
	}

	return int(id), data, nil
}

// GetSchema retrieves the schema for the decoded data
func (d *Decoder) GetSchema(schemaID int) (*Schema, error) {
	return d.registry.GetSchemaByID(schemaID)
}
