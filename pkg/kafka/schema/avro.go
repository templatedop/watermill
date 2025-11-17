package schema

import (
	"fmt"

	"github.com/linkedin/goavro/v2"
)

// AvroCodec implements Avro encoding/decoding with schema registry
type AvroCodec struct {
	registry *RegistryClient
	subject  string
	schema   *Schema
	codec    *goavro.Codec
}

// NewAvroCodec creates a new Avro codec
func NewAvroCodec(registry *RegistryClient, subject string) (*AvroCodec, error) {
	// Get latest schema
	schema, err := registry.GetLatestSchema(subject)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest schema: %w", err)
	}

	// Create Avro codec
	codec, err := goavro.NewCodec(schema.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro codec: %w", err)
	}

	return &AvroCodec{
		registry: registry,
		subject:  subject,
		schema:   schema,
		codec:    codec,
	}, nil
}

// NewAvroCodecWithSchema creates a new Avro codec with a specific schema
func NewAvroCodecWithSchema(registry *RegistryClient, subject string, schemaString string) (*AvroCodec, error) {
	// Register schema
	schema, err := registry.RegisterSchema(subject, schemaString, SchemaTypeAvro)
	if err != nil {
		return nil, fmt.Errorf("failed to register schema: %w", err)
	}

	// Create Avro codec
	codec, err := goavro.NewCodec(schemaString)
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro codec: %w", err)
	}

	return &AvroCodec{
		registry: registry,
		subject:  subject,
		schema:   schema,
		codec:    codec,
	}, nil
}

// Encode encodes data using Avro and prepends schema ID
func (ac *AvroCodec) Encode(data interface{}) ([]byte, error) {
	// Encode using Avro
	binary, err := ac.codec.BinaryFromNative(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode Avro: %w", err)
	}

	// Prepend schema ID
	encoder := NewEncoder(ac.registry)
	encoded, err := encoder.Encode(ac.schema.ID, binary)
	if err != nil {
		return nil, fmt.Errorf("failed to encode with schema ID: %w", err)
	}

	return encoded, nil
}

// Decode decodes Avro data with schema registry
func (ac *AvroCodec) Decode(encoded []byte) (interface{}, error) {
	// Decode schema ID and data
	decoder := NewDecoder(ac.registry)
	schemaID, data, err := decoder.Decode(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to decode schema ID: %w", err)
	}

	// Get schema
	schema, err := ac.registry.GetSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Create codec for this schema
	codec, err := goavro.NewCodec(schema.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro codec: %w", err)
	}

	// Decode Avro
	native, _, err := codec.NativeFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro: %w", err)
	}

	return native, nil
}

// EncodeTyped encodes typed data using Avro
func (ac *AvroCodec) EncodeTyped(data map[string]interface{}) ([]byte, error) {
	return ac.Encode(data)
}

// DecodeTyped decodes to typed data
func (ac *AvroCodec) DecodeTyped(encoded []byte) (map[string]interface{}, error) {
	result, err := ac.Decode(encoded)
	if err != nil {
		return nil, err
	}

	typed, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("decoded value is not a map")
	}

	return typed, nil
}

// GetSchema returns the current schema
func (ac *AvroCodec) GetSchema() *Schema {
	return ac.schema
}

// UpdateSchema updates to a new schema version
func (ac *AvroCodec) UpdateSchema(version int) error {
	schema, err := ac.registry.GetSchemaByVersion(ac.subject, version)
	if err != nil {
		return fmt.Errorf("failed to get schema version %d: %w", version, err)
	}

	codec, err := goavro.NewCodec(schema.Schema)
	if err != nil {
		return fmt.Errorf("failed to create Avro codec: %w", err)
	}

	ac.schema = schema
	ac.codec = codec

	return nil
}

// AvroSchemaBuilder helps build Avro schemas
type AvroSchemaBuilder struct {
	name      string
	namespace string
	fields    []map[string]interface{}
}

// NewAvroSchemaBuilder creates a new Avro schema builder
func NewAvroSchemaBuilder(name, namespace string) *AvroSchemaBuilder {
	return &AvroSchemaBuilder{
		name:      name,
		namespace: namespace,
		fields:    make([]map[string]interface{}, 0),
	}
}

// AddField adds a field to the schema
func (asb *AvroSchemaBuilder) AddField(name, fieldType string, optional bool) *AvroSchemaBuilder {
	field := map[string]interface{}{
		"name": name,
	}

	if optional {
		field["type"] = []interface{}{"null", fieldType}
		field["default"] = nil
	} else {
		field["type"] = fieldType
	}

	asb.fields = append(asb.fields, field)
	return asb
}

// AddComplexField adds a complex field to the schema
func (asb *AvroSchemaBuilder) AddComplexField(name string, fieldType interface{}, optional bool) *AvroSchemaBuilder {
	field := map[string]interface{}{
		"name": name,
	}

	if optional {
		field["type"] = []interface{}{"null", fieldType}
		field["default"] = nil
	} else {
		field["type"] = fieldType
	}

	asb.fields = append(asb.fields, field)
	return asb
}

// Build builds the Avro schema as JSON string
func (asb *AvroSchemaBuilder) Build() (string, error) {
	schema := map[string]interface{}{
		"type":      "record",
		"name":      asb.name,
		"namespace": asb.namespace,
		"fields":    asb.fields,
	}

	// Convert to JSON
	jsonBytes, err := jsonMarshal(schema)
	if err != nil {
		return "", fmt.Errorf("failed to marshal schema: %w", err)
	}

	return string(jsonBytes), nil
}

// Example Avro schemas
const (
	OrderEventSchema = `{
		"type": "record",
		"name": "OrderEvent",
		"namespace": "com.ecommerce.events",
		"fields": [
			{"name": "order_id", "type": "string"},
			{"name": "customer_id", "type": "string"},
			{"name": "total", "type": "double"},
			{"name": "status", "type": "string"},
			{"name": "created_at", "type": "long"},
			{"name": "items", "type": {"type": "array", "items": "string"}},
			{"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": null}
		]
	}`

	PaymentEventSchema = `{
		"type": "record",
		"name": "PaymentEvent",
		"namespace": "com.ecommerce.events",
		"fields": [
			{"name": "payment_id", "type": "string"},
			{"name": "order_id", "type": "string"},
			{"name": "amount", "type": "double"},
			{"name": "currency", "type": "string"},
			{"name": "status", "type": "string"},
			{"name": "payment_method", "type": "string"},
			{"name": "processed_at", "type": "long"}
		]
	}`

	CustomerEventSchema = `{
		"type": "record",
		"name": "CustomerEvent",
		"namespace": "com.ecommerce.events",
		"fields": [
			{"name": "customer_id", "type": "string"},
			{"name": "email", "type": "string"},
			{"name": "name", "type": "string"},
			{"name": "created_at", "type": "long"},
			{"name": "tier", "type": ["null", "string"], "default": null}
		]
	}`
)
