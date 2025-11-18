package schema

import (
	"fmt"

	"github.com/linkedin/goavro/v2"
)

// AvroCodec implements Avro encoding/decoding with schema registry
type AvroCodec struct {
	*BaseCodec
	codecs map[int]*goavro.Codec
}

// NewAvroCodec creates a new Avro codec
func NewAvroCodec(registry SchemaRegistry) *AvroCodec {
	return &AvroCodec{
		BaseCodec: NewBaseCodec(registry),
		codecs:    make(map[int]*goavro.Codec),
	}
}

// Encode encodes data to Avro format with schema
func (c *AvroCodec) Encode(schemaID int, data interface{}) ([]byte, error) {
	// Get codec for schema
	codec, err := c.getCodec(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get codec: %w", err)
	}

	// Encode to Avro binary
	binary, err := codec.BinaryFromNative(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode to avro: %w", err)
	}

	// Add magic byte and schema ID
	return c.EncodeWithMagicByte(schemaID, binary)
}

// Decode decodes Avro data with schema
func (c *AvroCodec) Decode(data []byte) (interface{}, int, error) {
	// Extract payload and schema ID
	payload, schemaID, err := c.DecodeWithMagicByte(data)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode magic byte: %w", err)
	}

	// Get codec for schema
	codec, err := c.getCodec(schemaID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get codec: %w", err)
	}

	// Decode from Avro binary
	native, _, err := codec.NativeFromBinary(payload)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode from avro: %w", err)
	}

	return native, schemaID, nil
}

// EncodeToJSON encodes data to Avro JSON format
func (c *AvroCodec) EncodeToJSON(schemaID int, data interface{}) ([]byte, error) {
	codec, err := c.getCodec(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get codec: %w", err)
	}

	return codec.TextualFromNative(nil, data)
}

// DecodeFromJSON decodes Avro JSON format
func (c *AvroCodec) DecodeFromJSON(schemaID int, data []byte) (interface{}, error) {
	codec, err := c.getCodec(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get codec: %w", err)
	}

	native, _, err := codec.NativeFromTextual(data)
	return native, err
}

// getCodec retrieves or creates a codec for a schema
func (c *AvroCodec) getCodec(schemaID int) (*goavro.Codec, error) {
	// Check if codec already exists
	if codec, exists := c.codecs[schemaID]; exists {
		return codec, nil
	}

	// Fetch schema from registry
	schema, err := c.registry.GetSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Create Avro codec
	codec, err := goavro.NewCodec(schema.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro codec: %w", err)
	}

	// Cache the codec
	c.codecs[schemaID] = codec

	return codec, nil
}

// AvroProducer wraps a producer with Avro serialization
type AvroProducer struct {
	codec     *AvroCodec
	subject   string
	schemaID  int
	autoRegister bool
}

// NewAvroProducer creates a new Avro producer
func NewAvroProducer(codec *AvroCodec, subject string, schemaID int) *AvroProducer {
	return &AvroProducer{
		codec:    codec,
		subject:  subject,
		schemaID: schemaID,
	}
}

// Serialize serializes data using Avro
func (p *AvroProducer) Serialize(data interface{}) ([]byte, error) {
	return p.codec.Encode(p.schemaID, data)
}

// AvroConsumer wraps a consumer with Avro deserialization
type AvroConsumer struct {
	codec *AvroCodec
}

// NewAvroConsumer creates a new Avro consumer
func NewAvroConsumer(codec *AvroCodec) *AvroConsumer {
	return &AvroConsumer{
		codec: codec,
	}
}

// Deserialize deserializes Avro data
func (c *AvroConsumer) Deserialize(data []byte) (interface{}, int, error) {
	return c.codec.Decode(data)
}

// AvroSchemaBuilder helps build Avro schemas
type AvroSchemaBuilder struct {
	namespace string
	name      string
	fields    []AvroField
}

// AvroField represents an Avro field
type AvroField struct {
	Name    string      `json:"name"`
	Type    interface{} `json:"type"`
	Default interface{} `json:"default,omitempty"`
	Doc     string      `json:"doc,omitempty"`
}

// NewAvroSchemaBuilder creates a new Avro schema builder
func NewAvroSchemaBuilder(namespace, name string) *AvroSchemaBuilder {
	return &AvroSchemaBuilder{
		namespace: namespace,
		name:      name,
		fields:    make([]AvroField, 0),
	}
}

// AddField adds a field to the schema
func (b *AvroSchemaBuilder) AddField(name string, fieldType interface{}) *AvroSchemaBuilder {
	b.fields = append(b.fields, AvroField{
		Name: name,
		Type: fieldType,
	})
	return b
}

// AddFieldWithDefault adds a field with a default value
func (b *AvroSchemaBuilder) AddFieldWithDefault(name string, fieldType interface{}, defaultValue interface{}) *AvroSchemaBuilder {
	b.fields = append(b.fields, AvroField{
		Name:    name,
		Type:    fieldType,
		Default: defaultValue,
	})
	return b
}

// AddDocumentedField adds a documented field
func (b *AvroSchemaBuilder) AddDocumentedField(name string, fieldType interface{}, doc string) *AvroSchemaBuilder {
	b.fields = append(b.fields, AvroField{
		Name: name,
		Type: fieldType,
		Doc:  doc,
	})
	return b
}

// Build builds the Avro schema JSON
func (b *AvroSchemaBuilder) Build() (string, error) {
	schema := map[string]interface{}{
		"type":      "record",
		"namespace": b.namespace,
		"name":      b.name,
		"fields":    b.fields,
	}

	// Convert to JSON string
	// Using goavro's canonical form ensures proper formatting
	codec, err := goavro.NewCodec(fmt.Sprintf(`%v`, schema))
	if err != nil {
		return "", fmt.Errorf("failed to create codec: %w", err)
	}

	return codec.Schema(), nil
}

// Common Avro types
var (
	AvroString  = "string"
	AvroInt     = "int"
	AvroLong    = "long"
	AvroFloat   = "float"
	AvroDouble  = "double"
	AvroBoolean = "boolean"
	AvroBytes   = "bytes"
	AvroNull    = "null"
)

// AvroNullable creates a nullable Avro type
func AvroNullable(typeName string) []string {
	return []string{"null", typeName}
}

// AvroArray creates an Avro array type
func AvroArray(itemType string) map[string]interface{} {
	return map[string]interface{}{
		"type":  "array",
		"items": itemType,
	}
}

// AvroMap creates an Avro map type
func AvroMap(valueType string) map[string]interface{} {
	return map[string]interface{}{
		"type":   "map",
		"values": valueType,
	}
}

// AvroEnum creates an Avro enum type
func AvroEnum(name string, symbols []string) map[string]interface{} {
	return map[string]interface{}{
		"type":    "enum",
		"name":    name,
		"symbols": symbols,
	}
}
