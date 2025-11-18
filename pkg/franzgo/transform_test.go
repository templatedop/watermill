package franzgo

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCommonTransformations_AddHeader(t *testing.T) {
	ct := &CommonTransformations{}
	transformer := ct.AddHeader("test-key", "test-value")

	record := &kgo.Record{
		Topic: "test",
		Value: []byte("value"),
	}

	transformed, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(transformed.Headers) != 1 {
		t.Fatalf("Expected 1 header, got %d", len(transformed.Headers))
	}

	if transformed.Headers[0].Key != "test-key" {
		t.Errorf("Expected header key 'test-key', got '%s'", transformed.Headers[0].Key)
	}

	if string(transformed.Headers[0].Value) != "test-value" {
		t.Errorf("Expected header value 'test-value', got '%s'", transformed.Headers[0].Value)
	}
}

func TestCommonTransformations_RemoveHeader(t *testing.T) {
	ct := &CommonTransformations{}
	transformer := ct.RemoveHeader("unwanted")

	record := &kgo.Record{
		Topic: "test",
		Value: []byte("value"),
		Headers: []kgo.RecordHeader{
			{Key: "keep", Value: []byte("keep-value")},
			{Key: "unwanted", Value: []byte("unwanted-value")},
		},
	}

	transformed, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(transformed.Headers) != 1 {
		t.Fatalf("Expected 1 header after removal, got %d", len(transformed.Headers))
	}

	if transformed.Headers[0].Key != "keep" {
		t.Errorf("Expected remaining header to be 'keep', got '%s'", transformed.Headers[0].Key)
	}
}

func TestCommonTransformations_SetKey(t *testing.T) {
	ct := &CommonTransformations{}
	newKey := []byte("new-key")
	transformer := ct.SetKey(newKey)

	record := &kgo.Record{
		Topic: "test",
		Key:   []byte("old-key"),
		Value: []byte("value"),
	}

	transformed, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !bytes.Equal(transformed.Key, newKey) {
		t.Errorf("Expected key '%s', got '%s'", newKey, transformed.Key)
	}
}

func TestCommonTransformations_JSONTransform(t *testing.T) {
	ct := &CommonTransformations{}

	// Add a field to JSON
	transformer := ct.JSONTransform(func(data map[string]interface{}) (map[string]interface{}, error) {
		data["processed"] = true
		data["count"] = float64(data["count"].(float64)) * 2
		return data, nil
	})

	inputData := map[string]interface{}{
		"name":  "test",
		"count": float64(5),
	}
	inputJSON, _ := json.Marshal(inputData)

	record := &kgo.Record{
		Topic: "test",
		Value: inputJSON,
	}

	transformed, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var outputData map[string]interface{}
	if err := json.Unmarshal(transformed.Value, &outputData); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	if processed, ok := outputData["processed"].(bool); !ok || !processed {
		t.Error("Expected 'processed' field to be true")
	}

	if count, ok := outputData["count"].(float64); !ok || count != 10 {
		t.Errorf("Expected count to be 10, got %v", count)
	}
}

func TestCommonTransformations_FilterByHeader(t *testing.T) {
	ct := &CommonTransformations{}
	predicate := ct.FilterByHeader("type", "important")

	tests := []struct {
		name     string
		headers  []kgo.RecordHeader
		expected bool
	}{
		{
			name: "Matching header",
			headers: []kgo.RecordHeader{
				{Key: "type", Value: []byte("important")},
			},
			expected: true,
		},
		{
			name: "Non-matching header",
			headers: []kgo.RecordHeader{
				{Key: "type", Value: []byte("normal")},
			},
			expected: false,
		},
		{
			name:     "Missing header",
			headers:  []kgo.RecordHeader{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &kgo.Record{
				Topic:   "test",
				Headers: tt.headers,
			}

			result := predicate(record)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestCommonTransformations_FilterByKeyPrefix(t *testing.T) {
	ct := &CommonTransformations{}
	predicate := ct.FilterByKeyPrefix([]byte("user:"))

	tests := []struct {
		name     string
		key      []byte
		expected bool
	}{
		{
			name:     "Matching prefix",
			key:      []byte("user:123"),
			expected: true,
		},
		{
			name:     "Non-matching prefix",
			key:      []byte("order:123"),
			expected: false,
		},
		{
			name:     "Empty key",
			key:      []byte(""),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &kgo.Record{
				Topic: "test",
				Key:   tt.key,
			}

			result := predicate(record)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestCommonTransformations_SplitByDelimiter(t *testing.T) {
	ct := &CommonTransformations{}
	transformer := ct.SplitByDelimiter('\n')

	input := []byte("line1\nline2\nline3")
	record := &kgo.Record{
		Topic: "test",
		Key:   []byte("key"),
		Value: input,
	}

	records, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("Expected 3 records, got %d", len(records))
	}

	expectedValues := []string{"line1", "line2", "line3"}
	for i, expectedValue := range expectedValues {
		if string(records[i].Value) != expectedValue {
			t.Errorf("Record %d: expected '%s', got '%s'", i, expectedValue, records[i].Value)
		}

		if string(records[i].Key) != "key" {
			t.Errorf("Record %d: key should be preserved", i)
		}
	}
}

func TestCommonTransformations_ExpandArray(t *testing.T) {
	ct := &CommonTransformations{}
	transformer := ct.ExpandArray("items")

	inputData := map[string]interface{}{
		"order_id": "123",
		"items": []interface{}{
			map[string]interface{}{"product": "A", "qty": float64(1)},
			map[string]interface{}{"product": "B", "qty": float64(2)},
		},
	}
	inputJSON, _ := json.Marshal(inputData)

	record := &kgo.Record{
		Topic: "test",
		Value: inputJSON,
	}

	records, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("Expected 2 records, got %d", len(records))
	}

	// Verify first expanded record
	var item1 map[string]interface{}
	if err := json.Unmarshal(records[0].Value, &item1); err != nil {
		t.Fatalf("Failed to unmarshal first item: %v", err)
	}

	if item1["product"] != "A" {
		t.Errorf("Expected first item product 'A', got '%v'", item1["product"])
	}
}

func TestCommonTransformations_Enrich(t *testing.T) {
	ct := &CommonTransformations{}

	// Enrichment function that adds user details
	lookupFunc := func(record *kgo.Record) (map[string]interface{}, error) {
		return map[string]interface{}{
			"user_name":  "John Doe",
			"user_email": "john@example.com",
		}, nil
	}

	transformer := ct.Enrich(lookupFunc)

	inputData := map[string]interface{}{
		"user_id": "123",
		"action":  "login",
	}
	inputJSON, _ := json.Marshal(inputData)

	record := &kgo.Record{
		Topic: "test",
		Value: inputJSON,
	}

	transformed, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var outputData map[string]interface{}
	if err := json.Unmarshal(transformed.Value, &outputData); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	if outputData["user_name"] != "John Doe" {
		t.Error("Enrichment data not added")
	}

	if outputData["action"] != "login" {
		t.Error("Original data was lost")
	}
}

func TestCommonTransformations_ToUpperCase(t *testing.T) {
	ct := &CommonTransformations{}
	transformer := ct.ToUpperCase()

	record := &kgo.Record{
		Topic: "test",
		Value: []byte("hello world"),
	}

	transformed, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "HELLO WORLD"
	if string(transformed.Value) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, transformed.Value)
	}
}

func TestCommonTransformations_ToLowerCase(t *testing.T) {
	ct := &CommonTransformations{}
	transformer := ct.ToLowerCase()

	record := &kgo.Record{
		Topic: "test",
		Value: []byte("HELLO WORLD"),
	}

	transformed, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "hello world"
	if string(transformed.Value) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, transformed.Value)
	}
}

func TestCommonTransformations_Mask(t *testing.T) {
	ct := &CommonTransformations{}
	transformer := ct.Mask("password", "***")

	inputData := map[string]interface{}{
		"username": "john",
		"password": "secret123",
	}
	inputJSON, _ := json.Marshal(inputData)

	record := &kgo.Record{
		Topic: "test",
		Value: inputJSON,
	}

	transformed, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var outputData map[string]interface{}
	if err := json.Unmarshal(transformed.Value, &outputData); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	if outputData["password"] != "***" {
		t.Errorf("Expected password to be masked, got '%v'", outputData["password"])
	}

	if outputData["username"] != "john" {
		t.Error("Other fields should be unchanged")
	}
}

func TestCommonTransformations_ExtractField(t *testing.T) {
	ct := &CommonTransformations{}
	transformer := ct.ExtractField("user")

	inputData := map[string]interface{}{
		"user": map[string]interface{}{
			"id":   "123",
			"name": "John",
		},
		"timestamp": "2024-01-01",
	}
	inputJSON, _ := json.Marshal(inputData)

	record := &kgo.Record{
		Topic: "test",
		Value: inputJSON,
	}

	transformed, err := transformer(record)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var outputData map[string]interface{}
	if err := json.Unmarshal(transformed.Value, &outputData); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	if outputData["id"] != "123" {
		t.Errorf("Expected id '123', got '%v'", outputData["id"])
	}

	if outputData["name"] != "John" {
		t.Errorf("Expected name 'John', got '%v'", outputData["name"])
	}

	if _, exists := outputData["timestamp"]; exists {
		t.Error("Other fields should be removed")
	}
}

func TestAsyncTransformer(t *testing.T) {
	// Simple transformer that doubles a number in JSON
	transformer := func(record *kgo.Record) (*kgo.Record, error) {
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			return nil, err
		}

		if num, ok := data["num"].(float64); ok {
			data["num"] = num * 2
		}

		newValue, _ := json.Marshal(data)
		record.Value = newValue
		return record, nil
	}

	asyncTransformer := NewAsyncTransformer(transformer, 4)

	// Transform multiple records
	records := make([]*kgo.Record, 10)
	for i := 0; i < 10; i++ {
		data := map[string]interface{}{"num": float64(i)}
		value, _ := json.Marshal(data)
		records[i] = &kgo.Record{
			Topic: "test",
			Value: value,
		}
	}

	transformed, err := asyncTransformer.TransformBatch(context.Background(), records)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(transformed) != len(records) {
		t.Fatalf("Expected %d transformed records, got %d", len(records), len(transformed))
	}

	// Verify transformation
	for i, record := range transformed {
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			t.Fatalf("Failed to unmarshal record %d: %v", i, err)
		}

		expected := float64(i * 2)
		if data["num"] != expected {
			t.Errorf("Record %d: expected num=%v, got %v", i, expected, data["num"])
		}
	}
}

func BenchmarkJSONTransform(b *testing.B) {
	ct := &CommonTransformations{}
	transformer := ct.JSONTransform(func(data map[string]interface{}) (map[string]interface{}, error) {
		data["processed"] = true
		return data, nil
	})

	data := map[string]interface{}{
		"field1": "value1",
		"field2": 123,
		"field3": true,
	}
	value, _ := json.Marshal(data)

	record := &kgo.Record{
		Topic: "test",
		Value: value,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = transformer(record)
	}
}

func BenchmarkEnrich(b *testing.B) {
	ct := &CommonTransformations{}
	lookupFunc := func(record *kgo.Record) (map[string]interface{}, error) {
		return map[string]interface{}{
			"enriched": true,
		}, nil
	}

	transformer := ct.Enrich(lookupFunc)

	data := map[string]interface{}{
		"id": "123",
	}
	value, _ := json.Marshal(data)

	record := &kgo.Record{
		Topic: "test",
		Value: value,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = transformer(record)
	}
}
