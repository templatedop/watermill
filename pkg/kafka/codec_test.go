package kafka

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestJSONCodec(t *testing.T) {
	codec := &JSONCodec{}

	type TestStruct struct {
		Name  string `json:"name"`
		Age   int    `json:"age"`
		Email string `json:"email"`
	}

	original := TestStruct{
		Name:  "John Doe",
		Age:   30,
		Email: "john@example.com",
	}

	// Test Encode
	encoded, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	if len(encoded) == 0 {
		t.Error("Encoded data is empty")
	}

	// Test Decode
	var decoded TestStruct
	err = codec.Decode(encoded, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	// Verify
	if decoded.Name != original.Name {
		t.Errorf("Expected name %s, got %s", original.Name, decoded.Name)
	}
	if decoded.Age != original.Age {
		t.Errorf("Expected age %d, got %d", original.Age, decoded.Age)
	}
	if decoded.Email != original.Email {
		t.Errorf("Expected email %s, got %s", original.Email, decoded.Email)
	}
}

func TestStringCodec(t *testing.T) {
	codec := &StringCodec{}

	original := "Hello, Kafka!"

	// Test Encode
	encoded, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	if string(encoded) != original {
		t.Errorf("Expected %s, got %s", original, string(encoded))
	}

	// Test Decode
	var decoded string
	err = codec.Decode(encoded, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded != original {
		t.Errorf("Expected %s, got %s", original, decoded)
	}
}

func TestStringCodecInvalidType(t *testing.T) {
	codec := &StringCodec{}

	// Test encoding non-string
	_, err := codec.Encode(123)
	if err == nil {
		t.Error("Expected error when encoding non-string")
	}

	// Test decoding to non-string pointer
	var num int
	err = codec.Decode([]byte("test"), &num)
	if err == nil {
		t.Error("Expected error when decoding to non-string pointer")
	}
}

func TestBytesCodec(t *testing.T) {
	codec := &BytesCodec{}

	original := []byte("binary data here")

	// Test Encode
	encoded, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	if !bytes.Equal(encoded, original) {
		t.Error("Encoded bytes don't match original")
	}

	// Test Decode
	var decoded []byte
	err = codec.Decode(encoded, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if !bytes.Equal(decoded, original) {
		t.Error("Decoded bytes don't match original")
	}
}

func TestInt64Codec(t *testing.T) {
	codec := &Int64Codec{}

	testCases := []int64{
		0,
		1,
		-1,
		123456789,
		-987654321,
		9223372036854775807,  // max int64
		-9223372036854775808, // min int64
	}

	for _, original := range testCases {
		t.Run("", func(t *testing.T) {
			// Test Encode
			encoded, err := codec.Encode(original)
			if err != nil {
				t.Fatalf("Failed to encode %d: %v", original, err)
			}

			if len(encoded) != 8 {
				t.Errorf("Expected 8 bytes, got %d", len(encoded))
			}

			// Test Decode
			var decoded int64
			err = codec.Decode(encoded, &decoded)
			if err != nil {
				t.Fatalf("Failed to decode: %v", err)
			}

			if decoded != original {
				t.Errorf("Expected %d, got %d", original, decoded)
			}
		})
	}
}

func TestInt64CodecInvalidType(t *testing.T) {
	codec := &Int64Codec{}

	// Test encoding non-int64
	_, err := codec.Encode("not a number")
	if err == nil {
		t.Error("Expected error when encoding non-int64")
	}

	// Test decoding to non-int64 pointer
	var str string
	err = codec.Decode([]byte{0, 0, 0, 0, 0, 0, 0, 1}, &str)
	if err == nil {
		t.Error("Expected error when decoding to non-int64 pointer")
	}

	// Test decoding invalid length
	var num int64
	err = codec.Decode([]byte{0, 0, 0}, &num)
	if err == nil {
		t.Error("Expected error when decoding invalid length")
	}
}

func TestCodecRegistry(t *testing.T) {
	registry := NewCodecRegistry()

	// Test built-in codecs
	jsonCodec := registry.Get("json")
	if jsonCodec == nil {
		t.Error("Expected json codec to be registered")
	}

	stringCodec := registry.Get("string")
	if stringCodec == nil {
		t.Error("Expected string codec to be registered")
	}

	bytesCodec := registry.Get("bytes")
	if bytesCodec == nil {
		t.Error("Expected bytes codec to be registered")
	}

	int64Codec := registry.Get("int64")
	if int64Codec == nil {
		t.Error("Expected int64 codec to be registered")
	}

	// Test non-existent codec
	nilCodec := registry.Get("nonexistent")
	if nilCodec != nil {
		t.Error("Expected nil for non-existent codec")
	}

	// Test custom codec registration
	type CustomCodec struct{}
	registry.Register("custom", &CustomCodec{})

	customCodec := registry.Get("custom")
	if customCodec == nil {
		t.Error("Expected custom codec to be registered")
	}
}

func TestCodecRegistryConcurrency(t *testing.T) {
	registry := NewCodecRegistry()

	// Test concurrent registration
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			codec := &JSONCodec{}
			registry.Register(string(rune(id)), codec)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Test concurrent reads
	for i := 0; i < 10; i++ {
		go func() {
			_ = registry.Get("json")
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func BenchmarkJSONCodecEncode(b *testing.B) {
	codec := &JSONCodec{}
	data := map[string]interface{}{
		"id":    "12345",
		"name":  "Test User",
		"email": "test@example.com",
		"age":   30,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = codec.Encode(data)
	}
}

func BenchmarkJSONCodecDecode(b *testing.B) {
	codec := &JSONCodec{}
	data := map[string]interface{}{
		"id":    "12345",
		"name":  "Test User",
		"email": "test@example.com",
		"age":   30,
	}
	encoded, _ := json.Marshal(data)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var decoded map[string]interface{}
		_ = codec.Decode(encoded, &decoded)
	}
}

func BenchmarkStringCodec(b *testing.B) {
	codec := &StringCodec{}
	data := "Hello, Kafka! This is a test message."

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, _ := codec.Encode(data)
		var decoded string
		_ = codec.Decode(encoded, &decoded)
	}
}

func BenchmarkInt64Codec(b *testing.B) {
	codec := &Int64Codec{}
	data := int64(123456789)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, _ := codec.Encode(data)
		var decoded int64
		_ = codec.Decode(encoded, &decoded)
	}
}

func BenchmarkBytesCodec(b *testing.B) {
	codec := &BytesCodec{}
	data := []byte("binary data here with some length")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, _ := codec.Encode(data)
		var decoded []byte
		_ = codec.Decode(encoded, &decoded)
	}
}
