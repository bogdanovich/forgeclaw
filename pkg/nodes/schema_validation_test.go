package nodes

import (
	"encoding/json"
	"testing"
)

func TestValidateJSONSchemaRejectsInvalidNestedTypes(t *testing.T) {
	raw := json.RawMessage(`{
		"type":"object",
		"properties":{"value":{"type":"not-a-json-schema-type"}}
	}`)
	if err := validateJSONSchema(raw); err == nil {
		t.Fatal("validateJSONSchema() accepted an invalid nested type")
	}
}

func TestValidateJSONSchemaAcceptsSupportedTypeArray(t *testing.T) {
	raw := json.RawMessage(`{"type":["string","null"]}`)
	if err := validateJSONSchema(raw); err != nil {
		t.Fatalf("validateJSONSchema() error = %v", err)
	}
}

func TestValidateJSONSchemaRejectsInvalidKeywordValue(t *testing.T) {
	raw := json.RawMessage(`{"type":"string","minLength":-1}`)
	if err := validateJSONSchema(raw); err == nil {
		t.Fatal("validateJSONSchema() accepted a negative minLength")
	}
}

func TestValidateJSONSchemaRejectsEmptyTypeArray(t *testing.T) {
	raw := json.RawMessage(`{"type":[]}`)
	if err := validateJSONSchema(raw); err == nil {
		t.Fatal("validateJSONSchema() accepted an empty type array")
	}
}
