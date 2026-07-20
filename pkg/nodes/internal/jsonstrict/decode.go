package jsonstrict

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

var ErrDuplicateMember = errors.New("duplicate JSON object member")

// Decode preserves JSON numbers and rejects duplicate object members at every
// nesting level. Duplicate rejection avoids parser-dependent first/last-wins
// behavior in signed, hashed, or routed protocol data.
func Decode(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := decodeValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, fmt.Errorf("read trailing JSON data: %w", err)
	}
	return value, nil
}

func decodeValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}
	switch delimiter {
	case '{':
		return decodeObject(decoder)
	case '[':
		return decodeArray(decoder)
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func decodeObject(decoder *json.Decoder) (map[string]any, error) {
	object := make(map[string]any)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, errors.New("JSON object member name is not a string")
		}
		if _, exists := object[key]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateMember, key)
		}
		value, err := decodeValue(decoder)
		if err != nil {
			return nil, err
		}
		object[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	return object, nil
}

func decodeArray(decoder *json.Decoder) ([]any, error) {
	var values []any
	for decoder.More() {
		value, err := decodeValue(decoder)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	return values, nil
}
