package jsonstrict

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strings"
)

var ErrDuplicateMember = errors.New("duplicate JSON object member")

var numberPattern = regexp.MustCompile(`^(-?)(0|[1-9][0-9]*)(?:\.([0-9]+))?(?:[eE]([+-]?[0-9]+))?$`)

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

// Canonical returns deterministic JSON with exact decimal normalization.
func Canonical(data []byte) ([]byte, error) {
	value, err := Decode(data)
	if err != nil {
		return nil, err
	}
	value, err = normalizeNumbers(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
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

func normalizeNumbers(value any) (any, error) {
	switch typed := value.(type) {
	case json.Number:
		return normalizeNumber(typed)
	case map[string]any:
		for key, child := range typed {
			normalized, err := normalizeNumbers(child)
			if err != nil {
				return nil, err
			}
			typed[key] = normalized
		}
	case []any:
		for index, child := range typed {
			normalized, err := normalizeNumbers(child)
			if err != nil {
				return nil, err
			}
			typed[index] = normalized
		}
	}
	return value, nil
}

func normalizeNumber(number json.Number) (json.Number, error) {
	parts := numberPattern.FindStringSubmatch(number.String())
	if parts == nil {
		return "", fmt.Errorf("invalid JSON number %q", number)
	}
	digits := strings.TrimLeft(parts[2]+parts[3], "0")
	if digits == "" {
		return json.Number("0"), nil
	}
	trailingZeros := len(digits) - len(strings.TrimRight(digits, "0"))
	digits = strings.TrimRight(digits, "0")

	exponent := big.NewInt(int64(-len(parts[3]) + trailingZeros))
	if parts[4] != "" {
		parsedExponent, ok := new(big.Int).SetString(parts[4], 10)
		if !ok {
			return "", fmt.Errorf("invalid JSON number exponent %q", parts[4])
		}
		exponent.Add(exponent, parsedExponent)
	}
	exponent.Add(exponent, big.NewInt(int64(len(digits)-1)))

	mantissa := digits[:1]
	if len(digits) > 1 {
		mantissa += "." + digits[1:]
	}
	if exponent.Sign() != 0 {
		mantissa += "e" + exponent.String()
	}
	return json.Number(parts[1] + mantissa), nil
}
