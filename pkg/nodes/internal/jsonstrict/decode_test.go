package jsonstrict

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestDecodeRejectsDuplicateMembersRecursively(t *testing.T) {
	for _, data := range []string{
		`{"type":"one","type":"two"}`,
		`{"schema":{"type":"one","type":"two"}}`,
		`[{"name":"one","name":"two"}]`,
	} {
		if _, err := Decode([]byte(data)); !errors.Is(err, ErrDuplicateMember) {
			t.Fatalf("Decode(%s) error = %v", data, err)
		}
	}
}

func TestDecodePreservesLargeNumbers(t *testing.T) {
	value, err := Decode([]byte(`{"maximum":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	maximum := value.(map[string]any)["maximum"]
	if maximum != json.Number("9007199254740993") {
		t.Fatalf("maximum = %#v", maximum)
	}
}

func TestCanonicalNormalizesEquivalentNumbersExactly(t *testing.T) {
	for _, data := range []string{`{"value":1}`, `{"value":1.0}`, `{"value":1e0}`} {
		canonical, err := Canonical([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		if string(canonical) != `{"value":1}` {
			t.Fatalf("Canonical(%s) = %s", data, canonical)
		}
	}
}

func TestCanonicalHandlesLargeExponentsWithoutExpansion(t *testing.T) {
	canonical, err := Canonical([]byte(`{"value":1000e999999}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != `{"value":1e1000002}` {
		t.Fatalf("Canonical() = %s", canonical)
	}
}
