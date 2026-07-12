package evaltrace

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

func Finalize(trace Trace) (Trace, error) {
	if trace.SchemaVersion == "" {
		trace.SchemaVersion = SchemaVersionV1
	}
	trace.Limits = NormalizeLimits(trace.Limits)
	sort.SliceStable(trace.Records, func(i, j int) bool {
		return trace.Records[i].Sequence < trace.Records[j].Sequence
	})
	for i := range trace.Records {
		data, err := canonicalJSON(trace.Records[i].Data)
		if err != nil {
			return Trace{}, fmt.Errorf("record %d data: %w", i+1, err)
		}
		trace.Records[i].Data = data
		digest, err := RecordDigest(trace.Records[i])
		if err != nil {
			return Trace{}, fmt.Errorf("record %d digest: %w", i+1, err)
		}
		trace.Records[i].Digest = digest
	}
	if err := Validate(trace); err != nil {
		return Trace{}, err
	}
	return trace, nil
}

func RecordDigest(record Record) (string, error) {
	record.Digest = ""
	data, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	data, err := json.Marshal(value)
	return json.RawMessage(data), err
}
