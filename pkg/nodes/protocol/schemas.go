package protocol

import (
	"embed"
	"fmt"
)

//go:embed schemas/*.json
var schemaFiles embed.FS

func Schema(name string) ([]byte, error) {
	data, err := schemaFiles.ReadFile("schemas/" + name + ".json")
	if err != nil {
		return nil, fmt.Errorf("read node protocol schema %q: %w", name, err)
	}
	return data, nil
}
