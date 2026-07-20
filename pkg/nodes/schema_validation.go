package nodes

import (
	"embed"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/sipeed/picoclaw/pkg/nodes/internal/jsonstrict"
)

const draft202012Prefix = "https://json-schema.org/draft/2020-12/"

//go:embed metaschema/*.json metaschema/meta/*.json
var metaSchemaFiles embed.FS

var (
	metaSchemaOnce     sync.Once
	resolvedMetaSchema *jsonschema.Resolved
	errMetaSchema      error
)

func validateJSONSchema(raw json.RawMessage) error {
	if _, err := jsonstrict.Decode(raw); err != nil {
		return err
	}
	// The validator classifies json.Number by its underlying string kind. Decode
	// a second time with encoding/json for meta-schema validation; exact numbers
	// remain preserved independently by the catalog canonicalizer.
	var instance any
	if err := json.Unmarshal(raw, &instance); err != nil {
		return err
	}
	meta, metaErr := draft202012MetaSchema()
	if metaErr != nil {
		return fmt.Errorf("initialize JSON Schema validator: %w", metaErr)
	}
	if validationErr := meta.Validate(instance); validationErr != nil {
		return validationErr
	}

	var schema jsonschema.Schema
	if decodeErr := json.Unmarshal(raw, &schema); decodeErr != nil {
		return decodeErr
	}
	_, resolveErr := schema.Resolve(nil)
	return resolveErr
}

func draft202012MetaSchema() (*jsonschema.Resolved, error) {
	metaSchemaOnce.Do(func() {
		root, err := loadDraft202012Schema("schema")
		if err != nil {
			errMetaSchema = err
			return
		}
		resolvedMetaSchema, errMetaSchema = root.Resolve(&jsonschema.ResolveOptions{
			Loader: loadDraft202012MetaSchema,
		})
	})
	return resolvedMetaSchema, errMetaSchema
}

func loadDraft202012MetaSchema(uri *url.URL) (*jsonschema.Schema, error) {
	name, ok := strings.CutPrefix(uri.String(), draft202012Prefix)
	if !ok {
		return nil, fmt.Errorf("unsupported meta-schema URI %q", uri)
	}
	name = strings.TrimSuffix(strings.SplitN(name, "#", 2)[0], ".json")
	return loadDraft202012Schema(name)
}

func loadDraft202012Schema(name string) (*jsonschema.Schema, error) {
	if name == "" || strings.Contains(name, "..") {
		return nil, fmt.Errorf("invalid embedded meta-schema name %q", name)
	}
	data, err := metaSchemaFiles.ReadFile("metaschema/" + name + ".json")
	if err != nil {
		return nil, err
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("decode embedded meta-schema %q: %w", name, err)
	}
	return &schema, nil
}
