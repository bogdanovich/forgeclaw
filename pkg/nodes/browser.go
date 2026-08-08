package nodes

import (
	"encoding/json"
	"fmt"
	"sort"
)

const (
	BrowserDriverPlaywrightMCP = "playwright_mcp"
	BrowserProfileManaged      = "managed"
	BrowserNetworkExactOrigins = "exact_origins"
	BrowserNetworkPublicWeb    = "public_web"
	BrowserNetworkAnyHTTP      = "any_http"

	MaxBrowserProfiles         = 8
	MaxBrowserActions          = 2
	MaxBrowserSessions         = 1
	MaxBrowserTabs             = 4
	MaxBrowserSessionSeconds   = 60 * 60
	MaxBrowserIdleSeconds      = 10 * 60
	MaxBrowserPreparedSeconds  = 5 * 60
	MaxBrowserActionSeconds    = 60
	MaxBrowserURLBytes         = 16 * 1024
	MaxBrowserTitleBytes       = 4 * 1024
	MaxBrowserSnapshotBytes    = 256 * 1024
	MaxBrowserScreenshotBytes  = 8 * 1024 * 1024
	MaxBrowserUploadBytes      = 32 * 1024 * 1024
	MaxBrowserDownloadBytes    = 32 * 1024 * 1024
	MaxBrowserSnapshotRefs     = 500
	MaxBrowserTextInputBytes   = 16 * 1024
	MaxBrowserToolResultBytes  = 320 * 1024
	MaxBrowserRetentionSeconds = 7 * 24 * 60 * 60
	MinBrowserToolResultBytes  = 64 * 1024
)

const (
	BrowserCommandSessionOpen   = "browser.session.open.v1"
	BrowserCommandSessionStatus = "browser.session.status.v1"
	BrowserCommandObserve       = "browser.observe.v1"
	BrowserCommandAct           = "browser.act.v1"
	BrowserCommandSessionClose  = "browser.session.close.v1"
)

type BrowserLimits struct {
	Sessions        int `json:"sessions"`
	Tabs            int `json:"tabs"`
	SessionSeconds  int `json:"session_seconds"`
	IdleSeconds     int `json:"idle_seconds"`
	PreparedSeconds int `json:"prepared_seconds"`
	ActionSeconds   int `json:"action_seconds"`
	SnapshotBytes   int `json:"snapshot_bytes"`
	ScreenshotBytes int `json:"screenshot_bytes"`
	UploadBytes     int `json:"upload_bytes"`
	DownloadBytes   int `json:"download_bytes"`
	SnapshotRefs    int `json:"snapshot_refs"`
	TextInputBytes  int `json:"text_input_bytes"`
	ToolResultBytes int `json:"tool_result_bytes"`
	RetentionSecs   int `json:"retention_seconds"`
}

func (limits BrowserLimits) Validate() error {
	if limits.Sessions != MaxBrowserSessions || limits.Tabs <= 0 || limits.Tabs > MaxBrowserTabs ||
		limits.SessionSeconds <= 0 || limits.SessionSeconds > MaxBrowserSessionSeconds ||
		limits.IdleSeconds <= 0 || limits.IdleSeconds > limits.SessionSeconds ||
		limits.IdleSeconds > MaxBrowserIdleSeconds ||
		limits.PreparedSeconds <= 0 || limits.PreparedSeconds > limits.SessionSeconds ||
		limits.PreparedSeconds > MaxBrowserPreparedSeconds ||
		limits.ActionSeconds <= 0 || limits.ActionSeconds > MaxBrowserActionSeconds ||
		limits.SnapshotBytes <= 0 || limits.SnapshotBytes > MaxBrowserSnapshotBytes ||
		limits.ScreenshotBytes <= 0 || limits.ScreenshotBytes > MaxBrowserScreenshotBytes ||
		limits.UploadBytes <= 0 || limits.UploadBytes > MaxBrowserUploadBytes ||
		limits.DownloadBytes <= 0 || limits.DownloadBytes > MaxBrowserDownloadBytes ||
		limits.SnapshotRefs <= 0 || limits.SnapshotRefs > MaxBrowserSnapshotRefs ||
		limits.TextInputBytes <= 0 || limits.TextInputBytes > MaxBrowserTextInputBytes ||
		limits.ToolResultBytes < MinBrowserToolResultBytes ||
		limits.ToolResultBytes > MaxBrowserToolResultBytes ||
		limits.RetentionSecs <= 0 || limits.RetentionSecs > MaxBrowserRetentionSeconds {
		return fmt.Errorf("%w: malformed browser limits", ErrInvalidCapability)
	}
	return nil
}

func (limits BrowserLimits) Effective() BrowserLimits {
	return BrowserLimits{
		Sessions:        effectiveBrowserLimit(limits.Sessions, MaxBrowserSessions),
		Tabs:            effectiveBrowserLimit(limits.Tabs, MaxBrowserTabs),
		SessionSeconds:  effectiveBrowserLimit(limits.SessionSeconds, MaxBrowserSessionSeconds),
		IdleSeconds:     effectiveBrowserLimit(limits.IdleSeconds, MaxBrowserIdleSeconds),
		PreparedSeconds: effectiveBrowserLimit(limits.PreparedSeconds, MaxBrowserPreparedSeconds),
		ActionSeconds:   effectiveBrowserLimit(limits.ActionSeconds, MaxBrowserActionSeconds),
		SnapshotBytes:   effectiveBrowserLimit(limits.SnapshotBytes, MaxBrowserSnapshotBytes),
		ScreenshotBytes: effectiveBrowserLimit(limits.ScreenshotBytes, MaxBrowserScreenshotBytes),
		UploadBytes:     effectiveBrowserLimit(limits.UploadBytes, MaxBrowserUploadBytes),
		DownloadBytes:   effectiveBrowserLimit(limits.DownloadBytes, MaxBrowserDownloadBytes),
		SnapshotRefs:    effectiveBrowserLimit(limits.SnapshotRefs, MaxBrowserSnapshotRefs),
		TextInputBytes:  effectiveBrowserLimit(limits.TextInputBytes, MaxBrowserTextInputBytes),
		ToolResultBytes: effectiveBrowserLimit(limits.ToolResultBytes, MaxBrowserToolResultBytes),
		RetentionSecs:   effectiveBrowserLimit(limits.RetentionSecs, MaxBrowserRetentionSeconds),
	}
}

func effectiveBrowserLimit(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

// BrowserProfileDescriptor is the model-safe projection of companion-local
// browser authority. Driver commands, endpoints, profile paths, lock paths,
// environment, and credentials intentionally never cross the node boundary.
type BrowserProfileDescriptor struct {
	Alias       string        `json:"alias"`
	Revision    string        `json:"revision"`
	Driver      string        `json:"driver"`
	Mode        string        `json:"mode"`
	NetworkMode string        `json:"network_mode"`
	DryRun      bool          `json:"dry_run"`
	Headed      bool          `json:"headed"`
	Actions     []string      `json:"actions"`
	Limits      BrowserLimits `json:"limits"`
}

func (profile BrowserProfileDescriptor) Validate() error {
	if err := (Alias(profile.Alias)).Validate(); err != nil ||
		!validInvocationIdentifier(profile.Revision) ||
		profile.Driver != BrowserDriverPlaywrightMCP || profile.Mode != BrowserProfileManaged ||
		(profile.NetworkMode != BrowserNetworkExactOrigins &&
			profile.NetworkMode != BrowserNetworkPublicWeb && profile.NetworkMode != BrowserNetworkAnyHTTP) ||
		!profile.DryRun || len(profile.Actions) == 0 ||
		len(profile.Actions) > MaxBrowserActions || !sort.StringsAreSorted(profile.Actions) {
		return fmt.Errorf("%w: malformed browser profile descriptor", ErrInvalidCapability)
	}
	seen := make(map[string]struct{}, len(profile.Actions))
	for _, action := range profile.Actions {
		if action != "download" && action != "navigate" {
			return fmt.Errorf("%w: unsupported browser action", ErrInvalidCapability)
		}
		if _, duplicate := seen[action]; duplicate {
			return fmt.Errorf("%w: duplicate browser action", ErrInvalidCapability)
		}
		seen[action] = struct{}{}
	}
	return profile.Limits.Validate()
}

func IsBrowserCommand(name string) bool {
	switch name {
	case BrowserCommandSessionOpen, BrowserCommandSessionStatus, BrowserCommandObserve,
		BrowserCommandAct, BrowserCommandSessionClose:
		return true
	default:
		return false
	}
}

// BrowserCommandDescriptors returns the internal typed capability catalog for
// one or more already-normalized companion profiles. It intentionally omits a
// model contract: the gateway browser broker, not nodes_invoke, is the only
// model-facing surface.
func BrowserCommandDescriptors(profiles []BrowserProfileDescriptor) ([]CommandDescriptor, error) {
	profiles = CloneBrowserProfileDescriptors(profiles)
	if err := validateBrowserProfiles(profiles); err != nil {
		return nil, err
	}
	commands := []struct {
		name string
		risk Risk
	}{
		{BrowserCommandSessionOpen, RiskWrite},
		{BrowserCommandSessionStatus, RiskRead},
		{BrowserCommandObserve, RiskRead},
		{BrowserCommandAct, RiskWrite},
		{BrowserCommandSessionClose, RiskWrite},
	}
	result := make([]CommandDescriptor, 0, len(commands))
	for _, command := range commands {
		result = append(result, CommandDescriptor{
			Name:            command.name,
			InputSchema:     BrowserCommandInputSchema(command.name, profiles),
			OutputSchema:    BrowserCommandOutputSchema(command.name),
			Risk:            command.risk,
			BrowserProfiles: CloneBrowserProfileDescriptors(profiles),
		})
	}
	if err := (CapabilityCatalog{Commands: result}).Validate(); err != nil {
		return nil, err
	}
	return result, nil
}

func validateBrowserProfiles(profiles []BrowserProfileDescriptor) error {
	if len(profiles) == 0 || len(profiles) > MaxBrowserProfiles {
		return fmt.Errorf("%w: malformed browser profile count", ErrInvalidCapability)
	}
	prior := ""
	revisions := make(map[string]struct{}, len(profiles))
	for _, profile := range profiles {
		if err := profile.Validate(); err != nil {
			return err
		}
		if prior != "" && profile.Alias <= prior {
			return fmt.Errorf("%w: browser profiles are not sorted", ErrInvalidCapability)
		}
		if _, duplicate := revisions[profile.Revision]; duplicate {
			return fmt.Errorf("%w: duplicate browser profile revision", ErrInvalidCapability)
		}
		revisions[profile.Revision] = struct{}{}
		prior = profile.Alias
	}
	return nil
}

func CloneBrowserProfileDescriptors(profiles []BrowserProfileDescriptor) []BrowserProfileDescriptor {
	result := make([]BrowserProfileDescriptor, len(profiles))
	for index := range profiles {
		result[index] = profiles[index]
		result[index].Actions = append([]string(nil), profiles[index].Actions...)
	}
	return result
}

func BrowserCommandInputSchema(command string, profiles []BrowserProfileDescriptor) json.RawMessage {
	profileBranches := make([]any, 0, len(profiles))
	actionBranches := make([]any, 0, len(profiles))
	allActions := make(map[string]struct{})
	for _, profile := range profiles {
		profileBranches = append(profileBranches, map[string]any{
			"required": []string{"profile", "profile_revision"},
			"properties": map[string]any{
				"profile":          map[string]any{"const": profile.Alias},
				"profile_revision": map[string]any{"const": profile.Revision},
			},
		})
		for _, action := range profile.Actions {
			allActions[action] = struct{}{}
		}
		for _, action := range profile.Actions {
			effect := "navigation"
			if action == "download" {
				effect = "download"
			}
			actionBranches = append(actionBranches, map[string]any{
				"required": []string{"profile_revision", "action", "effect"},
				"properties": map[string]any{
					"profile_revision": map[string]any{"const": profile.Revision},
					"action":           browserActionSchema([]string{action}),
					"effect":           map[string]any{"const": effect},
				},
			})
		}
	}
	actions := make([]string, 0, len(allActions))
	for action := range allActions {
		actions = append(actions, action)
	}
	sort.Strings(actions)
	identifier := map[string]any{"type": "string", "minLength": 1, "maxLength": MaxIDLength}
	digest := map[string]any{"type": "string", "pattern": "^[a-f0-9]{64}$"}
	properties := map[string]any{}
	required := []string{}
	var profileConstraint any
	add := func(name string, schema any) {
		properties[name] = schema
		required = append(required, name)
	}
	switch command {
	case BrowserCommandSessionOpen:
		add("session_id", identifier)
		add("profile", identifier)
		add("profile_revision", identifier)
		profileConstraint = map[string]any{"oneOf": profileBranches}
		add("browser_policy_revision", digest)
		add("dry_run", map[string]any{"const": true})
		add("limits", browserLimitsSchema())
	case BrowserCommandSessionStatus, BrowserCommandSessionClose:
		add("session_id", identifier)
		add("profile_revision", identifier)
	case BrowserCommandObserve:
		add("session_id", identifier)
		add("tab_id", identifier)
		add("snapshot_generation", map[string]any{"type": "integer", "minimum": 1})
		add("screenshot", map[string]any{"type": "boolean"})
	case BrowserCommandAct:
		add("session_id", identifier)
		add("tab_id", identifier)
		add("snapshot_generation", map[string]any{"type": "integer", "minimum": 1})
		add("action_invocation_id", identifier)
		add("action", browserActionSchema(actions))
		add("effect", map[string]any{"enum": []string{"navigation", "download"}})
		add("prepared_action_hash", digest)
		add("browser_policy_revision", digest)
		add("profile_revision", identifier)
		properties["approval_digest"] = digest
		profileConstraint = map[string]any{"oneOf": actionBranches}
	default:
		return json.RawMessage("false")
	}
	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": required, "properties": properties,
	}
	if profileConstraint != nil {
		schema["allOf"] = []any{profileConstraint}
	}
	return mustJSON(schema)
}

func BrowserCommandOutputSchema(command string) json.RawMessage {
	identifier := map[string]any{"type": "string", "minLength": 1, "maxLength": MaxIDLength}
	state := map[string]any{
		"enum": []string{"opening", "ready", "closing", "closed", "lost", "unknown"},
	}
	safeReason := map[string]any{"type": "string", "maxLength": 128}
	baseStatus := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"session_id", "state"},
		"properties": map[string]any{
			"session_id": identifier, "state": state, "reason": safeReason,
			"recovery": map[string]any{"enum": []string{"none", "retry_status", "close", "operator"}},
		},
	}
	switch command {
	case BrowserCommandSessionStatus, BrowserCommandSessionClose:
		return mustJSON(baseStatus)
	case BrowserCommandSessionOpen:
		return mustJSON(map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required": []string{
				"session_id",
				"state",
				"tab_id",
				"controller",
				"features",
				"expires_at",
				"idle_expires_at",
			},
			"properties": map[string]any{
				"session_id": identifier, "state": state, "tab_id": identifier,
				"controller": map[string]any{"const": "agent"},
				"features": map[string]any{
					"type": "object", "additionalProperties": false,
					"required": []string{"observe", "navigate", "screenshot", "download"},
					"properties": map[string]any{
						"observe": map[string]any{"type": "boolean"}, "navigate": map[string]any{"type": "boolean"},
						"screenshot": map[string]any{"type": "boolean"}, "download": map[string]any{"type": "boolean"},
					},
				},
				"expires_at":      map[string]any{"type": "integer", "minimum": 1},
				"idle_expires_at": map[string]any{"type": "integer", "minimum": 1},
			},
		})
	case BrowserCommandObserve:
		return browserObservationSchema()
	case BrowserCommandAct:
		return mustJSON(map[string]any{
			"type": "object", "additionalProperties": false,
			"required": []string{"action_invocation_id", "state"},
			"properties": map[string]any{
				"action_invocation_id": identifier,
				"state":                map[string]any{"enum": []string{"accepted", "succeeded", "failed", "unknown"}},
				"reason":               safeReason,
				"observation":          rawSchema(browserObservationSchema()),
				"artifact":             browserArtifactSchema(MaxBrowserDownloadBytes),
			},
		})
	default:
		return json.RawMessage("false")
	}
}

func browserLimitsSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required": []string{
			"sessions",
			"tabs",
			"session_seconds",
			"idle_seconds",
			"prepared_seconds",
			"action_seconds",
			"snapshot_bytes",
			"screenshot_bytes",
			"upload_bytes",
			"download_bytes",
			"snapshot_refs",
			"text_input_bytes",
			"tool_result_bytes",
			"retention_seconds",
		},
		"properties": map[string]any{
			"sessions":         map[string]any{"const": MaxBrowserSessions},
			"tabs":             map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserTabs},
			"session_seconds":  map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserSessionSeconds},
			"idle_seconds":     map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserIdleSeconds},
			"prepared_seconds": map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserPreparedSeconds},
			"action_seconds":   map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserActionSeconds},
			"snapshot_bytes":   map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserSnapshotBytes},
			"screenshot_bytes": map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserScreenshotBytes},
			"upload_bytes":     map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserUploadBytes},
			"download_bytes":   map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserDownloadBytes},
			"snapshot_refs":    map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserSnapshotRefs},
			"text_input_bytes": map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserTextInputBytes},
			"tool_result_bytes": map[string]any{
				"type":    "integer",
				"minimum": MinBrowserToolResultBytes,
				"maximum": MaxBrowserToolResultBytes,
			},
			"retention_seconds": map[string]any{"type": "integer", "minimum": 1, "maximum": MaxBrowserRetentionSeconds},
		},
	}
}

func browserActionSchema(actions []string) map[string]any {
	branches := make([]any, 0, len(actions))
	for _, action := range actions {
		switch action {
		case "navigate":
			branches = append(branches, map[string]any{
				"type": "object", "additionalProperties": false,
				"required": []string{"kind", "url"},
				"properties": map[string]any{
					"kind": map[string]any{"const": "navigate"},
					"url":  map[string]any{"type": "string", "minLength": 1, "maxLength": MaxBrowserURLBytes},
				},
			})
		case "download":
			branches = append(branches, map[string]any{
				"type": "object", "additionalProperties": false,
				"required": []string{"kind", "ref"},
				"properties": map[string]any{
					"kind": map[string]any{"const": "download"},
					"ref":  map[string]any{"type": "string", "minLength": 1, "maxLength": MaxIDLength},
				},
			})
		}
	}
	return map[string]any{"oneOf": branches}
}

func browserObservationSchema() json.RawMessage {
	return mustJSON(map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required": []string{
			"session_id",
			"tab_id",
			"snapshot_generation",
			"url",
			"origin",
			"snapshot",
			"elements",
			"truncated",
		},
		"properties": map[string]any{
			"session_id":          map[string]any{"type": "string", "minLength": 1, "maxLength": MaxIDLength},
			"tab_id":              map[string]any{"type": "string", "minLength": 1, "maxLength": MaxIDLength},
			"snapshot_generation": map[string]any{"type": "integer", "minimum": 1},
			"url":                 map[string]any{"type": "string", "maxLength": MaxBrowserURLBytes},
			"origin":              map[string]any{"type": "string", "maxLength": MaxBrowserURLBytes},
			"title":               map[string]any{"type": "string", "maxLength": MaxBrowserTitleBytes},
			"snapshot":            map[string]any{"type": "string", "maxLength": MaxBrowserSnapshotBytes},
			"truncated":           map[string]any{"type": "boolean"},
			"elements": map[string]any{
				"type": "array", "maxItems": 500,
				"items": map[string]any{
					"type": "object", "additionalProperties": false,
					"required": []string{"ref", "role", "name"},
					"properties": map[string]any{
						"ref":  map[string]any{"type": "string", "minLength": 1, "maxLength": MaxIDLength},
						"role": map[string]any{"type": "string", "maxLength": 128},
						"name": map[string]any{"type": "string", "maxLength": 4096},
					},
				},
			},
			"screenshot": browserArtifactSchema(MaxBrowserScreenshotBytes),
		},
	})
}

func browserArtifactSchema(maximumBytes int) map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"transfer_id", "sha256", "size", "content_type"},
		"properties": map[string]any{
			"transfer_id":  map[string]any{"type": "string", "minLength": 1, "maxLength": MaxIDLength},
			"sha256":       map[string]any{"type": "string", "pattern": "^[a-f0-9]{64}$"},
			"size":         map[string]any{"type": "integer", "minimum": 1, "maximum": maximumBytes},
			"content_type": map[string]any{"type": "string", "minLength": 1, "maxLength": 255},
		},
	}
}

func rawSchema(data json.RawMessage) map[string]any {
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		return map[string]any{"not": map[string]any{}}
	}
	return schema
}

func mustJSON(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage("false")
	}
	return data
}
