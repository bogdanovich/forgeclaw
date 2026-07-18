package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/fileutil"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

const (
	curatedMemoryTarget           = "memory/MEMORY.md"
	maxCuratedMemoryMutationBytes = 64 * 1024
)

var curatedMemoryLocks sync.Map

type MemoryTool struct {
	workspace  string
	path       string
	invalidate func()
	events     runtimeevents.Bus
}

type memoryMutationResponse struct {
	Status    string `json:"status"`
	Operation string `json:"operation"`
	Target    string `json:"target"`
	Changed   bool   `json:"changed"`
}

type MemoryMutationPayload struct {
	Operation        string `json:"operation"`
	Outcome          string `json:"outcome"`
	Target           string `json:"target"`
	ContentHash      string `json:"content_hash,omitempty"`
	ContentBytes     int    `json:"content_bytes,omitempty"`
	ReplacementHash  string `json:"replacement_hash,omitempty"`
	ReplacementBytes int    `json:"replacement_bytes,omitempty"`
	MatchCount       int    `json:"match_count,omitempty"`
	ErrorCode        string `json:"error_code,omitempty"`
}

func NewMemoryTool(workspace string, invalidate func(), eventBus runtimeevents.Bus) *MemoryTool {
	return &MemoryTool{
		workspace:  workspace,
		path:       filepath.Join(workspace, "memory", "MEMORY.md"),
		invalidate: invalidate,
		events:     eventBus,
	}
}

func (t *MemoryTool) Name() string {
	return "memory"
}

func (t *MemoryTool) Description() string {
	return "Add, replace, or remove durable stable facts in curated workspace memory. Use replace for corrections and remove for explicit forgetting. Do not use this for transient session state or daily notes."
}

func (t *MemoryTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"operation": map[string]any{
				"type":        "string",
				"enum":        []string{"add", "replace", "remove"},
				"description": "Mutation to apply to curated stable memory.",
			},
			"content": map[string]any{
				"type":        "string",
				"description": "Content to add, or the exact existing content to replace or remove.",
			},
			"replacement": map[string]any{
				"type":        "string",
				"description": "New content. Required only for replace.",
			},
		},
		"required": []string{"operation", "content"},
	}
}

func (t *MemoryTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsMutating
}

func (t *MemoryTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyNonCancellable
}

func (t *MemoryTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	operation, err := requiredStringArg(args, "operation", "operation")
	if err != nil {
		return t.failure(ctx, "", "invalid_arguments", MemoryMutationPayload{}, err)
	}
	content, err := requiredStringArg(args, "content", "content")
	if err != nil {
		return t.failure(ctx, operation, "invalid_arguments", MemoryMutationPayload{}, err)
	}
	payload := MemoryMutationPayload{
		Operation:    operation,
		Target:       curatedMemoryTarget,
		ContentHash:  curatedMemoryHash(content),
		ContentBytes: len(content),
	}
	if len(content) > maxCuratedMemoryMutationBytes {
		return t.failure(
			ctx,
			operation,
			"content_too_large",
			payload,
			fmt.Errorf("content exceeds %d bytes", maxCuratedMemoryMutationBytes),
		)
	}

	replacement := ""
	if operation == "replace" {
		replacement, err = requiredStringArg(args, "replacement", "replacement")
		if err != nil {
			return t.failure(ctx, operation, "invalid_arguments", payload, err)
		}
		payload.ReplacementHash = curatedMemoryHash(replacement)
		payload.ReplacementBytes = len(replacement)
		if len(replacement) > maxCuratedMemoryMutationBytes {
			return t.failure(
				ctx,
				operation,
				"replacement_too_large",
				payload,
				fmt.Errorf("replacement exceeds %d bytes", maxCuratedMemoryMutationBytes),
			)
		}
	}

	lock := curatedMemoryLock(t.path)
	lock.Lock()
	defer lock.Unlock()

	validatedPath, err := validatePathWithAllowPaths(curatedMemoryTarget, t.workspace, true, nil)
	if err != nil {
		return t.failure(ctx, operation, "path_validation_failed", payload, err)
	}
	current, err := os.ReadFile(validatedPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return t.failure(ctx, operation, "read_failed", payload, fmt.Errorf("read curated memory: %w", err))
	}

	updated, outcome, changed, matches, err := applyCuratedMemoryMutation(
		string(current),
		operation,
		content,
		replacement,
	)
	payload.MatchCount = matches
	if err != nil {
		return t.failure(ctx, operation, "mutation_rejected", payload, err)
	}
	if !changed {
		payload.Outcome = outcome
		t.publish(ctx, payload, runtimeevents.SeverityInfo)
		return memoryMutationResult(operation, outcome, false)
	}

	if err := os.MkdirAll(filepath.Dir(validatedPath), 0o755); err != nil {
		return t.failure(ctx, operation, "directory_create_failed", payload, err)
	}
	validatedPath, err = validatePathWithAllowPaths(curatedMemoryTarget, t.workspace, true, nil)
	if err != nil {
		return t.failure(ctx, operation, "path_validation_failed", payload, err)
	}
	if err := fileutil.WriteFileAtomic(validatedPath, []byte(updated), 0o600); err != nil {
		return t.failure(ctx, operation, "write_failed", payload, fmt.Errorf("write curated memory: %w", err))
	}
	if t.invalidate != nil {
		t.invalidate()
	}
	payload.Outcome = outcome
	t.publish(ctx, payload, runtimeevents.SeverityInfo)
	return memoryMutationResult(operation, outcome, true).WithWriteAudit(WriteAuditEntry{
		Kind:    "memory",
		Target:  curatedMemoryTarget,
		Action:  operation,
		Tool:    t.Name(),
		Success: true,
	})
}

func applyCuratedMemoryMutation(
	current, operation, content, replacement string,
) (updated, outcome string, changed bool, matches int, err error) {
	switch operation {
	case "add":
		if normalizedMemoryContains(current, content) {
			return current, "duplicate", false, 1, nil
		}
		return appendCuratedMemory(current, content), "added", true, 0, nil
	case "replace", "remove":
		ranges := exactMemoryEntryRanges(current, content)
		matches = len(ranges)
		if matches == 0 {
			return current, "", false, 0, errors.New("content was not found in curated memory")
		}
		if matches > 1 {
			return current, "", false, matches, errors.New(
				"content matched more than one location; provide a more specific exact value",
			)
		}
		if operation == "replace" {
			if content == replacement {
				return current, "unchanged", false, 1, nil
			}
			match := ranges[0]
			updated = current[:match.start] + replacement + current[match.end:]
			return normalizeCuratedMemoryEnd(updated), "replaced", true, 1, nil
		}
		match := ranges[0]
		updated = current[:match.start] + current[match.end:]
		return normalizeCuratedMemoryEnd(updated), "removed", true, 1, nil
	default:
		return current, "", false, 0, errors.New("operation must be one of add, replace, remove")
	}
}

type memoryTextRange struct {
	start int
	end   int
}

func exactMemoryEntryRanges(current, content string) []memoryTextRange {
	var ranges []memoryTextRange
	for offset := 0; offset <= len(current)-len(content); {
		relative := strings.Index(current[offset:], content)
		if relative < 0 {
			break
		}
		start := offset + relative
		end := start + len(content)
		startsOnBoundary := start == 0 || current[start-1] == '\n'
		endsOnBoundary := end == len(current) || current[end] == '\n'
		if startsOnBoundary && endsOnBoundary {
			ranges = append(ranges, memoryTextRange{start: start, end: end})
		}
		offset = start + 1
	}
	return ranges
}

func normalizedMemoryContains(current, candidate string) bool {
	normalizedCandidate := normalizeMemoryEntry(candidate)
	if normalizedCandidate == "" {
		return false
	}
	segments := append(strings.Split(current, "\n\n"), strings.Split(current, "\n")...)
	for _, segment := range segments {
		if normalizeMemoryEntry(segment) == normalizedCandidate {
			return true
		}
	}
	return false
}

func normalizeMemoryEntry(text string) string {
	text = strings.TrimSpace(text)
	for _, prefix := range []string{"- ", "* ", "+ "} {
		text = strings.TrimPrefix(text, prefix)
	}
	return strings.ToLower(strings.Join(strings.Fields(text), " "))
}

func appendCuratedMemory(current, content string) string {
	current = strings.TrimRight(current, " \t\r\n")
	content = strings.TrimSpace(content)
	if current == "" {
		return content + "\n"
	}
	return current + "\n\n" + content + "\n"
}

func normalizeCuratedMemoryEnd(content string) string {
	content = strings.TrimRight(content, " \t\r\n")
	if content == "" {
		return ""
	}
	return content + "\n"
}

func curatedMemoryLock(path string) *sync.Mutex {
	lock, _ := curatedMemoryLocks.LoadOrStore(path, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func curatedMemoryHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func memoryMutationResult(operation, status string, changed bool) *ToolResult {
	data, _ := json.Marshal(memoryMutationResponse{
		Status:    status,
		Operation: operation,
		Target:    curatedMemoryTarget,
		Changed:   changed,
	})
	return SilentResult(string(data))
}

func (t *MemoryTool) failure(
	ctx context.Context,
	operation, errorCode string,
	payload MemoryMutationPayload,
	err error,
) *ToolResult {
	payload.Operation = operation
	payload.Target = curatedMemoryTarget
	payload.Outcome = "failed"
	payload.ErrorCode = errorCode
	t.publish(ctx, payload, runtimeevents.SeverityError)
	return ErrorResult(err.Error()).WithError(err)
}

func (t *MemoryTool) publish(
	ctx context.Context,
	payload MemoryMutationPayload,
	severity runtimeevents.Severity,
) {
	if t == nil || t.events == nil {
		return
	}
	t.events.PublishNonBlocking(runtimeevents.Event{
		Kind: runtimeevents.KindAgentMemoryMutation,
		Source: runtimeevents.Source{
			Component: "tool",
			Name:      t.Name(),
		},
		Scope: runtimeevents.Scope{
			AgentID:    ToolAgentID(ctx),
			SessionKey: ToolSessionKey(ctx),
			Channel:    ToolChannel(ctx),
			ChatID:     ToolChatID(ctx),
			TopicID:    ToolTopicID(ctx),
			SenderID:   ToolSenderID(ctx),
			MessageID:  ToolMessageID(ctx),
		},
		Severity: severity,
		Payload:  payload,
		Attrs: map[string]any{
			"operation": payload.Operation,
			"outcome":   payload.Outcome,
			"target":    payload.Target,
		},
	})
}
