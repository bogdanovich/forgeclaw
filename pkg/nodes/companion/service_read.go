package companion

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
)

func boundedServiceLogRequest(
	request ServiceLogRequest,
	limits nodes.ServiceLogLimits,
) (int, int, error) {
	if request.Entries < 0 || request.SinceSeconds < 0 {
		return 0, 0, &ServiceManagerError{Code: "input_invalid"}
	}
	entries := request.Entries
	if entries == 0 || entries > limits.EntriesMax {
		entries = limits.EntriesMax
	}
	since := request.SinceSeconds
	if since == 0 || since > limits.AgeSecondsMax {
		since = limits.AgeSecondsMax
	}
	return entries, since, nil
}

func parseSystemdStatus(raw []byte) (map[string]string, error) {
	properties := make(map[string]string, 4)
	allowed := map[string]struct{}{
		"LoadState": {}, "ActiveState": {}, "SubState": {}, "UnitFileState": {},
	}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found {
			return nil, errors.New("malformed systemd status")
		}
		if _, ok := allowed[key]; !ok {
			return nil, errors.New("unexpected systemd status property")
		}
		if _, duplicate := properties[key]; duplicate {
			return nil, errors.New("duplicate systemd status property")
		}
		properties[key] = value
	}
	if len(properties) != len(allowed) {
		return nil, errors.New("incomplete systemd status")
	}
	return properties, nil
}

func unavailableServiceStatus(service string, observedAt int64, code string) ServiceStatus {
	return ServiceStatus{
		Service:     service,
		LoadState:   "unknown",
		ActiveState: "unknown",
		Substate:    "unknown",
		Enabled:     "unknown",
		ObservedAt:  observedAt,
		Code:        code,
	}
}

func normalizeSystemdLoadState(value string) string {
	switch value {
	case "loaded", "stub", "merged":
		return "loaded"
	case "not-found":
		return "not_found"
	case "masked":
		return "masked"
	case "error", "bad-setting":
		return "error"
	default:
		return "unknown"
	}
}

func normalizeSystemdActiveState(value string) string {
	switch value {
	case "active", "inactive", "activating", "deactivating", "failed":
		return value
	default:
		return "unknown"
	}
}

func normalizeSystemdSubstate(value string) string {
	switch value {
	case "running", "exited", "dead", "failed":
		return value
	case "start-pre", "start", "start-post", "auto-restart", "auto-restart-queued":
		return "start"
	case "stop", "stop-watchdog", "stop-sigterm", "stop-sigkill", "final-sigterm", "final-sigkill":
		return "stop"
	case "reload", "reload-signal":
		return "reload"
	default:
		return "unknown"
	}
}

func normalizeSystemdEnabledState(value string) string {
	switch value {
	case "enabled", "enabled-runtime", "linked", "linked-runtime":
		return "enabled"
	case "disabled":
		return "disabled"
	case "static", "indirect", "generated", "transient", "alias":
		return "static"
	case "masked", "masked-runtime":
		return "masked"
	default:
		return "unknown"
	}
}

func parseSystemdLogs(
	service string,
	raw []byte,
	entriesMax int,
	bytesMax int,
) (ServiceLogs, error) {
	logs := ServiceLogs{Service: service, Records: make([]ServiceLogRecord, 0, entriesMax)}
	lines := bytes.Split(raw, []byte{'\n'})
	for _, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		value, err := jsonstrict.Decode(line)
		if err != nil {
			return ServiceLogs{}, &ServiceManagerError{Code: "log_record_invalid"}
		}
		object, ok := value.(map[string]any)
		if !ok {
			return ServiceLogs{}, &ServiceManagerError{Code: "log_record_invalid"}
		}
		timestampRaw, timestampOK := object["__REALTIME_TIMESTAMP"].(string)
		messageRaw, messageOK := object["MESSAGE"].(string)
		if !timestampOK || !messageOK {
			return ServiceLogs{}, &ServiceManagerError{Code: "log_record_invalid"}
		}
		micros, parseErr := strconv.ParseInt(timestampRaw, 10, 64)
		if parseErr != nil || micros <= 0 {
			return ServiceLogs{}, &ServiceManagerError{Code: "log_record_invalid"}
		}
		message, messageTruncated := sanitizeServiceLogMessage(messageRaw)
		timestamp := micros / 1_000_000
		if timestamp <= 0 {
			return ServiceLogs{}, &ServiceManagerError{Code: "log_record_invalid"}
		}
		record := ServiceLogRecord{Timestamp: timestamp, Message: message}
		if priority, found := object["PRIORITY"].(string); found {
			record.Severity = normalizeJournalPriority(priority)
		}
		candidate := logs
		candidate.Records = append(append([]ServiceLogRecord(nil), logs.Records...), record)
		candidate.Truncated = logs.Truncated || messageTruncated
		if len(candidate.Records) > entriesMax || !serviceLogsFit(candidate, bytesMax) {
			logs.Truncated = true
			break
		}
		logs = candidate
	}
	sort.SliceStable(logs.Records, func(i, j int) bool {
		return logs.Records[i].Timestamp < logs.Records[j].Timestamp
	})
	if !serviceLogsFit(logs, bytesMax) {
		return ServiceLogs{}, &ServiceManagerError{Code: "output_limit_too_small"}
	}
	return logs, nil
}

func sanitizeServiceLogMessage(message string) (string, bool) {
	var builder strings.Builder
	truncated := false
	for _, character := range strings.ToValidUTF8(message, "�") {
		if unicode.IsControl(character) && character != '\n' && character != '\t' {
			character = '�'
		}
		encodedBytes := utf8.RuneLen(character)
		if builder.Len()+encodedBytes > nodes.MaxServiceLogRecordBytes {
			truncated = true
			break
		}
		builder.WriteRune(character)
	}
	return builder.String(), truncated
}

func normalizeJournalPriority(priority string) string {
	switch priority {
	case "0", "1", "2":
		return "critical"
	case "3":
		return "error"
	case "4":
		return "warning"
	case "5":
		return "notice"
	case "6":
		return "info"
	case "7":
		return "debug"
	default:
		return ""
	}
}

func serviceLogsFit(logs ServiceLogs, bytesMax int) bool {
	data, err := json.Marshal(logs)
	return err == nil && len(data) <= bytesMax
}
