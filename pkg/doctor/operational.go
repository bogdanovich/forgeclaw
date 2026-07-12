package doctor

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/tasks"
)

const (
	CheckOperationalStateUnreadable = "state.unreadable"
	CheckTaskStaleActive            = "tasks.stale_active"
	CheckTaskRecentFailure          = "tasks.recent_failure"
	CheckDeliveryPendingTerminal    = "deliveries.pending_terminal"
	CheckDeliveryRecentFailure      = "deliveries.recent_failure"
	CheckRestartReconciliation      = "restart.reconciliation_pending"
	CheckHandoffContinuation        = "restart.continuation_pending"
	CheckHandoffFailure             = "restart.recent_failure"
)

const (
	defaultStaleTaskAge       = 30 * time.Minute
	defaultPendingDeliveryAge = 15 * time.Minute
	defaultRecentFailureAge   = 24 * time.Hour
	defaultHandoffAge         = 10 * time.Minute
)

type operationalThresholds struct {
	now                time.Time
	staleTaskAge       time.Duration
	pendingDeliveryAge time.Duration
	recentFailureAge   time.Duration
	handoffAge         time.Duration
}

type handoffSentinel struct {
	Kind               string    `json:"kind"`
	Status             string    `json:"status"`
	RequestedAt        time.Time `json:"requested_at"`
	UpdatedAt          time.Time `json:"updated_at"`
	ContinuationSentAt time.Time `json:"continuation_sent_at"`
}

func runOperationalChecks(cfg *config.Config, opts Options) []Finding {
	thresholds := normalizeOperationalThresholds(opts)
	workspaces := configuredWorkspaces(cfg)
	findings := make([]Finding, 0)
	for index, workspace := range workspaces {
		label := fmt.Sprintf("workspace[%d]", index)
		findings = append(findings, auditTaskState(workspace, label, thresholds)...)
	}
	if len(workspaces) > 0 {
		findings = append(findings, auditHandoffs(workspaces[0], "workspace[0]", thresholds)...)
	}
	return findings
}

func normalizeOperationalThresholds(opts Options) operationalThresholds {
	result := operationalThresholds{
		now:                opts.Now,
		staleTaskAge:       opts.StaleTaskAge,
		pendingDeliveryAge: opts.PendingDeliveryAge,
		recentFailureAge:   opts.RecentFailureAge,
		handoffAge:         opts.HandoffAge,
	}
	if result.now.IsZero() {
		result.now = time.Now()
	}
	if result.staleTaskAge <= 0 {
		result.staleTaskAge = defaultStaleTaskAge
	}
	if result.pendingDeliveryAge <= 0 {
		result.pendingDeliveryAge = defaultPendingDeliveryAge
	}
	if result.recentFailureAge <= 0 {
		result.recentFailureAge = defaultRecentFailureAge
	}
	if result.handoffAge <= 0 {
		result.handoffAge = defaultHandoffAge
	}
	return result
}

func configuredWorkspaces(cfg *config.Config) []string {
	seen := map[string]struct{}{}
	normalize := func(value string) string {
		value = filepath.Clean(strings.TrimSpace(value))
		if value == "" || value == "." {
			return ""
		}
		return value
	}
	defaultWorkspace := normalize(cfg.Agents.Defaults.Workspace)
	if defaultWorkspace != "" {
		seen[defaultWorkspace] = struct{}{}
	}
	others := make([]string, 0, len(cfg.Agents.List))
	for _, agent := range cfg.Agents.List {
		workspace := normalize(agent.Workspace)
		if workspace == "" {
			continue
		}
		if _, exists := seen[workspace]; !exists {
			seen[workspace] = struct{}{}
			others = append(others, workspace)
		}
	}
	sort.Strings(others)
	result := make([]string, 0, len(seen))
	if defaultWorkspace != "" {
		result = append(result, defaultWorkspace)
	}
	result = append(result, others...)
	return result
}

func auditTaskState(workspace, label string, thresholds operationalThresholds) []Finding {
	path := filepath.Join(workspace, "state", "task_registry.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return []Finding{stateUnreadableFinding(label + "/state/task_registry.json")}
	}
	var snapshot tasks.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return []Finding{stateUnreadableFinding(label + "/state/task_registry.json")}
	}

	counts := struct{ stale, taskFailure, pending, deliveryFailure int }{}
	nowMillis := thresholds.now.UnixMilli()
	for _, record := range snapshot.Tasks {
		ref := taskReferenceTime(record)
		age := durationSinceMillis(nowMillis, ref)
		switch record.Status {
		case tasks.StatusQueued, tasks.StatusRunning:
			if ref == 0 || age >= thresholds.staleTaskAge {
				counts.stale++
			}
		case tasks.StatusFailed, tasks.StatusTimedOut, tasks.StatusLost:
			if ref > 0 && age <= thresholds.recentFailureAge {
				counts.taskFailure++
			}
		}
		if isTerminalTaskStatus(record.Status) && record.DeliveryStatus == tasks.DeliveryPending &&
			(ref == 0 || age >= thresholds.pendingDeliveryAge) {
			counts.pending++
		}
		if (record.DeliveryStatus == tasks.DeliveryFailed || record.DeliveryStatus == tasks.DeliveryParentMissing) &&
			ref > 0 && age <= thresholds.recentFailureAge {
			counts.deliveryFailure++
		}
	}

	var findings []Finding
	statePath := label + "/state/task_registry.json"
	if counts.stale > 0 {
		findings = append(findings, operationalCountFinding(CheckTaskStaleActive, SeverityFail,
			"Active tasks have stopped reporting progress",
			"Queued or running tasks older than the stale threshold may have lost their runtime owner.",
			"Inspect task_status and restart or reconcile the affected tasks.", statePath, counts.stale))
	}
	if counts.pending > 0 {
		findings = append(findings, operationalCountFinding(
			CheckDeliveryPendingTerminal,
			SeverityWarning,
			"Terminal tasks still have pending delivery",
			"Completed tasks with old pending delivery may never have reached their intended recipient.",
			"Inspect delivery status and retry or explicitly settle the affected deliveries.",
			statePath,
			counts.pending,
		))
	}
	if counts.taskFailure > 0 {
		findings = append(findings, operationalCountFinding(CheckTaskRecentFailure, SeverityWarning,
			"Tasks failed recently", "Recent failed, timed-out, or lost tasks may require operator attention.",
			"Inspect task_status for failure details and retry where appropriate.", statePath, counts.taskFailure))
	}
	if counts.deliveryFailure > 0 {
		findings = append(findings, operationalCountFinding(
			CheckDeliveryRecentFailure,
			SeverityWarning,
			"Task deliveries failed recently",
			"Recent failed or parent-missing deliveries indicate lost user-facing results.",
			"Inspect task_status and channel health, then retry delivery where safe.",
			statePath,
			counts.deliveryFailure,
		))
	}
	return findings
}

func auditHandoffs(workspace, label string, thresholds operationalThresholds) []Finding {
	paths := []string{
		filepath.Join("state", "gateway-restart", "restart-sentinel.json"),
		filepath.Join("state", "gateway-deploy", "deploy-sentinel.json"),
	}
	var findings []Finding
	for _, relative := range paths {
		data, err := os.ReadFile(filepath.Join(workspace, relative))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		evidencePath := label + "/" + filepath.ToSlash(relative)
		if err != nil {
			findings = append(findings, stateUnreadableFinding(evidencePath))
			continue
		}
		var sentinel handoffSentinel
		if err := json.Unmarshal(data, &sentinel); err != nil {
			findings = append(findings, stateUnreadableFinding(evidencePath))
			continue
		}
		ref := sentinel.UpdatedAt
		if ref.IsZero() {
			ref = sentinel.RequestedAt
		}
		age := thresholds.now.Sub(ref)
		if (sentinel.Status == "pending" || sentinel.Status == "running") &&
			(ref.IsZero() || age >= thresholds.handoffAge) {
			findings = append(findings, newFinding(CheckRestartReconciliation, SeverityFail,
				"Gateway handoff remains active past its threshold",
				"An old pending or running sentinel indicates restart/deploy reconciliation did not settle.",
				"Inspect gateway_handoff_status and gateway logs before retrying the operation.",
				Evidence{Path: evidencePath, Summary: "one unresolved handoff sentinel is present"}))
		}
		if isTerminalHandoffStatus(sentinel.Status) && sentinel.ContinuationSentAt.IsZero() &&
			(ref.IsZero() || age >= thresholds.handoffAge) {
			findings = append(findings, newFinding(CheckHandoffContinuation, SeverityWarning,
				"Gateway handoff continuation was not delivered",
				"A terminal handoff without continuation acknowledgement may have left the requesting chat uninformed.",
				"Inspect gateway_handoff_status and channel delivery, then acknowledge or retry safely.",
				Evidence{Path: evidencePath, Summary: "one terminal handoff lacks continuation acknowledgement"}))
		}
		if sentinel.Status == "failed" && !ref.IsZero() && age <= thresholds.recentFailureAge {
			findings = append(findings, newFinding(CheckHandoffFailure, SeverityWarning,
				"Gateway handoff failed recently", "A recent restart or deploy failure may need operator attention.",
				"Inspect gateway_handoff_status and logs before retrying.",
				Evidence{Path: evidencePath, Summary: "one recent handoff failure is present"}))
		}
	}
	return findings
}

func stateUnreadableFinding(path string) Finding {
	return newFinding(CheckOperationalStateUnreadable, SeverityError, "Operational state is unreadable",
		"Doctor cannot safely audit missing semantics in an unreadable or malformed state file.",
		"Check file permissions and JSON integrity; restore from a trusted backup if needed.",
		Evidence{Path: path, Summary: "state file could not be read or decoded; contents omitted"})
}

func operationalCountFinding(
	id string,
	severity Severity,
	title, rationale, remediation, path string,
	count int,
) Finding {
	return newFinding(id, severity, title, rationale, remediation,
		Evidence{Path: path, Summary: fmt.Sprintf("%d matching record(s); record details omitted", count)})
}

func taskReferenceTime(record tasks.Record) int64 {
	for _, value := range []int64{record.LastEventAt, record.EndedAt, record.StartedAt, record.CreatedAt} {
		if value > 0 {
			return value
		}
	}
	return 0
}

func durationSinceMillis(now, then int64) time.Duration {
	if then <= 0 || then >= now {
		return 0
	}
	return time.Duration(now-then) * time.Millisecond
}

func isTerminalTaskStatus(status tasks.Status) bool {
	switch status {
	case tasks.StatusSucceeded, tasks.StatusFailed, tasks.StatusTimedOut, tasks.StatusCancelled, tasks.StatusLost:
		return true
	default:
		return false
	}
}

func isTerminalHandoffStatus(status string) bool {
	return status == "succeeded" || status == "failed"
}
