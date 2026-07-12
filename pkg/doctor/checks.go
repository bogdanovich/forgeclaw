package doctor

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/sipeed/picoclaw/pkg/config"
)

const (
	CheckGatewayPublicExposure    = "gateway.public_exposure"
	CheckChannelOpenAllowFrom     = "channels.open_allow_from"
	CheckChannelPermissiveTrigger = "channels.permissive_group_trigger"
	CheckExecRemoteWrite          = "tools.exec_remote_write"
	CheckFilesystemWriteScope     = "tools.filesystem_write_scope"
	CheckInstallSkillEnabled      = "tools.install_skill_enabled"
	CheckIsolationDisabled        = "isolation.disabled_or_ineffective"
	CheckMCPRemoteTransport       = "mcp.remote_transport"
	CheckMCPInsecureTransport     = "mcp.insecure_transport"
	CheckMCPOverexposedTransport  = "mcp.overexposed_transport"
	CheckPlaintextCredential      = "credentials.plaintext_presence"
	CheckExternalSkillRegistry    = "skills.external_registry"
	CheckSkillShadowing           = "skills.workspace_global_shadowing"
	CheckSkillAutoMutability      = "skills.automatic_mutability"
	CheckEvolutionAutoApply       = "evolution.auto_apply"
	CheckModelFallbackMissing     = "models.fallback_missing"
	CheckModelFallbackDuplicate   = "models.fallback_duplicate"
	CheckModelFallbackCycle       = "models.fallback_cycle"
	CheckAgentFallbackMissing     = "agents.fallback_missing"
	CheckContextTokenInconsistent = "tokens.context_inconsistent"
)

func runChecks(cfg *config.Config, raw rawDocuments) []Finding {
	findings := make([]Finding, 0, 16)
	findings = append(findings, checkGateway(cfg)...)
	findings = append(findings, checkChannels(cfg)...)
	findings = append(findings, checkToolRisks(cfg)...)
	findings = append(findings, checkIsolation(cfg)...)
	findings = append(findings, checkMCP(cfg)...)
	findings = append(findings, checkPlaintextCredentials(raw)...)
	findings = append(findings, checkSkills(cfg)...)
	findings = append(findings, checkEvolution(cfg)...)
	findings = append(findings, checkFallbacks(cfg)...)
	findings = append(findings, checkTokenBudgets(cfg)...)
	return findings
}

func checkGateway(cfg *config.Config) []Finding {
	host := strings.TrimSpace(cfg.Gateway.Host)
	if host == "" {
		return nil
	}
	if isWildcardHost(host) || isPublicIP(host) {
		return []Finding{newFinding(
			CheckGatewayPublicExposure,
			SeverityFail,
			"Gateway listens on a wildcard or public address",
			"Gateway binding can expose local control surfaces outside the host.",
			"Bind gateway.host to localhost/127.0.0.1 or place it behind an authenticated reverse proxy.",
			Evidence{Path: "gateway.host", Summary: "gateway host is wildcard or public"},
		)}
	}
	return nil
}

func checkChannels(cfg *config.Config) []Finding {
	var findings []Finding
	for _, name := range sortedChannelNames(cfg.Channels) {
		ch := cfg.Channels[name]
		if ch == nil || !ch.Enabled {
			continue
		}
		if len(ch.AllowFrom) == 0 || sliceHasWildcard(ch.AllowFrom) {
			findings = append(findings, newFinding(
				CheckChannelOpenAllowFrom,
				SeverityWarning,
				"Enabled remote channel allows all senders",
				"An empty or wildcard allow_from permits any sender accepted by the channel transport.",
				"Set allow_from to explicit user, chat, or account identifiers where the channel supports it.",
				Evidence{
					Path:    fmt.Sprintf("channel_list.%s.allow_from", name),
					Summary: "enabled channel has empty or wildcard allow_from",
				},
			))
		}
		if groupTriggerPermissive(ch.GroupTrigger) {
			findings = append(findings, newFinding(
				CheckChannelPermissiveTrigger,
				SeverityWarning,
				"Enabled channel has permissive group trigger",
				"Group chats that do not require mentions, prefixes, or topic restrictions increase accidental or hostile activation risk.",
				"Enable group_trigger.mention_only, configure narrow prefixes/topics, or disable group handling for this channel.",
				Evidence{
					Path:    fmt.Sprintf("channel_list.%s.group_trigger", name),
					Summary: "group trigger can respond without mention, prefix, or topic constraint",
				},
			))
		}
	}
	return findings
}

func checkToolRisks(cfg *config.Config) []Finding {
	var findings []Finding
	execWriteCapable := !strings.EqualFold(
		strings.TrimSpace(cfg.Tools.Exec.PermissionMode),
		"read_only",
	)
	if cfg.Tools.Exec.Enabled && cfg.Tools.Exec.AllowRemote && execWriteCapable {
		findings = append(findings, newFinding(
			CheckExecRemoteWrite,
			SeverityFail,
			"Remote exec is enabled with write-capable permissions",
			"Remote-origin tool calls can start local processes and may mutate the host when permission_mode is not read_only.",
			"Disable tools.exec.allow_remote or set tools.exec.permission_mode to read_only.",
			Evidence{
				Path:    "tools.exec",
				Summary: "exec enabled, allow_remote true, permission mode not read_only",
			},
		))
	}
	if cfg.Tools.WriteFile.Enabled || cfg.Tools.AppendFile.Enabled || cfg.Tools.ApplyPatch.Enabled {
		if !cfg.Agents.Defaults.RestrictToWorkspace || hasBroadPath(cfg.Tools.AllowWritePaths) {
			findings = append(findings, newFinding(
				CheckFilesystemWriteScope,
				SeverityFail,
				"Filesystem write tools have broad scope",
				"Write-capable file tools are risky when workspace restriction is disabled or broad write roots are allowed.",
				"Keep restrict_to_workspace enabled and limit tools.allow_write_paths to explicit project directories.",
				Evidence{
					Path:    "tools",
					Summary: "write-capable file tools are enabled with broad write scope",
				},
			))
		}
	}
	if cfg.Tools.InstallSkill.Enabled {
		findings = append(findings, newFinding(
			CheckInstallSkillEnabled,
			SeverityWarning,
			"Install-skill tool is enabled",
			"Skill installation can mutate local skill directories and introduce new instructions or scripts.",
			"Disable tools.install_skill unless interactive skill installation is required.",
			Evidence{Path: "tools.install_skill.enabled", Summary: "install_skill tool is enabled"},
		))
	}
	return findings
}

func checkIsolation(cfg *config.Config) []Finding {
	if !cfg.Isolation.Enabled {
		return []Finding{newFinding(
			CheckIsolationDisabled,
			SeverityWarning,
			"Process isolation is disabled",
			"Subprocess isolation is opt-in; disabled isolation leaves command execution dependent on other controls.",
			"Enable isolation where supported and keep exposed paths minimal and read-only where possible.",
			Evidence{Path: "isolation.enabled", Summary: "isolation is disabled"},
		)}
	}
	for idx, expose := range cfg.Isolation.ExposePaths {
		mode := strings.TrimSpace(expose.Mode)
		writable := strings.EqualFold(mode, "rw") || strings.EqualFold(mode, "write")
		if writable {
			return []Finding{newFinding(
				CheckIsolationDisabled,
				SeverityWarning,
				"Process isolation exposes writable host paths",
				"Writable exposed paths reduce the effectiveness of subprocess isolation.",
				"Expose only the required paths and prefer read-only modes.",
				Evidence{
					Path:    fmt.Sprintf("isolation.expose_paths[%d].mode", idx),
					Summary: "exposed path is writable",
				},
			)}
		}
	}
	return nil
}

func checkMCP(cfg *config.Config) []Finding {
	var findings []Finding
	for _, name := range sortedMCPNames(cfg.Tools.MCP.Servers) {
		server := cfg.Tools.MCP.Servers[name]
		if !server.Enabled {
			continue
		}
		transport := config.EffectiveMCPTransportType(server)
		if transport == "sse" || transport == "http" {
			findings = append(findings, newFinding(
				CheckMCPRemoteTransport,
				SeverityWarning,
				"Enabled MCP server uses remote transport",
				"Remote MCP transports expand the trusted tool surface beyond local stdio process boundaries.",
				"Prefer stdio MCP servers or restrict remote endpoints to trusted loopback services.",
				Evidence{
					Path:    fmt.Sprintf("tools.mcp.servers.%s", name),
					Summary: "enabled MCP server uses remote transport",
				},
			))
			if isHTTPURL(server.URL) {
				findings = append(findings, newFinding(
					CheckMCPInsecureTransport,
					SeverityFail,
					"Enabled MCP server uses insecure HTTP",
					"Plain HTTP MCP traffic can expose prompts, tool data, and credentials on the network path.",
					"Use HTTPS or a local stdio transport for MCP servers.",
					Evidence{
						Path:    fmt.Sprintf("tools.mcp.servers.%s.url", name),
						Summary: "MCP URL uses http scheme",
					},
				))
			}
			if !isLoopbackURL(server.URL) {
				findings = append(findings, newFinding(
					CheckMCPOverexposedTransport,
					SeverityWarning,
					"Enabled MCP server points outside loopback",
					"Non-loopback MCP endpoints depend on external network trust and server-side access controls.",
					"Use loopback endpoints, private authenticated networks, or stdio where possible.",
					Evidence{
						Path:    fmt.Sprintf("tools.mcp.servers.%s.url", name),
						Summary: "MCP endpoint is not loopback",
					},
				))
			}
		}
	}
	return findings
}

func checkPlaintextCredentials(raw rawDocuments) []Finding {
	var findings []Finding
	findings = append(findings, plaintextFromJSON(raw.ConfigJSON, "config.json")...)
	if node, err := parseSecurityYAML(raw.SecurityYAML); err == nil && node != nil {
		findings = append(findings, plaintextFromYAML(node, "security.yml")...)
	}
	return findings
}

func checkSkills(cfg *config.Config) []Finding {
	var findings []Finding
	for idx, registry := range cfg.Tools.Skills.Registries {
		if registry == nil || !registry.Enabled {
			continue
		}
		if registry.BaseURL != "" && !isLoopbackURL(registry.BaseURL) {
			findings = append(findings, newFinding(
				CheckExternalSkillRegistry,
				SeverityWarning,
				"External skill registry is enabled",
				"External registries can influence skill discovery and installation inputs.",
				"Enable only trusted registries and pin/review installed skills.",
				Evidence{
					Path:    fmt.Sprintf("tools.skills.registries[%d].base_url", idx),
					Summary: "enabled registry has non-loopback base_url",
				},
			))
		}
	}
	if cfg.Tools.FindSkills.Enabled && cfg.Tools.Skills.Enabled {
		findings = append(findings, newFinding(
			CheckSkillAutoMutability,
			SeverityInfo,
			"Skill discovery is enabled",
			"Skill discovery is read-oriented but may feed later installation workflows if install_skill is also available.",
			"Keep install_skill disabled unless installation is intentionally delegated.",
			Evidence{Path: "tools.find_skills.enabled", Summary: "find_skills is enabled"},
		))
	}
	defaultWorkspace := cleanPath(cfg.Agents.Defaults.Workspace)
	for idx, agent := range cfg.Agents.List {
		if agent.Workspace != "" && cleanPath(agent.Workspace) != defaultWorkspace {
			findings = append(findings, newFinding(
				CheckSkillShadowing,
				SeverityInfo,
				"Agent workspace can shadow global skills",
				"Workspace-local skills may override or supplement globally installed skills for that agent.",
				"Review workspace skills and keep trusted skill sources separated from untrusted workspaces.",
				Evidence{
					Path:    fmt.Sprintf("agents.list[%d].workspace", idx),
					Summary: "agent workspace differs from default workspace",
				},
			))
		}
	}
	return findings
}

func checkEvolution(cfg *config.Config) []Finding {
	if cfg.Evolution.AutoAppliesDrafts() ||
		(cfg.Evolution.Enabled && cfg.Evolution.RunsColdPathAutomatically()) {
		return []Finding{
			newFinding(
				CheckEvolutionAutoApply,
				SeverityWarning,
				"Evolution can run automatically",
				"Automatic evolution or apply mode can create or apply local changes without an explicit per-change command.",
				"Use observe/manual modes unless automatic local mutation is intended and reviewed.",
				Evidence{
					Path:    "evolution",
					Summary: "evolution apply or automatic cold-path mode is enabled",
				},
			),
		}
	}
	return nil
}

func checkFallbacks(cfg *config.Config) []Finding {
	var findings []Finding
	modelNames := map[string]struct{}{}
	graph := map[string][]string{}
	for _, model := range cfg.ModelList {
		if model == nil || strings.TrimSpace(model.ModelName) == "" {
			continue
		}
		name := strings.TrimSpace(model.ModelName)
		modelNames[name] = struct{}{}
		graph[name] = append(graph[name], model.Fallbacks...)
		seen := map[string]struct{}{}
		for _, fallback := range model.Fallbacks {
			fallback = strings.TrimSpace(fallback)
			if fallback == "" {
				continue
			}
			if _, ok := modelNames[fallback]; !ok && !modelNameExists(cfg.ModelList, fallback) {
				findings = append(findings, newFinding(
					CheckModelFallbackMissing,
					SeverityFail,
					"Model fallback references a missing model",
					"Missing fallback references can break failover when the primary model is unavailable.",
					"Add the referenced model to model_list or remove the fallback entry.",
					Evidence{
						Path:    "model_list." + name + ".fallbacks",
						Summary: "fallback model is not defined",
					},
				))
			}
			if _, duplicate := seen[fallback]; duplicate {
				findings = append(findings, newFinding(
					CheckModelFallbackDuplicate,
					SeverityWarning,
					"Model fallback list contains duplicates",
					"Duplicate fallbacks reduce deterministic failover clarity.",
					"Remove repeated fallback model names.",
					Evidence{
						Path:    "model_list." + name + ".fallbacks",
						Summary: "fallback list contains a duplicate reference",
					},
				))
			}
			seen[fallback] = struct{}{}
		}
	}
	for _, cycle := range findCycles(graph) {
		findings = append(findings, newFinding(
			CheckModelFallbackCycle,
			SeverityFail,
			"Model fallback graph contains a cycle",
			"Cyclic fallback chains can retry models indefinitely or prevent predictable failover.",
			"Remove at least one fallback edge in the cycle.",
			Evidence{
				Path:    "model_list.fallbacks",
				Summary: "fallback cycle detected: " + strings.Join(cycle, " -> "),
			},
		))
	}

	for idx, agent := range cfg.Agents.List {
		findings = append(findings, checkAgentModelRef(idx, "model", agent.Model, modelNames)...)
		if agent.Subagents != nil {
			findings = append(
				findings,
				checkAgentModelRef(idx, "subagents.model", agent.Subagents.Model, modelNames)...)
		}
	}
	return findings
}

func checkTokenBudgets(cfg *config.Config) []Finding {
	var findings []Finding
	defaults := cfg.Agents.Defaults
	if defaults.ContextWindow > 0 && defaults.MaxTokens > defaults.ContextWindow {
		findings = append(findings, newFinding(
			CheckContextTokenInconsistent,
			SeverityFail,
			"Max tokens exceed context window",
			"max_tokens greater than context_window can produce invalid model requests or unusable context budgeting.",
			"Set max_tokens below the effective context window.",
			Evidence{
				Path:    "agents.defaults.max_tokens",
				Summary: "max_tokens is greater than context_window",
			},
		))
	}
	if defaults.SummarizeTokenPercent < 0 || defaults.SummarizeTokenPercent > 100 {
		findings = append(findings, newFinding(
			CheckContextTokenInconsistent,
			SeverityWarning,
			"Summarization token percent is outside 0-100",
			"Out-of-range summarization thresholds can prevent compaction or trigger it unexpectedly.",
			"Set summarize_token_percent to a value between 1 and 100.",
			Evidence{
				Path:    "agents.defaults.summarize_token_percent",
				Summary: "summarize_token_percent is outside 0-100",
			},
		))
	}
	if defaults.ContextWindow > 0 && defaults.SummarizeTokenPercent > 0 {
		threshold := defaults.ContextWindow * defaults.SummarizeTokenPercent / 100
		if defaults.MaxTokens > 0 && threshold > 0 && defaults.MaxTokens >= threshold {
			findings = append(findings, newFinding(
				CheckContextTokenInconsistent,
				SeverityWarning,
				"Max tokens leave little room before summarization",
				"Large max_tokens relative to the summarization threshold can make compaction ineffective.",
				"Lower max_tokens or raise the context window/summarization threshold.",
				Evidence{
					Path:    "agents.defaults",
					Summary: "max_tokens is at or above summarization threshold",
				},
			))
		}
	}
	return findings
}

func newFinding(
	id string,
	severity Severity,
	title, rationale, remediation string,
	evidence ...Evidence,
) Finding {
	return Finding{
		ID:          id,
		Severity:    severity,
		Status:      statusForSeverity(severity),
		Title:       title,
		Rationale:   rationale,
		Remediation: remediation,
		Evidence:    evidence,
	}
}

func statusForSeverity(severity Severity) Status {
	if severity == SeverityWarning || severity == SeverityInfo {
		return StatusWarn
	}
	return StatusFail
}

func isWildcardHost(host string) bool {
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	return host == "0.0.0.0" || host == "::" || host == "*" || host == ""
}

func isPublicIP(host string) bool {
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsUnspecified()
}

func isHTTPURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && strings.EqualFold(parsed.Scheme, "http")
}

func isLoopbackURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Hostname() == "" {
		return false
	}
	host := parsed.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func sliceHasWildcard(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "*" {
			return true
		}
	}
	return false
}

func groupTriggerPermissive(trigger config.GroupTriggerConfig) bool {
	if trigger.Disabled || trigger.MentionOnly || len(trigger.Prefixes) > 0 ||
		len(trigger.Topics) > 0 {
		return false
	}
	return true
}

func hasBroadPath(paths []string) bool {
	for _, path := range paths {
		cleaned := cleanPath(path)
		if cleaned == "/" || cleaned == "." || cleaned == "*" ||
			strings.HasPrefix(cleaned, "/home") {
			return true
		}
	}
	return false
}

func cleanPath(path string) string {
	return strings.TrimSpace(strings.TrimRight(path, "/"))
}

func sortedChannelNames(channels config.ChannelsConfig) []string {
	names := make([]string, 0, len(channels))
	for name := range channels {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedMCPNames(servers map[string]config.MCPServerConfig) []string {
	names := make([]string, 0, len(servers))
	for name := range servers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func modelNameExists(models config.SecureModelList, name string) bool {
	for _, model := range models {
		if model != nil && strings.TrimSpace(model.ModelName) == name {
			return true
		}
	}
	return false
}

func checkAgentModelRef(
	agentIndex int,
	field string,
	model *config.AgentModelConfig,
	modelNames map[string]struct{},
) []Finding {
	if model == nil {
		return nil
	}
	var findings []Finding
	refs := append([]string{model.Primary}, model.Fallbacks...)
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		if _, ok := modelNames[ref]; !ok {
			findings = append(findings, newFinding(
				CheckAgentFallbackMissing,
				SeverityFail,
				"Agent model reference is missing",
				"Missing agent model references can prevent agent startup or subagent delegation.",
				"Add the referenced model to model_list or update the agent model reference.",
				Evidence{
					Path:    fmt.Sprintf("agents.list[%d].%s", agentIndex, field),
					Summary: "referenced model is not defined",
				},
			))
		}
	}
	return findings
}

func findCycles(graph map[string][]string) [][]string {
	var cycles [][]string
	visited := map[string]bool{}
	onStack := map[string]int{}
	var stack []string

	var visit func(string)
	visit = func(node string) {
		visited[node] = true
		onStack[node] = len(stack)
		stack = append(stack, node)
		for _, next := range graph[node] {
			next = strings.TrimSpace(next)
			if next == "" {
				continue
			}
			if idx, ok := onStack[next]; ok {
				cycle := append([]string{}, stack[idx:]...)
				cycle = append(cycle, next)
				cycles = append(cycles, cycle)
				continue
			}
			if !visited[next] {
				visit(next)
			}
		}
		stack = stack[:len(stack)-1]
		delete(onStack, node)
	}

	nodes := make([]string, 0, len(graph))
	for node := range graph {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		if !visited[node] {
			visit(node)
		}
	}
	return cycles
}

func plaintextFromJSON(data []byte, root string) []Finding {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil
	}
	var findings []Finding
	walkJSON(decoded, root, &findings)
	return findings
}

func walkJSON(value any, path string, findings *[]Finding) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			walkJSON(typed[key], path+"."+key, findings)
		}
	case []any:
		for idx, item := range typed {
			walkJSON(item, path+"["+strconv.Itoa(idx)+"]", findings)
		}
	case string:
		if isPlaintextCredentialPath(path, typed) {
			*findings = append(*findings, plaintextFinding(path))
		}
	}
}

func plaintextFromYAML(root *yaml.Node, rootName string) []Finding {
	var findings []Finding
	walkYAML(root, rootName, &findings)
	return findings
}

func walkYAML(node *yaml.Node, path string, findings *[]Finding) {
	if node == nil {
		return
	}
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		walkYAML(node.Content[0], path, findings)
		return
	}
	switch node.Kind {
	case yaml.MappingNode:
		for idx := 0; idx+1 < len(node.Content); idx += 2 {
			key := node.Content[idx].Value
			walkYAML(node.Content[idx+1], path+"."+key, findings)
		}
	case yaml.SequenceNode:
		for idx, item := range node.Content {
			walkYAML(item, path+"["+strconv.Itoa(idx)+"]", findings)
		}
	case yaml.ScalarNode:
		if isPlaintextCredentialPath(path, node.Value) {
			*findings = append(*findings, plaintextFinding(path))
		}
	}
}

func isPlaintextCredentialPath(path, value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || value == configPlaceholder() {
		return false
	}
	lowerValue := strings.ToLower(value)
	if strings.HasPrefix(lowerValue, "enc://") ||
		strings.HasPrefix(lowerValue, "file://") ||
		strings.HasPrefix(lowerValue, "env://") ||
		strings.HasPrefix(lowerValue, "${") {
		return false
	}
	path = strings.ToLower(path)
	sensitiveKeys := []string{
		"api_key", "api_keys", "token", "auth_token", "access_token", "bot_token", "app_token",
		"secret", "app_secret", "client_secret", "password", "passphrase", "verification_token",
		"encrypt_key", "sasl_password", "nickserv_password",
	}
	for _, key := range sensitiveKeys {
		if strings.HasSuffix(path, "."+key) || strings.Contains(path, "."+key+"[") {
			return true
		}
	}
	return false
}

func configPlaceholder() string {
	return "[NOT_HERE]"
}

func plaintextFinding(path string) Finding {
	return newFinding(
		CheckPlaintextCredential,
		SeverityFail,
		"Plaintext credential is present in config documents",
		"Plaintext secrets in config or security documents can leak through backups, logs, sync, or accidental commits.",
		"Move the secret to encrypted security storage or a file/env reference; rotate if exposure is possible.",
		Evidence{
			Path:    path,
			Summary: "plaintext credential-like value is present; value intentionally omitted",
		},
	)
}
