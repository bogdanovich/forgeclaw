package loopguard

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

type Action string

const (
	ActionAllow Action = "allow"
	ActionWarn  Action = "warn"
	ActionBlock Action = "block"
	ActionHalt  Action = "halt"
)

type Semantics string

const (
	SemanticsUnknown            Semantics = "unknown"
	SemanticsReadOnlyIdempotent Semantics = "read_only_idempotent"
	SemanticsMutating           Semantics = "mutating"
)

type Config struct {
	Enabled             bool
	WarningsEnabled     bool
	HardStopsEnabled    bool
	ExactFailureWarn    int
	ExactFailureBlock   int
	SameToolFailureWarn int
	SameToolFailureHalt int
	NoProgressWarn      int
	NoProgressBlock     int
	MaxSignatures       int
}

func DefaultConfig() Config {
	return Config{
		Enabled:             true,
		WarningsEnabled:     true,
		HardStopsEnabled:    false,
		ExactFailureWarn:    2,
		ExactFailureBlock:   5,
		SameToolFailureWarn: 3,
		SameToolFailureHalt: 8,
		NoProgressWarn:      2,
		NoProgressBlock:     5,
		MaxSignatures:       64,
	}
}

func (c Config) Normalized() Config {
	defaults := DefaultConfig()
	if c.ExactFailureWarn <= 0 {
		c.ExactFailureWarn = defaults.ExactFailureWarn
	}
	if c.ExactFailureBlock < c.ExactFailureWarn {
		c.ExactFailureBlock = max(defaults.ExactFailureBlock, c.ExactFailureWarn)
	}
	if c.SameToolFailureWarn <= 0 {
		c.SameToolFailureWarn = defaults.SameToolFailureWarn
	}
	if c.SameToolFailureHalt < c.SameToolFailureWarn {
		c.SameToolFailureHalt = max(defaults.SameToolFailureHalt, c.SameToolFailureWarn)
	}
	if c.NoProgressWarn <= 0 {
		c.NoProgressWarn = defaults.NoProgressWarn
	}
	if c.NoProgressBlock < c.NoProgressWarn {
		c.NoProgressBlock = max(defaults.NoProgressBlock, c.NoProgressWarn)
	}
	if c.MaxSignatures <= 0 {
		c.MaxSignatures = defaults.MaxSignatures
	}
	return c
}

type Decision struct {
	Action    Action
	Code      string
	Tool      string
	ArgsHash  string
	Count     int
	Threshold int
	Message   string
}

func (d Decision) AllowsExecution() bool {
	return d.Action == ActionAllow || d.Action == ActionWarn
}

type Observation struct {
	Tool       string
	Args       map[string]any
	ResultText string
	Failed     bool
	Semantics  Semantics
}

type progressRecord struct {
	resultHash string
	count      int
}

type Controller struct {
	config Config

	exactFailures map[string]int
	noProgress    map[string]progressRecord
	tracked       map[string]struct{}
	order         []string

	failureStreakTool  string
	failureStreakCount int
}

func New(config Config) *Controller {
	return &Controller{
		config:        config.Normalized(),
		exactFailures: make(map[string]int),
		noProgress:    make(map[string]progressRecord),
		tracked:       make(map[string]struct{}),
	}
}

func (c *Controller) Before(tool string, args map[string]any, semantics Semantics) Decision {
	tool = strings.TrimSpace(tool)
	sig := callSignature(tool, args)
	decision := allowDecision(tool, sig.argsHash)
	if c == nil || !c.config.Enabled {
		return decision
	}
	if c.config.HardStopsEnabled {
		if count := c.exactFailures[sig.key]; count >= c.config.ExactFailureBlock {
			return decisionFor(
				ActionBlock,
				"repeated_exact_failure_block",
				tool,
				sig.argsHash,
				count,
				c.config.ExactFailureBlock,
				fmt.Sprintf(
					"Blocked %s after %d identical failed calls. Change arguments or strategy.",
					toolLabel(tool),
					count,
				),
			)
		}
		if tool != "" && c.failureStreakTool == tool && c.failureStreakCount >= c.config.SameToolFailureHalt {
			return decisionFor(
				ActionBlock,
				"same_tool_failure_block",
				tool,
				sig.argsHash,
				c.failureStreakCount,
				c.config.SameToolFailureHalt,
				fmt.Sprintf(
					"Blocked %s after %d consecutive failures. Use a different tool path or report the blocker.",
					toolLabel(tool),
					c.failureStreakCount,
				),
			)
		}
		if semantics == SemanticsReadOnlyIdempotent {
			if record, ok := c.noProgress[sig.key]; ok && record.count >= c.config.NoProgressBlock {
				return decisionFor(
					ActionBlock,
					"read_only_no_progress_block",
					tool,
					sig.argsHash,
					record.count,
					c.config.NoProgressBlock,
					fmt.Sprintf(
						"Blocked %s after %d identical read-only results. Reuse the result or change the query.",
						toolLabel(tool),
						record.count,
					),
				)
			}
		}
	}
	return decision
}

func (c *Controller) After(observation Observation) Decision {
	tool := strings.TrimSpace(observation.Tool)
	sig := callSignature(tool, observation.Args)
	decision := allowDecision(tool, sig.argsHash)
	if c == nil || !c.config.Enabled {
		return decision
	}
	c.remember(sig.key)

	if observation.Failed {
		delete(c.noProgress, sig.key)
		count := c.exactFailures[sig.key] + 1
		c.exactFailures[sig.key] = count
		if c.failureStreakTool == tool && tool != "" {
			c.failureStreakCount++
		} else {
			c.failureStreakTool = tool
			c.failureStreakCount = 1
		}

		if c.config.HardStopsEnabled && c.failureStreakCount >= c.config.SameToolFailureHalt {
			return decisionFor(
				ActionHalt,
				"same_tool_failure_halt",
				tool,
				sig.argsHash,
				c.failureStreakCount,
				c.config.SameToolFailureHalt,
				fmt.Sprintf(
					"Stopped %s after %d consecutive failures. Change tool path before continuing.",
					toolLabel(tool),
					c.failureStreakCount,
				),
			)
		}
		if c.config.WarningsEnabled && count >= c.config.ExactFailureWarn {
			return decisionFor(
				ActionWarn,
				"repeated_exact_failure_warning",
				tool,
				sig.argsHash,
				count,
				c.config.ExactFailureWarn,
				fmt.Sprintf(
					"%s failed %d times with identical arguments. Inspect the error and change strategy.",
					toolLabel(tool),
					count,
				),
			)
		}
		if c.config.WarningsEnabled && c.failureStreakCount >= c.config.SameToolFailureWarn {
			return decisionFor(
				ActionWarn,
				"same_tool_failure_warning",
				tool,
				sig.argsHash,
				c.failureStreakCount,
				c.config.SameToolFailureWarn,
				fmt.Sprintf(
					"%s failed %d consecutive times. Diagnose the failure or use another tool path.",
					toolLabel(tool),
					c.failureStreakCount,
				),
			)
		}
		return decision
	}

	delete(c.exactFailures, sig.key)
	c.failureStreakTool = ""
	c.failureStreakCount = 0
	if observation.Semantics != SemanticsReadOnlyIdempotent {
		delete(c.noProgress, sig.key)
		return decision
	}

	resultHash := HashText(observation.ResultText)
	record := c.noProgress[sig.key]
	if record.resultHash == resultHash && record.resultHash != "" {
		record.count++
	} else {
		record = progressRecord{resultHash: resultHash, count: 1}
	}
	c.noProgress[sig.key] = record
	if c.config.WarningsEnabled && record.count >= c.config.NoProgressWarn {
		return decisionFor(
			ActionWarn,
			"read_only_no_progress_warning",
			tool,
			sig.argsHash,
			record.count,
			c.config.NoProgressWarn,
			fmt.Sprintf(
				"%s returned the same read-only result %d times. Reuse it or change the query.",
				toolLabel(tool),
				record.count,
			),
		)
	}
	return decision
}

func HashArguments(args map[string]any) string {
	data, err := json.Marshal(args)
	if err != nil {
		data = []byte(`{"canonicalization":"unsupported"}`)
	}
	return hashBytes(data)
}

func HashText(text string) string {
	return hashBytes([]byte(text))
}

type signature struct {
	key      string
	argsHash string
}

func callSignature(tool string, args map[string]any) signature {
	argsHash := HashArguments(args)
	return signature{key: tool + "\x00" + argsHash, argsHash: argsHash}
}

func (c *Controller) remember(key string) {
	if _, ok := c.tracked[key]; ok {
		return
	}
	c.tracked[key] = struct{}{}
	c.order = append(c.order, key)
	for len(c.order) > c.config.MaxSignatures {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.tracked, oldest)
		delete(c.exactFailures, oldest)
		delete(c.noProgress, oldest)
	}
}

func allowDecision(tool, argsHash string) Decision {
	return Decision{Action: ActionAllow, Code: "allow", Tool: tool, ArgsHash: argsHash}
}

func decisionFor(action Action, code, tool, argsHash string, count, threshold int, message string) Decision {
	return Decision{
		Action: action, Code: code, Tool: tool, ArgsHash: argsHash,
		Count: count, Threshold: threshold, Message: message,
	}
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func toolLabel(tool string) string {
	if tool == "" {
		return "the tool"
	}
	return tool
}
