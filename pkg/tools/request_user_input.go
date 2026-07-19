package tools

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

const (
	defaultRequestUserInputTimeout = time.Hour
	maximumRequestUserInputTimeout = 24 * time.Hour
)

type RequestUserInputToolOptions struct {
	DefaultTimeout time.Duration
	MaxTimeout     time.Duration
}

type RequestUserInputTool struct {
	defaultTimeout time.Duration
	maxTimeout     time.Duration
}

func NewRequestUserInputTool(options RequestUserInputToolOptions) (*RequestUserInputTool, error) {
	defaultTimeout := options.DefaultTimeout
	if defaultTimeout == 0 {
		defaultTimeout = defaultRequestUserInputTimeout
	}
	maxTimeout := options.MaxTimeout
	if maxTimeout == 0 {
		maxTimeout = maximumRequestUserInputTimeout
	}
	if maxTimeout < time.Minute || maxTimeout > maximumRequestUserInputTimeout {
		return nil, fmt.Errorf(
			"request_user_input max timeout must be between 1 minute and 24 hours",
		)
	}
	if defaultTimeout < time.Minute || defaultTimeout > maxTimeout {
		return nil, fmt.Errorf(
			"request_user_input default timeout must be between 1 minute and max timeout",
		)
	}
	return &RequestUserInputTool{
		defaultTimeout: defaultTimeout,
		maxTimeout:     maxTimeout,
	}, nil
}

func (t *RequestUserInputTool) Name() string { return "request_user_input" }

func (t *RequestUserInputTool) Description() string {
	return "Pause the current task and ask the user one to three short questions when their input is required to continue safely or choose between meaningful alternatives. Do not use this for optional confirmation or information that can be discovered with available tools."
}

func (t *RequestUserInputTool) Parameters() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"questions": map[string]any{
				"type":     "array",
				"minItems": 1,
				"maxItems": interactions.MaxQuestions,
				"items": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"properties": map[string]any{
						"id": map[string]any{
							"type":        "string",
							"description": "Stable snake_case identifier for this answer.",
						},
						"header": map[string]any{
							"type":        "string",
							"description": "Optional short label of at most 12 characters.",
						},
						"question": map[string]any{
							"type":        "string",
							"description": "A concise question whose answer materially unblocks the task.",
						},
						"options": map[string]any{
							"type":     "array",
							"minItems": 2,
							"maxItems": interactions.MaxOptions,
							"items": map[string]any{
								"type":                 "object",
								"additionalProperties": false,
								"properties": map[string]any{
									"label": map[string]any{
										"type":        "string",
										"description": "Short user-facing choice label.",
									},
									"description": map[string]any{
										"type":        "string",
										"description": "One sentence describing the choice's impact or tradeoff.",
									},
								},
								"required": []string{"label", "description"},
							},
						},
					},
					"required": []string{"id", "question"},
				},
			},
			"timeout_seconds": map[string]any{
				"type":        "integer",
				"minimum":     int(time.Minute / time.Second),
				"maximum":     int(t.maxTimeout / time.Second),
				"description": "Optional wait time before the question expires.",
			},
		},
		"required": []string{"questions"},
	}
}

func (t *RequestUserInputTool) Execute(_ context.Context, args map[string]any) *ToolResult {
	questions, err := parseInteractionQuestions(args["questions"])
	if err != nil {
		return ErrorResult(err.Error())
	}
	timeout, err := t.parseTimeout(args["timeout_seconds"])
	if err != nil {
		return ErrorResult(err.Error())
	}
	request := interactions.SuspensionRequest{
		Kind:      interactions.KindQuestion,
		Questions: questions,
		Timeout:   timeout,
	}
	if err := interactions.ValidateSuspensionRequest(request); err != nil {
		return ErrorResult(err.Error())
	}
	return &ToolResult{Silent: true, Suspension: &request}
}

func (*RequestUserInputTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsMutating
}

func (*RequestUserInputTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}

func (t *RequestUserInputTool) parseTimeout(raw any) (time.Duration, error) {
	if raw == nil {
		return t.defaultTimeout, nil
	}
	seconds, ok := numericInteger(raw)
	if !ok {
		return 0, fmt.Errorf("timeout_seconds must be an integer")
	}
	minimumSeconds := int64(time.Minute / time.Second)
	maximumSeconds := int64(t.maxTimeout / time.Second)
	if seconds < minimumSeconds || seconds > maximumSeconds {
		return 0, fmt.Errorf(
			"timeout_seconds must be between %d and %d",
			minimumSeconds,
			maximumSeconds,
		)
	}
	return time.Duration(seconds) * time.Second, nil
}

func parseInteractionQuestions(raw any) ([]interactions.Question, error) {
	items, ok := raw.([]any)
	if !ok || len(items) == 0 {
		return nil, fmt.Errorf("questions must contain 1 to %d entries", interactions.MaxQuestions)
	}
	questions := make([]interactions.Question, 0, len(items))
	for index, rawQuestion := range items {
		questionArgs, ok := rawQuestion.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("questions[%d] must be an object", index)
		}
		id, err := requiredStringArg(questionArgs, "id", fmt.Sprintf("questions[%d].id", index))
		if err != nil {
			return nil, err
		}
		text, err := requiredStringArg(
			questionArgs,
			"question",
			fmt.Sprintf("questions[%d].question", index),
		)
		if err != nil {
			return nil, err
		}
		header, err := optionalStringArg(questionArgs, "header")
		if err != nil {
			return nil, fmt.Errorf("questions[%d].%w", index, err)
		}
		options, err := parseInteractionOptions(questionArgs["options"], index)
		if err != nil {
			return nil, err
		}
		questions = append(questions, interactions.Question{
			ID: id, Header: header, Question: text, Options: options,
		})
	}
	return questions, nil
}

func parseInteractionOptions(raw any, questionIndex int) ([]interactions.Option, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok || (len(items) != 0 && len(items) < 2) {
		return nil, fmt.Errorf(
			"questions[%d].options must be empty or contain 2 to %d entries",
			questionIndex,
			interactions.MaxOptions,
		)
	}
	options := make([]interactions.Option, 0, len(items))
	for optionIndex, rawOption := range items {
		optionArgs, ok := rawOption.(map[string]any)
		if !ok {
			return nil, fmt.Errorf(
				"questions[%d].options[%d] must be an object",
				questionIndex,
				optionIndex,
			)
		}
		label, err := requiredStringArg(
			optionArgs,
			"label",
			fmt.Sprintf("questions[%d].options[%d].label", questionIndex, optionIndex),
		)
		if err != nil {
			return nil, err
		}
		description, err := requiredStringArg(
			optionArgs,
			"description",
			fmt.Sprintf("questions[%d].options[%d].description", questionIndex, optionIndex),
		)
		if err != nil {
			return nil, err
		}
		options = append(options, interactions.Option{Label: label, Description: description})
	}
	return options, nil
}

func numericInteger(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int64:
		return number, true
	case float64:
		invalid := math.IsNaN(number) ||
			math.IsInf(number, 0) ||
			math.Trunc(number) != number ||
			number > math.MaxInt64 ||
			number < math.MinInt64
		if invalid {
			return 0, false
		}
		return int64(number), true
	default:
		return 0, false
	}
}
