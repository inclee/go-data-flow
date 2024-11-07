package handler

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"go-data-flow/pkg/stream"
	"go-data-flow/pkg/util/jsonpath"
)

type ComparisonOperator string

const (
	Equal              ComparisonOperator = "=="
	NotEqual           ComparisonOperator = "!="
	GreaterThan        ComparisonOperator = ">"
	LessThan           ComparisonOperator = "<"
	GreaterThanOrEqual ComparisonOperator = ">="
	LessThanOrEqual    ComparisonOperator = "<="
)

type MatchCondition struct {
	Field    string             `yaml:"filed"`
	Operator ComparisonOperator `yaml:"operator"`
	Value    interface{}        `yaml:"vlaue"`
}

func ParseCondition(conditionStr string) (*MatchCondition, error) {
	re := regexp.MustCompile(`\s*([^\s]+)\s*(==|!=|>=|<=|>|<)\s*(.+)\s*`)
	matches := re.FindStringSubmatch(conditionStr)
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid condition format: %s", conditionStr)
	}

	field := matches[1]
	operator := ComparisonOperator(matches[2])
	valueStr := matches[3]

	value, err := parseValue(valueStr)
	if err != nil {
		return nil, fmt.Errorf("invalid value in condition: %s", valueStr)
	}

	return &MatchCondition{
		Field:    field,
		Operator: operator,
		Value:    value,
	}, nil
}

func parseValue(valueStr string) (interface{}, error) {
	if i, err := strconv.Atoi(valueStr); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return f, nil
	}
	return strings.Trim(valueStr, `"'`), nil
}

type Matcher interface {
	Match(context.Context, *stream.Event) *stream.Event
	MatchIngrex(ctx context.Context, event *stream.Event) (bool, bool)
	MatchData(ctx context.Context, data map[string]interface{}) bool
}

type MatchConfig struct {
	Keys  []string `yaml:"keys"`
	Conds []string `yaml:"conds"`
}

func (f MatchConfig) IsZero() bool {
	return len(f.Keys) == 0 && len(f.Conds) == 0
}

func GetMatchConfig(val reflect.Value) MatchConfig {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		structField := val.Type().Field(i)
		if structField.Anonymous {
			if matcher := GetMatchConfig(field); !matcher.IsZero() {
				return matcher
			}

		} else if field.CanInterface() {
			if matcher, ok := field.Interface().(MatchConfig); ok {
				return matcher
			}
		}
	}
	return MatchConfig{}
}

type DefaultMatcher struct {
	inRegexs []*regexp.Regexp
	Conds    []*MatchCondition
}

func NewDefaultMatcher(config MatchConfig) (*DefaultMatcher, error) {
	in := []*regexp.Regexp{}
	for _, reg := range config.Keys {
		in = append(in, regexp.MustCompile(reg))
	}
	conds := []*MatchCondition{}
	for _, condExpr := range config.Conds {
		cond, err := ParseCondition(condExpr)
		if err != nil {
			return nil, err
		}
		conds = append(conds, cond)
	}
	return &DefaultMatcher{
		inRegexs: in,
		Conds:    conds,
	}, nil
}

func (f *DefaultMatcher) Match(ctx context.Context, event *stream.Event) (out *stream.Event) {
	_, matched := f.MatchIngrex(ctx, event)
	out = &stream.Event{}
	out.Context = event.Context
	out.Topic = event.Topic
	out.Datas = []map[string]interface{}{}
	if len(f.Conds) == 0 {
		if matched {
			return event
		} else {
			return out
		}
	}

	for _, data := range event.Datas {
		if f.MatchData(ctx, data) {
			out.Datas = append(out.Datas, data)
		}
	}
	return out
}

func (f *DefaultMatcher) MatchIngrex(ctx context.Context, event *stream.Event) (bool, bool) {
	matched := true
	if len(f.inRegexs) > 0 {
		find := false
		for _, regex := range f.inRegexs {
			if regex.MatchString(event.Topic) {
				find = true
				break
			}
		}
		matched = find
	}
	return len(f.inRegexs) > 0, matched
}
func (f *DefaultMatcher) MatchData(ctx context.Context, data map[string]interface{}) bool {
	for _, condition := range f.Conds {
		if !evaluateCondition(jsonpath.Get(data, condition.Field), condition.Operator, condition.Value) {
			return false
		}
	}
	return true
}

func evaluateCondition(actual interface{}, operator ComparisonOperator, target interface{}) bool {
	actualValue := fmt.Sprintf("%v", actual)
	targetValue := fmt.Sprintf("%v", target)

	switch operator {
	case Equal:
		return actualValue == targetValue
	case NotEqual:
		return actualValue != targetValue
	case GreaterThan:
		return actualValue > targetValue
	case LessThan:
		return actualValue < targetValue
	case GreaterThanOrEqual:
		return actualValue >= targetValue
	case LessThanOrEqual:
		return actualValue <= targetValue
	default:
		return false
	}
}
