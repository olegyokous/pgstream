package wal

import (
	"errors"
	"fmt"
)

// Validator checks WAL messages against a set of rules and returns a
// validation error if any rule is violated. Messages that pass all rules
// are forwarded unchanged.
type Validator struct {
	rules []ValidationRule
}

// ValidationRule defines a single named predicate applied to a message.
type ValidationRule struct {
	Name      string
	Predicate func(*Message) error
}

// ErrValidation is returned when a message fails one or more rules.
type ErrValidation struct {
	Rule    string
	Message string
}

func (e *ErrValidation) Error() string {
	return fmt.Sprintf("validation rule %q failed: %s", e.Rule, e.Message)
}

// RequireTable returns a ValidationRule that rejects messages whose table
// name is empty.
func RequireTable() ValidationRule {
	return ValidationRule{
		Name: "require_table",
		Predicate: func(m *Message) error {
			if m.Table == "" {
				return errors.New("table name must not be empty")
			}
			return nil
		},
	}
}

// RequireAction returns a ValidationRule that rejects messages whose action
// is not one of the provided set.
func RequireAction(allowed ...string) ValidationRule {
	set := make(map[string]struct{}, len(allowed))
	for _, a := range allowed {
		set[a] = struct{}{}
	}
	return ValidationRule{
		Name: "require_action",
		Predicate: func(m *Message) error {
			if _, ok := set[m.Action]; !ok {
				return fmt.Errorf("action %q is not in allowed set %v", m.Action, allowed)
			}
			return nil
		},
	}
}

// NewValidator constructs a Validator from the supplied rules. At least one
// rule is required.
func NewValidator(rules ...ValidationRule) (*Validator, error) {
	if len(rules) == 0 {
		return nil, errors.New("validator: at least one rule is required")
	}
	return &Validator{rules: rules}, nil
}

// Validate applies all rules to m. The first failing rule causes an
// *ErrValidation to be returned. If m is nil the call is a no-op.
func (v *Validator) Validate(m *Message) error {
	if m == nil {
		return nil
	}
	for _, r := range v.rules {
		if err := r.Predicate(m); err != nil {
			return &ErrValidation{Rule: r.Name, Message: err.Error()}
		}
	}
	return nil
}
