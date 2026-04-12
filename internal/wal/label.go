package wal

import "strings"

// Labeler attaches a fixed set of key/value labels to every message's Meta
// map. Labels are merged with any existing meta entries; existing keys are
// NOT overwritten unless OverwriteExisting is set.
type Labeler struct {
	labels    map[string]string
	overwrite bool
}

// LabelOption configures a Labeler.
type LabelOption func(*Labeler)

// WithLabelOverwrite allows the labeler to overwrite existing meta keys.
func WithLabelOverwrite() LabelOption {
	return func(l *Labeler) { l.overwrite = true }
}

// NewLabeler creates a Labeler that stamps each message with the supplied
// key/value pairs. Keys are normalised to lower-case.
func NewLabeler(labels map[string]string, opts ...LabelOption) (*Labeler, error) {
	if len(labels) == 0 {
		return nil, errorf("labeler: at least one label is required")
	}
	norm := make(map[string]string, len(labels))
	for k, v := range labels {
		norm[strings.ToLower(k)] = v
	}
	l := &Labeler{labels: norm}
	for _, o := range opts {
		o(l)
	}
	return l, nil
}

// Apply stamps msg.Meta with the configured labels and returns the message
// unchanged in all other respects. A nil message is passed through.
func (l *Labeler) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	if msg.Meta == nil {
		msg.Meta = make(map[string]string, len(l.labels))
	}
	for k, v := range l.labels {
		if _, exists := msg.Meta[k]; exists && !l.overwrite {
			continue
		}
		msg.Meta[k] = v
	}
	return msg
}
