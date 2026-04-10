package wal

import "fmt"

// Fanout distributes a single WAL message to multiple named output writers.
// Each writer receives every message; errors are collected and returned together.
type Fanout struct {
	writers map[string]Writer
}

// Writer is anything that can accept a formatted WAL message.
type Writer interface {
	Write(msg *Message) error
}

// NewFanout creates a Fanout with the supplied named writers.
func NewFanout(writers map[string]Writer) (*Fanout, error) {
	if len(writers) == 0 {
		return nil, fmt.Errorf("fanout: at least one writer is required")
	}
	return &Fanout{writers: writers}, nil
}

// Dispatch sends msg to every registered writer.
// All writers are attempted even if one fails; a combined error is returned
// when one or more writers report a failure.
func (f *Fanout) Dispatch(msg *Message) error {
	var errs []error
	for name, w := range f.writers {
		if err := w.Write(msg); err != nil {
			errs = append(errs, fmt.Errorf("fanout[%s]: %w", name, err))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return joinErrors(errs)
}

// Len returns the number of registered writers.
func (f *Fanout) Len() int { return len(f.writers) }

// joinErrors combines multiple errors into a single descriptive error.
func joinErrors(errs []error) error {
	msg := errs[0].Error()
	for _, e := range errs[1:] {
		msg += "; " + e.Error()
	}
	return fmt.Errorf("%s", msg)
}
