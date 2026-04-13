package wal

import "errors"

// ProjectConfig controls which columns are retained or excluded.
type ProjectConfig struct {
	// Columns is the explicit list of column names to keep.
	// If non-empty, all other columns are dropped.
	Columns []string
	// Exclude lists column names to drop; used when Columns is empty.
	Exclude []string
	// Table, if non-empty, scopes projection to this table only.
	Table string
}

// Projector retains or drops columns from WAL messages.
type Projector struct {
	cfg    ProjectConfig
	keep   map[string]struct{}
	exclude map[string]struct{}
}

// NewProjector constructs a Projector from cfg.
// Returns an error when both Columns and Exclude are non-empty.
func NewProjector(cfg ProjectConfig) (*Projector, error) {
	if len(cfg.Columns) > 0 && len(cfg.Exclude) > 0 {
		return nil, errors.New("projector: Columns and Exclude are mutually exclusive")
	}
	p := &Projector{
		cfg:     cfg,
		keep:    make(map[string]struct{}, len(cfg.Columns)),
		exclude: make(map[string]struct{}, len(cfg.Exclude)),
	}
	for _, c := range cfg.Columns {
		p.keep[c] = struct{}{}
	}
	for _, c := range cfg.Exclude {
		p.exclude[c] = struct{}{}
	}
	return p, nil
}

// Apply returns msg with columns filtered according to the projection rules.
// Returns nil when msg is nil.
func (p *Projector) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	if p.cfg.Table != "" && msg.Table != p.cfg.Table {
		return msg
	}
	filtered := make([]Column, 0, len(msg.Columns))
	for _, col := range msg.Columns {
		if p.shouldKeep(col.Name) {
			filtered = append(filtered, col)
		}
	}
	out := *msg
	out.Columns = filtered
	return &out
}

func (p *Projector) shouldKeep(name string) bool {
	if len(p.keep) > 0 {
		_, ok := p.keep[name]
		return ok
	}
	if len(p.exclude) > 0 {
		_, drop := p.exclude[name]
		return !drop
	}
	return true
}
