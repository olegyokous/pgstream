package wal

// Filter holds criteria for including or excluding WAL messages.
type Filter struct {
	// Tables is a set of "schema.table" strings to include. Empty means all.
	Tables map[string]struct{}
	// Actions is a set of ActionTypes to include. Empty means all.
	Actions map[ActionType]struct{}
}

// NewFilter creates a Filter from optional table and action lists.
func NewFilter(tables []string, actions []ActionType) *Filter {
	f := &Filter{
		Tables:  make(map[string]struct{}),
		Actions: make(map[ActionType]struct{}),
	}
	for _, t := range tables {
		f.Tables[t] = struct{}{}
	}
	for _, a := range actions {
		f.Actions[a] = struct{}{}
	}
	return f
}

// Match returns true if the message passes the filter criteria.
func (f *Filter) Match(msg *Message) bool {
	if msg == nil {
		return false
	}
	if len(f.Tables) > 0 {
		key := msg.Schema + "." + msg.Table
		if _, ok := f.Tables[key]; !ok {
			return false
		}
	}
	if len(f.Actions) > 0 {
		if _, ok := f.Actions[msg.Action]; !ok {
			return false
		}
	}
	return true
}
