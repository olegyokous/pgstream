package wal

import "strings"

// Tag represents a key-value label attached to a WAL message.
type Tag struct {
	Key   string
	Value string
}

// Tagger enriches WAL messages with static or dynamic tags.
type Tagger struct {
	static []Tag
	rules  []tagRule
}

type tagRule struct {
	table  string // empty means match all
	action string // empty means match all
	tag    Tag
}

// NewTagger constructs a Tagger with the given static tags applied to every
// message. Use AddRule to attach tags conditionally.
func NewTagger(static ...Tag) *Tagger {
	return &Tagger{static: static}
}

// AddRule registers a tag that is applied only when the message matches the
// given table and action. Empty string is treated as a wildcard.
func (t *Tagger) AddRule(table, action string, tag Tag) {
	t.rules = append(t.rules, tagRule{
		table:  strings.ToLower(table),
		action: strings.ToLower(action),
		tag:    tag,
	})
}

// Apply attaches matching tags to msg.Meta and returns the enriched message.
// If msg is nil it is returned unchanged.
func (t *Tagger) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	if msg.Meta == nil {
		msg.Meta = map[string]string{}
	}
	for _, tag := range t.static {
		msg.Meta[tag.Key] = tag.Value
	}
	tbl := strings.ToLower(msg.Table)
	act := strings.ToLower(msg.Action)
	for _, r := range t.rules {
		tableMatch := r.table == "" || r.table == tbl
		actionMatch := r.action == "" || r.action == act
		if tableMatch && actionMatch {
			msg.Meta[r.tag.Key] = r.tag.Value
		}
	}
	return msg
}
