package wal

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// DigestAlgorithm selects the hashing algorithm used by the Digester.
type DigestAlgorithm string

const (
	DigestMD5    DigestAlgorithm = "md5"
	DigestSHA256 DigestAlgorithm = "sha256"
)

// DigestConfig controls which columns are included in the digest and which
// algorithm is used.
type DigestConfig struct {
	// Algorithm is the hashing algorithm. Defaults to sha256.
	Algorithm DigestAlgorithm
	// Columns restricts the digest to the named columns. Empty means all columns.
	Columns []string
	// Field is the meta key under which the digest is stored.
	Field string
}

// Digester computes a deterministic hash over a message's column values and
// stores it in the message metadata.
type Digester struct {
	cfg DigestConfig
}

// NewDigester returns a Digester or an error if the configuration is invalid.
func NewDigester(cfg DigestConfig) (*Digester, error) {
	if cfg.Algorithm == "" {
		cfg.Algorithm = DigestSHA256
	}
	if cfg.Algorithm != DigestMD5 && cfg.Algorithm != DigestSHA256 {
		return nil, fmt.Errorf("digest: unknown algorithm %q", cfg.Algorithm)
	}
	if cfg.Field == "" {
		cfg.Field = "_digest"
	}
	return &Digester{cfg: cfg}, nil
}

// Apply computes the digest for msg and stores it in msg.Meta[Field].
// A nil message is returned unchanged.
func (d *Digester) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}

	keys := make([]string, 0, len(msg.Columns))
	want := columnSet(d.cfg.Columns)
	for k := range msg.Columns {
		if len(want) == 0 || want[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "%s=%v;", k, msg.Columns[k])
	}

	var digest string
	switch d.cfg.Algorithm {
	case DigestMD5:
		sum := md5.Sum([]byte(sb.String())) //nolint:gosec
		digest = hex.EncodeToString(sum[:])
	default:
		sum := sha256.Sum256([]byte(sb.String()))
		digest = hex.EncodeToString(sum[:])
	}

	if msg.Meta == nil {
		msg.Meta = make(map[string]string)
	}
	msg.Meta[d.cfg.Field] = digest
	return msg
}

func columnSet(cols []string) map[string]bool {
	if len(cols) == 0 {
		return nil
	}
	m := make(map[string]bool, len(cols))
	for _, c := range cols {
		m[c] = true
	}
	return m
}
