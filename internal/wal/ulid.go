package wal

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"
)

// counter provides a monotonic sequence component for ULID-like IDs.
var counter atomic.Uint64

// newULID returns a time-ordered, unique string identifier.
// Format: <unix-ms-hex>-<random-hex> — lightweight, no external deps.
func newULID() string {
	ms := uint64(time.Now().UnixMilli())
	seq := counter.Add(1)

	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	rndVal := binary.BigEndian.Uint32(rnd[:])

	return fmt.Sprintf("%013x-%04x-%08x", ms, seq&0xFFFF, rndVal)
}
