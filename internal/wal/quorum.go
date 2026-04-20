package wal

import (
	"errors"
	"fmt"
	"sync"
)

// QuorumPolicy controls how many acknowledgements are required.
type QuorumPolicy int

const (
	// QuorumAll requires every voter to acknowledge.
	QuorumAll QuorumPolicy = iota
	// QuorumMajority requires more than half of voters to acknowledge.
	QuorumMajority
	// QuorumAny requires at least one voter to acknowledge.
	QuorumAny
)

// Voter is a function that casts a vote for a given message.
type Voter func(msg *Message) (bool, error)

// Quorum collects votes from a set of Voters and decides whether a
// Message is accepted based on the configured QuorumPolicy.
type Quorum struct {
	mu     sync.Mutex
	voters []Voter
	policy QuorumPolicy
}

// NewQuorum creates a Quorum with the given policy and at least one voter.
func NewQuorum(policy QuorumPolicy, voters ...Voter) (*Quorum, error) {
	if len(voters) == 0 {
		return nil, errors.New("quorum: at least one voter is required")
	}
	return &Quorum{voters: voters, policy: policy}, nil
}

// Decide applies all voters to msg and returns true when the configured
// policy threshold is met. The first voter error is returned immediately.
func (q *Quorum) Decide(msg *Message) (bool, error) {
	q.mu.Lock()
	voters := q.voters
	policy := q.policy
	q.mu.Unlock()

	total := len(voters)
	ayes := 0

	for _, v := range voters {
		ok, err := v(msg)
		if err != nil {
			return false, fmt.Errorf("quorum: voter error: %w", err)
		}
		if ok {
			ayes++
		}
	}

	switch policy {
	case QuorumAll:
		return ayes == total, nil
	case QuorumMajority:
		return ayes > total/2, nil
	case QuorumAny:
		return ayes >= 1, nil
	default:
		return false, fmt.Errorf("quorum: unknown policy %d", policy)
	}
}

// Len returns the number of registered voters.
func (q *Quorum) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.voters)
}
