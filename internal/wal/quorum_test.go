package wal

import (
	"errors"
	"testing"
)

func quorumMsg(table, action string) *Message {
	return &Message{Relation: table, Action: action}
}

func TestNewQuorum_RequiresAtLeastOneVoter(t *testing.T) {
	_, err := NewQuorum(QuorumAll)
	if err == nil {
		t.Fatal("expected error for zero voters")
	}
}

func TestNewQuorum_ValidConfig(t *testing.T) {
	v := func(m *Message) (bool, error) { return true, nil }
	q, err := NewQuorum(QuorumAll, v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Len() != 1 {
		t.Fatalf("expected 1 voter, got %d", q.Len())
	}
}

func TestQuorum_AllPolicyRequiresEveryVote(t *testing.T) {
	yes := func(m *Message) (bool, error) { return true, nil }
	no := func(m *Message) (bool, error) { return false, nil }

	q, _ := NewQuorum(QuorumAll, yes, yes, no)
	ok, err := q.Decide(quorumMsg("users", "INSERT"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false when not all voters agree")
	}

	q2, _ := NewQuorum(QuorumAll, yes, yes)
	ok2, _ := q2.Decide(quorumMsg("users", "INSERT"))
	if !ok2 {
		t.Fatal("expected true when all voters agree")
	}
}

func TestQuorum_MajorityPolicyRequiresMoreThanHalf(t *testing.T) {
	yes := func(m *Message) (bool, error) { return true, nil }
	no := func(m *Message) (bool, error) { return false, nil }

	q, _ := NewQuorum(QuorumMajority, yes, yes, no)
	ok, err := q.Decide(quorumMsg("orders", "UPDATE"))
	if err != nil || !ok {
		t.Fatalf("expected majority to pass: ok=%v err=%v", ok, err)
	}

	q2, _ := NewQuorum(QuorumMajority, yes, no, no)
	ok2, _ := q2.Decide(quorumMsg("orders", "UPDATE"))
	if ok2 {
		t.Fatal("expected false when minority agrees")
	}
}

func TestQuorum_AnyPolicyRequiresOneVote(t *testing.T) {
	yes := func(m *Message) (bool, error) { return true, nil }
	no := func(m *Message) (bool, error) { return false, nil }

	q, _ := NewQuorum(QuorumAny, no, no, yes)
	ok, err := q.Decide(quorumMsg("events", "DELETE"))
	if err != nil || !ok {
		t.Fatalf("expected any to pass: ok=%v err=%v", ok, err)
	}

	q2, _ := NewQuorum(QuorumAny, no, no)
	ok2, _ := q2.Decide(quorumMsg("events", "DELETE"))
	if ok2 {
		t.Fatal("expected false when no voter agrees")
	}
}

func TestQuorum_VoterErrorShortCircuits(t *testing.T) {
	boom := func(m *Message) (bool, error) { return false, errors.New("voter exploded") }
	q, _ := NewQuorum(QuorumAny, boom)
	_, err := q.Decide(quorumMsg("t", "INSERT"))
	if err == nil {
		t.Fatal("expected error from failing voter")
	}
}
