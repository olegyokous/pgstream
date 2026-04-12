package wal

import (
	"testing"
)

func priorityMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewPrioritizer_RequiresRules(t *testing.T) {
	_, err := NewPrioritizer(PriorityNormal, nil)
	if err == nil {
		t.Fatal("expected error for empty rules")
	}
}

func TestPrioritizer_DefaultLevelWhenNoMatch(t *testing.T) {
	p, _ := NewPrioritizer(PriorityLow, []PriorityRule{
		{Table: "orders", Priority: PriorityHigh},
	})
	level := p.Assign(priorityMsg("users", "INSERT"))
	if level != PriorityLow {
		t.Fatalf("expected %d, got %d", PriorityLow, level)
	}
}

func TestPrioritizer_TableMatchAssignsPriority(t *testing.T) {
	p, _ := NewPrioritizer(PriorityLow, []PriorityRule{
		{Table: "orders", Priority: PriorityHigh},
	})
	level := p.Assign(priorityMsg("orders", "INSERT"))
	if level != PriorityHigh {
		t.Fatalf("expected %d, got %d", PriorityHigh, level)
	}
}

func TestPrioritizer_ActionMatchAssignsPriority(t *testing.T) {
	p, _ := NewPrioritizer(PriorityLow, []PriorityRule{
		{Action: "DELETE", Priority: PriorityHigh},
	})
	level := p.Assign(priorityMsg("any_table", "DELETE"))
	if level != PriorityHigh {
		t.Fatalf("expected %d, got %d", PriorityHigh, level)
	}
}

func TestPrioritizer_FirstMatchWins(t *testing.T) {
	p, _ := NewPrioritizer(PriorityLow, []PriorityRule{
		{Table: "orders", Priority: PriorityHigh},
		{Table: "orders", Priority: PriorityNormal},
	})
	level := p.Assign(priorityMsg("orders", "UPDATE"))
	if level != PriorityHigh {
		t.Fatalf("expected %d, got %d", PriorityHigh, level)
	}
}

func TestPrioritizer_NilMessageReturnsDefault(t *testing.T) {
	p, _ := NewPrioritizer(PriorityNormal, []PriorityRule{
		{Table: "orders", Priority: PriorityHigh},
	})
	if got := p.Assign(nil); got != PriorityNormal {
		t.Fatalf("expected %d, got %d", PriorityNormal, got)
	}
}

func TestPrioritizer_SortOrdersHighToLow(t *testing.T) {
	p, _ := NewPrioritizer(PriorityLow, []PriorityRule{
		{Table: "orders", Priority: PriorityHigh},
		{Table: "logs", Priority: PriorityNormal},
	})
	msgs := []*Message{
		priorityMsg("logs", "INSERT"),
		priorityMsg("other", "INSERT"),
		priorityMsg("orders", "INSERT"),
	}
	p.Sort(msgs)
	if msgs[0].Table != "orders" {
		t.Fatalf("expected orders first, got %s", msgs[0].Table)
	}
	if msgs[1].Table != "logs" {
		t.Fatalf("expected logs second, got %s", msgs[1].Table)
	}
}
