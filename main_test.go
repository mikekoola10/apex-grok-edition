package main

import (
	"testing"
)

func TestDecomposeGoal(t *testing.T) {
	subtasks := decomposeGoal("research nft")
	if len(subtasks) != 3 {
		t.Errorf("Expected 3 subtasks for research nft, got %d", len(subtasks))
	}

	subtasks = decomposeGoal("something else")
	if len(subtasks) != 1 {
		t.Errorf("Expected 1 subtask for generic goal, got %d", len(subtasks))
	}
}

func TestCreateSandbox(t *testing.T) {
	id := "12345678-1234-1234-1234-123456789012"
	sandbox := createSandbox(id)
	expected := "sandbox-12345678"
	if sandbox != expected {
		t.Errorf("Expected %s, got %s", expected, sandbox)
	}
}
