package schedule

import (
	"errors"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
)

func TestIndependentStateflow(t *testing.T) {
	definition := &pb.StateflowDefinition{
		Init: "a_b",
		Steps: []*pb.StateflowStep{
			{
				From:  "a_b",
				To:    "c",
				Event: "first",
			},
			{
				From:  "a",
				To:    "b_c",
				Event: "other",
			},
			{
				From: "c",
				To:   "a",
			},
			{
				From:  "a",
				To:    "a",
				Event: "self",
			},
		},
	}
	flow, err := CompileStateflow(definition)
	if err != nil {
		t.Fatal(err)
	}

	definition.Init = "mutated"
	definition.Steps[0].To = "mutated"
	state := flow.Initial()
	if hasNext, err := flow.HasNext(state); !hasNext || err != nil {
		t.Fatal("underscore state incorrectly terminal")
	}
	if _, err := flow.Step(state, "b_c"); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal("colliding edge accepted")
	}

	for _, target := range []string{"c", "a", "a", "b_c"} {
		step, err := flow.Step(state, target)
		if err != nil || step.From != state || step.To != target {
			t.Fatalf("transition=%v error=%v", step, err)
		}
		if target == "a" && step.From == "a" && step.Event != "self" {
			t.Fatal("self-loop lost event label")
		}
		state = step.To
		step.To = "mutated"
		repeated, err := flow.Step(step.From, target)
		if err != nil || repeated.To != target {
			t.Fatal("returned step exposed the transition table")
		}
	}
	if hasNext, err := flow.HasNext(state); hasNext || err != nil {
		t.Fatal("terminal state has outgoing edge")
	}

	if _, err := flow.HasNext("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestStateflowBoundaries(t *testing.T) {
	for _, definition := range []*pb.StateflowDefinition{
		nil,
		{},
		{Init: "\xff"},
		{
			Init:  "start",
			Steps: []*pb.StateflowStep{nil},
		},
		{
			Init:  "start",
			Steps: []*pb.StateflowStep{{From: "start"}},
		},
		{
			Init: "start",
			Steps: []*pb.StateflowStep{
				{
					From: "start",
					To:   "end",
				},
				{
					From: "start",
					To:   "end",
				},
			},
		},
	} {
		if _, err := CompileStateflow(definition); !errors.Is(err, ErrInvalidDefinition) {
			t.Fatal("invalid stateflow accepted")
		}
	}

	flow, err := CompileStateflow(&pb.StateflowDefinition{Init: "only"})
	if err != nil {
		t.Fatal(err)
	}
	if hasNext, err := flow.HasNext("only"); hasNext || err != nil {
		t.Fatal("single-state flow should be terminal")
	}

	if flow.Initial() != "only" {
		t.Fatal("initial state was lost")
	}
	if _, err := flow.Step("missing", "only"); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown state accepted")
	}
	if _, err := flow.Step("only", "only"); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal("single-state flow accepted an undeclared self-loop")
	}
}
