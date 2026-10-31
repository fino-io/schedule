package schedule

import (
	"errors"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func TestIndependentStateflow(t *testing.T) {
	definition := &pb.StateflowDefinition{
		Init: "a_b",
		Steps: []*pb.StateflowStep{
			{From: "a_b", To: "c", Event: "first"},
			{From: "a", To: "b_c", Event: "other"},
			{From: "c", To: "a"},
			{From: "a", To: "a", Event: "self"},
		},
	}
	flow, err := CompileStateflow(definition)
	if err != nil {
		t.Fatal(err)
	}
	definition.Init = "mutated"
	instance := flow.Start()
	if hasNext, err := flow.HasNext(instance.State); !hasNext || err != nil {
		t.Fatal("underscore state incorrectly terminal")
	}
	before := proto.Clone(instance)
	if _, _, err := flow.Advance(instance, "b_c"); !errors.Is(err, ErrInvalidEvent) || !proto.Equal(before, instance) {
		t.Fatal("colliding edge accepted or input mutated")
	}
	for _, target := range []string{"c", "a", "a", "b_c"} {
		next, step, err := flow.Advance(instance, target)
		if err != nil || step.From != instance.State || step.To != target {
			t.Fatalf("transition=%v error=%v", step, err)
		}
		if target == "a" && step.From == "a" && step.Event != "self" {
			t.Fatal("self-loop lost event label")
		}
		instance = next
	}
	if hasNext, err := flow.HasNext(instance.State); hasNext || err != nil {
		t.Fatal("terminal state has outgoing edge")
	}
	data, err := proto.Marshal(instance)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &pb.StateflowInstance{}
	if err := proto.Unmarshal(data, decoded); err != nil {
		t.Fatal(err)
	}
	restored, err := flow.Restore(decoded)
	if err != nil || !proto.Equal(restored, instance) {
		t.Fatal("snapshot round-trip failed")
	}
	if _, err := flow.HasNext("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestStateflowBoundaries(t *testing.T) {
	for _, definition := range []*pb.StateflowDefinition{
		nil, {}, {Init: "start", Steps: []*pb.StateflowStep{nil}},
		{Init: "start", Steps: []*pb.StateflowStep{{From: "start"}}},
		{Init: "start", Steps: []*pb.StateflowStep{{From: "start", To: "end"}, {From: "start", To: "end"}}},
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
	for _, snapshot := range []*pb.StateflowInstance{
		nil, {Definition: &pb.StateflowDefinition{Init: "other"}, State: "other"},
		{Definition: &pb.StateflowDefinition{Init: "only"}, State: "missing"},
	} {
		if _, err := flow.Restore(snapshot); !errors.Is(err, ErrInvalidSnapshot) {
			t.Fatal("invalid snapshot accepted")
		}
	}
}
