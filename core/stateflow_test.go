package schedule

import (
	"fmt"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func TestDefaultStateflow(t *testing.T) {
	for _, terminal := range []string{"failed", "stopped", "finished", "paused"} {
		t.Run(terminal, func(t *testing.T) {
			flow, err := NewStateflow(nil)
			if err != nil {
				t.Fatal(err)
			}
			if flow.GetStart() != "created" || flow.IsDone() {
				t.Fatal("unexpected initial state")
			}
			if err := flow.Goto("running"); err != nil {
				t.Fatal(err)
			}
			before := flow.Snapshot()
			if err := flow.Goto("pause"); err == nil || !proto.Equal(before, flow.Snapshot()) {
				t.Fatal("invalid transition changed the machine")
			}
			if err := flow.Goto(terminal); err != nil {
				t.Fatal(err)
			}
			if flow.State() != terminal || !flow.IsDone() {
				t.Fatal("terminal state not applied")
			}
			flow.Orchestrate()
			flow.Orchestrate()
			if flow.State() != "created" || len(flow.Snapshot().Definition.Steps) != 5 {
				t.Fatal("reset accumulated transitions")
			}
		})
	}
}

func TestCustomStatesCyclesAndRestore(t *testing.T) {
	definition := &pb.StateflowDefinition{
		Init: "a_b",
		Steps: []*pb.StateflowStep{
			{From: "a_b", To: "c", Event: "first"},
			{From: "a", To: "b_c", Event: "collision"},
			{From: "c", To: "a", Event: "continue"},
			{From: "a", To: "a", Event: "self"},
		},
	}
	flow, err := NewStateflow(definition)
	if err != nil {
		t.Fatal(err)
	}
	definition.Init = "mutated"
	if !flow.HasNext() {
		t.Fatal("underscore state lost its outgoing transition")
	}
	if err := flow.Goto("b_c"); err == nil {
		t.Fatal("colliding transition accepted")
	}
	for _, state := range []string{"c", "a", "a"} {
		if err := flow.Goto(state); err != nil {
			t.Fatal(err)
		}
	}
	data, err := proto.Marshal(flow.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &pb.StateflowInstance{}
	if err := proto.Unmarshal(data, snapshot); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreStateflow(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Definition.Steps[1].To = "mutated"
	if err := restored.Goto("b_c"); err != nil || !restored.IsDone() {
		t.Fatalf("restored machine = %s, error = %v", restored.State(), err)
	}
	if flow.State() != "a" {
		t.Fatal("restored machine shared state with the original")
	}
}

func TestStateflowWithManyStates(t *testing.T) {
	definition := &pb.StateflowDefinition{Init: "state_0"}
	for i := range 32 {
		definition.Steps = append(definition.Steps, &pb.StateflowStep{
			From: fmt.Sprintf("state_%d", i),
			To:   fmt.Sprintf("state_%d", i+1),
		})
	}
	flow, err := NewStateflow(definition)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 32; i++ {
		if !flow.HasNext() {
			t.Fatalf("state %s unexpectedly terminal", flow.State())
		}
		if err := flow.Goto(fmt.Sprintf("state_%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if !flow.IsDone() {
		t.Fatal("last state should be terminal")
	}
}

func TestInvalidStateflow(t *testing.T) {
	for _, definition := range []*pb.StateflowDefinition{
		{},
		{Init: "start", Steps: []*pb.StateflowStep{nil}},
		{Init: "start", Steps: []*pb.StateflowStep{{From: "start"}}},
		{Init: "start", Steps: []*pb.StateflowStep{{From: "start", To: "end"}, {From: "start", To: "end"}}},
	} {
		if _, err := NewStateflow(definition); err == nil {
			t.Fatal("invalid definition accepted")
		}
	}
	for _, instance := range []*pb.StateflowInstance{nil, {Definition: DefaultStateflow(), State: "unknown"}} {
		if _, err := RestoreStateflow(instance); err == nil {
			t.Fatal("invalid snapshot accepted")
		}
	}
	flow, err := NewStateflow(&pb.StateflowDefinition{Init: "only"})
	if err != nil || !flow.IsDone() {
		t.Fatal("single-state flow should be terminal")
	}
}
