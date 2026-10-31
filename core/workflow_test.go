package schedule

import (
	"fmt"
	"reflect"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func TestStart(t *testing.T) {
	definition := &pb.WorkflowDefinition{
		Root: NewSequence("root",
			NewParallel("parallel", NewTask("a"), NewSequence("nested", NewTask("b"), NewTask("c"))),
			NewTask("tail"),
		),
		Stateflow: &pb.StateflowDefinition{Init: "queued", Steps: []*pb.StateflowStep{{From: "queued", To: "executing"}}},
	}
	before := proto.Clone(definition)
	first, err := Start(definition)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Start(definition)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, definition) || !proto.Equal(first, second) {
		t.Fatal("start mutated its definition or produced different results")
	}
	var ids []string
	for _, item := range first.Instance.Items {
		ids = append(ids, item.NodeId)
		if item.State != "queued" || item.ErrorMessage != nil {
			t.Fatal("activation changed a state or introduced an error")
		}
		wantActive := item.NodeId != "c" && item.NodeId != "tail"
		if item.Activated != wantActive {
			t.Fatalf("activation of %s = %v", item.NodeId, item.Activated)
		}
	}
	if !reflect.DeepEqual(ids, []string{"root", "parallel", "a", "nested", "b", "c", "tail"}) {
		t.Fatalf("instance order = %v", ids)
	}
	if len(first.ReadyTasks) != 2 || first.ReadyTasks[0].NodeId != "a" || first.ReadyTasks[1].NodeId != "b" {
		t.Fatalf("initial tasks = %v", first.ReadyTasks)
	}
	if !reflect.DeepEqual(first.ReadyTasks[1].Path, []string{"root", "parallel", "nested", "b"}) {
		t.Fatalf("task path = %v", first.ReadyTasks[1].Path)
	}
	first.Instance.Items[0].State = "mutated"
	first.ReadyTasks[0].Path[0] = "mutated"
	if second.Instance.Items[0].State != "queued" || second.ReadyTasks[0].Path[0] != "root" {
		t.Fatal("instances share mutable state")
	}
	if first.Ended || len(first.StateChanges) != 0 {
		t.Fatal("start introduced an implicit state change or completion")
	}
}

func ExampleStart() {
	definition := &pb.WorkflowDefinition{
		Root: NewSequence("root", NewParallel("prepare", NewTask("download"), NewTask("compile")), NewTask("publish")),
	}
	transition, err := Start(definition)
	if err != nil {
		panic(err)
	}
	for _, task := range transition.ReadyTasks {
		fmt.Println(task.NodeId, task.Path)
	}
	// Output:
	// download [root prepare download]
	// compile [root prepare compile]
}
