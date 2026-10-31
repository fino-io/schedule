package schedule

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	pb "github.com/fino-io/schedule/go/schedule"
)

func TestTaskExpansionAndPath(t *testing.T) {
	root := NewSequence("root",
		NewParallel("parallel", NewTask("a"), NewSequence("nested", NewTask("b"), NewTask("c"))),
		NewTask("tail"),
	)
	tasks, err := GetTasks(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := taskIDs(tasks); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("initial tasks = %v", got)
	}
	path, err := FindPath(root, "c")
	if err != nil {
		t.Fatal(err)
	}
	if got := taskIDs(path); !reflect.DeepEqual(got, []string{"root", "parallel", "nested", "c"}) {
		t.Fatalf("path = %v", got)
	}
	if _, err := FindPath(root, "missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("missing task error = %v", err)
	}
}

func TestInvalidTasks(t *testing.T) {
	cycle := NewSequence("cycle")
	cycle.GetSequenceTask().Items = []*pb.WorkItemDefinition{cycle}
	shared := NewTask("shared")
	deep := NewTask("leaf")
	for i := range maxTaskDepth {
		deep = NewSequence(fmt.Sprint(i), deep)
	}
	tests := map[string]*pb.WorkItemDefinition{
		"nil":             nil,
		"empty sequence":  NewSequence("sequence"),
		"empty parallel":  NewParallel("parallel"),
		"nil child":       NewSequence("sequence", nil),
		"missing id":      NewTask(""),
		"missing kind":    {Id: "empty"},
		"nil wrapper":     {Id: "nil", Task: (*pb.WorkItemDefinition_SimpleTask)(nil)},
		"nil leaf":        {Id: "nil", Task: &pb.WorkItemDefinition_SimpleTask{}},
		"nil sequence":    {Id: "nil", Task: (*pb.WorkItemDefinition_SequenceTask)(nil)},
		"nil parallel":    {Id: "nil", Task: &pb.WorkItemDefinition_ParallelTask{}},
		"duplicate id":    NewParallel("parent", NewTask("same"), NewTask("same")),
		"shared subtree":  NewParallel("parent", shared, shared),
		"cyclic subtree":  cycle,
		"excessive depth": deep,
	}
	for name, root := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := GetTasks(root); err == nil {
				t.Fatal("invalid tree accepted")
			}
		})
	}
}

func TestAppendItemsAtomic(t *testing.T) {
	root := NewParallel("root")
	root.Name = "metadata"
	root.GetParallelTask().Master = "original"
	if err := AppendItems(root, NewTask("first")); err != nil {
		t.Fatal(err)
	}
	if err := AppendItems(root, NewTask("first")); err == nil {
		t.Fatal("duplicate accepted")
	}
	if len(root.GetParallelTask().Items) != 1 || root.Name != "metadata" || root.GetParallelTask().Master != "original" {
		t.Fatal("failed append changed the parent")
	}
	if err := AppendItems(root, root); err == nil {
		t.Fatal("self-reference accepted")
	}
	if err := AppendItems(NewTask("leaf"), NewTask("child")); err == nil {
		t.Fatal("leaf accepted a child")
	}
	if err := AppendItems(&pb.WorkItemDefinition{Id: "nil", Task: (*pb.WorkItemDefinition_ParallelTask)(nil)}, NewTask("child")); err == nil {
		t.Fatal("nil wrapper accepted")
	}
}

func taskIDs(tasks []*pb.WorkItemDefinition) []string {
	ids := make([]string, len(tasks))
	for i, task := range tasks {
		ids[i] = task.Id
	}
	return ids
}
