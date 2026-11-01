package schedule_test

import (
	"fmt"

	schedule "github.com/fino-io/schedule/core"
	pb "github.com/fino-io/schedule/go/schedule"
)

func ExampleWorkflow() {
	workflow, err := schedule.Compile(&pb.Definition{
		Root: schedule.Seq("root",
			schedule.Any("source", schedule.Task("primary"), schedule.Task("backup")),
			schedule.Task("publish"),
		),
	})
	if err != nil {
		panic(err)
	}

	current, err := workflow.Start("run-42")
	if err != nil {
		panic(err)
	}

	for i, node := range []string{"primary", "publish", "backup"} {
		var executionID string
		for _, state := range current.Instance.Nodes {
			if state.NodeId == node {
				executionID = state.ExecutionId
			}
		}
		current, err = workflow.Advance(current.Instance, &pb.Event{
			Id:          fmt.Sprintf("event-%d", i),
			InstanceId:  "run-42",
			Kind:        pb.EventKind_EVENT_KIND_SUCCEEDED,
			NodeId:      node,
			ExecutionId: executionID,
		})
		if err != nil {
			panic(err)
		}
		fmt.Println(node, current.ResultReady, current.Settled)
	}
	// Output:
	// primary false false
	// publish true false
	// backup true true
}
