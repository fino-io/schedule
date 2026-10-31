package schedule

import (
	"errors"

	pb "github.com/fino-io/schedule/go/schedule"
)

func ValidateWorkflow(definition *pb.WorkflowDefinition) error {
	if definition == nil {
		return errors.New("workflow definition is required")
	}
	if err := ValidateTask(definition.Root); err != nil {
		return err
	}
	if definition.Stateflow != nil {
		return ValidateStateflow(definition.Stateflow)
	}
	return nil
}

// Start creates a detached initial snapshot and the same first task batch as
// GetTasks. Activation never adds an implicit state transition or executes work.
// Subsequent scheduling and result aggregation remain the caller's concern,
// as the source library has no implementation of either behavior.
func Start(definition *pb.WorkflowDefinition) (*pb.WorkflowTransition, error) {
	if err := ValidateWorkflow(definition); err != nil {
		return nil, err
	}
	flow := definition.Stateflow
	if flow == nil {
		flow = DefaultStateflow()
	}
	result := &pb.WorkflowTransition{Instance: &pb.WorkflowInstance{}}
	var visit func(*pb.WorkItemDefinition, bool, []string)
	visit = func(node *pb.WorkItemDefinition, active bool, path []string) {
		path = append(path, node.Id)
		result.Instance.Items = append(result.Instance.Items, &pb.WorkItemState{
			NodeId:    node.Id,
			State:     flow.Init,
			Activated: active,
		})
		if active && node.GetSimpleTask() != nil {
			result.ReadyTasks = append(result.ReadyTasks, &pb.ReadyTask{
				NodeId: node.Id,
				Path:   append([]string(nil), path...),
			})
		}
		for index, child := range children(node) {
			childActive := active && (node.GetParallelTask() != nil || index == 0)
			visit(child, childActive, path)
		}
	}
	visit(definition.Root, true, nil)
	// With no outgoing initial transition, the root is already done on creation.
	result.Ended = true
	for _, step := range flow.Steps {
		if step.From == flow.Init {
			result.Ended = false
			break
		}
	}
	return result, nil
}
