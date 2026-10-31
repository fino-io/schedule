package schedule

import (
	"errors"
	"fmt"

	pb "github.com/fino-io/schedule/go/schedule"
)

const (
	maxTaskDepth = 256
	maxTaskNodes = 10000
)

var ErrTaskNotFound = errors.New("task not found")

func NewTask(id string) *pb.WorkItemDefinition {
	return &pb.WorkItemDefinition{
		Id:   id,
		Task: &pb.WorkItemDefinition_SimpleTask{SimpleTask: &pb.SimpleTaskDefinition{}},
	}
}

func NewSequence(id string, items ...*pb.WorkItemDefinition) *pb.WorkItemDefinition {
	return &pb.WorkItemDefinition{
		Id: id,
		Task: &pb.WorkItemDefinition_SequenceTask{SequenceTask: &pb.SequenceTaskDefinition{
			Items: append([]*pb.WorkItemDefinition(nil), items...),
		}},
	}
}

func NewParallel(id string, items ...*pb.WorkItemDefinition) *pb.WorkItemDefinition {
	return &pb.WorkItemDefinition{
		Id: id,
		Task: &pb.WorkItemDefinition_ParallelTask{ParallelTask: &pb.ParallelTaskDefinition{
			Items: append([]*pb.WorkItemDefinition(nil), items...),
		}},
	}
}

// AppendItems accepts any task kind as a child. Invalid additions leave the
// parent unchanged. An empty composite can be filled before validating the tree.
func AppendItems(parent *pb.WorkItemDefinition, items ...*pb.WorkItemDefinition) error {
	if parent == nil {
		return errors.New("parent is required")
	}

	var current []*pb.WorkItemDefinition
	switch task := parent.Task.(type) {
	case *pb.WorkItemDefinition_SequenceTask:
		if task == nil || task.SequenceTask == nil {
			return fmt.Errorf("task %q has an empty sequence definition", parent.Id)
		}
		current = task.SequenceTask.Items
	case *pb.WorkItemDefinition_ParallelTask:
		if task == nil || task.ParallelTask == nil {
			return fmt.Errorf("task %q has an empty parallel definition", parent.Id)
		}
		current = task.ParallelTask.Items
	default:
		return fmt.Errorf("task %q is not a composite", parent.Id)
	}
	combined := append(append([]*pb.WorkItemDefinition(nil), current...), items...)
	candidate := NewSequence(parent.Id, combined...)
	if err := ValidateTask(candidate); err != nil {
		return err
	}

	if sequence := parent.GetSequenceTask(); sequence != nil {
		sequence.Items = combined
	} else {
		parent.GetParallelTask().Items = combined
	}
	return nil
}

// ValidateTask checks a bounded, nonempty tree with unique IDs and no shared or
// cyclic nodes. Maximum depth is 256 and maximum node count is 10,000.
func ValidateTask(root *pb.WorkItemDefinition) error {
	ids := make(map[string]bool)
	nodes := make(map[*pb.WorkItemDefinition]bool)
	var visit func(*pb.WorkItemDefinition, int) error
	visit = func(node *pb.WorkItemDefinition, depth int) error {
		if node == nil {
			return errors.New("task is required")
		}
		if depth > maxTaskDepth || len(nodes) >= maxTaskNodes {
			return errors.New("task tree exceeds depth or node limit")
		}
		if nodes[node] {
			return fmt.Errorf("task %q is shared or cyclic", node.Id)
		}
		if node.Id == "" || ids[node.Id] {
			return fmt.Errorf("task ID %q is empty or duplicated", node.Id)
		}
		nodes[node], ids[node.Id] = true, true

		var items []*pb.WorkItemDefinition
		switch task := node.Task.(type) {
		case *pb.WorkItemDefinition_SimpleTask:
			if task == nil || task.SimpleTask == nil {
				return fmt.Errorf("task %q has an empty leaf definition", node.Id)
			}
			return nil
		case *pb.WorkItemDefinition_SequenceTask:
			if task == nil || task.SequenceTask == nil {
				return fmt.Errorf("task %q has an empty sequence definition", node.Id)
			}
			items = task.SequenceTask.Items
		case *pb.WorkItemDefinition_ParallelTask:
			if task == nil || task.ParallelTask == nil {
				return fmt.Errorf("task %q has an empty parallel definition", node.Id)
			}
			items = task.ParallelTask.Items
		default:
			return fmt.Errorf("task %q has no task definition", node.Id)
		}
		if len(items) == 0 {
			return fmt.Errorf("task %q has no children", node.Id)
		}
		for _, item := range items {
			if err := visit(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root, 1)
}

// GetTasks preserves the source library's initial expansion: the first child
// of a sequence and every branch of a parallel. It does not inspect run states.
func GetTasks(root *pb.WorkItemDefinition) ([]*pb.WorkItemDefinition, error) {
	if err := ValidateTask(root); err != nil {
		return nil, err
	}
	var tasks []*pb.WorkItemDefinition
	var collect func(*pb.WorkItemDefinition)
	collect = func(node *pb.WorkItemDefinition) {
		if node.GetSimpleTask() != nil {
			tasks = append(tasks, node)
			return
		}
		items := children(node)
		if node.GetSequenceTask() != nil {
			items = items[:1]
		}
		for _, item := range items {
			collect(item)
		}
	}
	collect(root)
	return tasks, nil
}

// FindPath returns the root-to-target path, including both endpoints.
func FindPath(root *pb.WorkItemDefinition, targetID string) ([]*pb.WorkItemDefinition, error) {
	if err := ValidateTask(root); err != nil {
		return nil, err
	}
	var find func(*pb.WorkItemDefinition) []*pb.WorkItemDefinition
	find = func(node *pb.WorkItemDefinition) []*pb.WorkItemDefinition {
		if node.Id == targetID {
			return []*pb.WorkItemDefinition{node}
		}
		for _, child := range children(node) {
			if path := find(child); path != nil {
				return append([]*pb.WorkItemDefinition{node}, path...)
			}
		}
		return nil
	}
	if path := find(root); path != nil {
		return path, nil
	}
	return nil, fmt.Errorf("%w: %q", ErrTaskNotFound, targetID)
}

func children(node *pb.WorkItemDefinition) []*pb.WorkItemDefinition {
	if sequence := node.GetSequenceTask(); sequence != nil {
		return sequence.Items
	}
	return node.GetParallelTask().GetItems()
}
