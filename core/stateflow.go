package schedule

import (
	"fmt"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

// Stateflow 仅索引独立业务状态流；实例只使用生成结构体，不维护第二份机器状态。
type Stateflow struct {
	definition *pb.StateflowDefinition
	steps      map[string]map[string]*pb.StateflowStep
}

func CompileStateflow(definition *pb.StateflowDefinition) (*Stateflow, error) {
	if definition == nil || definition.Init == "" || !validText(definition.Init) {
		return nil, fmt.Errorf("%w: initial state is required", ErrInvalidDefinition)
	}

	flow := &Stateflow{
		definition: proto.Clone(definition).(*pb.StateflowDefinition),
		steps:      map[string]map[string]*pb.StateflowStep{definition.Init: {}},
	}

	for _, step := range flow.definition.Steps {
		if step == nil || step.From == "" || step.To == "" || !validText(step.From, step.To, step.Event) {
			return nil, fmt.Errorf("%w: transition endpoints are required", ErrInvalidDefinition)
		}

		for _, state := range []string{step.From, step.To} {
			if flow.steps[state] == nil {
				flow.steps[state] = make(map[string]*pb.StateflowStep)
			}
		}
		if flow.steps[step.From][step.To] != nil {
			return nil, fmt.Errorf("%w: duplicate transition %q -> %q", ErrInvalidDefinition, step.From, step.To)
		}

		flow.steps[step.From][step.To] = step
	}

	return flow, nil
}

func (f *Stateflow) Start() *pb.StateflowInstance {
	return &pb.StateflowInstance{
		Definition: proto.Clone(f.definition).(*pb.StateflowDefinition),
		State:      f.definition.Init,
	}
}

func (f *Stateflow) Restore(instance *pb.StateflowInstance) (*pb.StateflowInstance, error) {
	if instance == nil || !proto.Equal(instance.Definition, f.definition) || f.steps[instance.State] == nil {
		return nil, fmt.Errorf("%w: stateflow definition or state does not match", ErrInvalidSnapshot)
	}
	return proto.Clone(instance).(*pb.StateflowInstance), nil
}

// Advance 返回新的实例和转换标签，支持合法自环，失败时不改变输入。
func (f *Stateflow) Advance(instance *pb.StateflowInstance, target string) (*pb.StateflowInstance, *pb.StateflowStep, error) {
	next, err := f.Restore(instance)
	if err != nil {
		return nil, nil, err
	}

	step := f.steps[instance.State][target]
	if step == nil {
		return nil, nil, fmt.Errorf("%w: cannot transition from %q to %q", ErrInvalidEvent, instance.State, target)
	}

	next.State = target

	return next, proto.Clone(step).(*pb.StateflowStep), nil
}

func (f *Stateflow) HasNext(state string) (bool, error) {
	steps, exists := f.steps[state]
	if !exists {
		return false, fmt.Errorf("%w: state %q", ErrNotFound, state)
	}
	return len(steps) > 0, nil
}
