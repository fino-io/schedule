package schedule

import (
	"fmt"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

// Stateflow 是不可变的业务转换表，当前状态由调用方保存。
type Stateflow struct {
	initial string
	steps   map[string]map[string]*pb.StateflowStep
}

func CompileStateflow(definition *pb.StateflowDefinition) (*Stateflow, error) {
	if definition == nil || definition.Init == "" || !validText(definition.Init) {
		return nil, fmt.Errorf("%w: initial state is required", ErrInvalidDefinition)
	}

	flow := &Stateflow{
		initial: definition.Init,
		steps:   map[string]map[string]*pb.StateflowStep{definition.Init: {}},
	}

	for _, step := range definition.Steps {
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

		flow.steps[step.From][step.To] = proto.Clone(step).(*pb.StateflowStep)
	}

	return flow, nil
}

func (f *Stateflow) Initial() string {
	return f.initial
}

// Step 查询合法转换并返回副本，不保存或修改调用方的当前状态。
func (f *Stateflow) Step(state, target string) (*pb.StateflowStep, error) {
	steps, exists := f.steps[state]
	if !exists {
		return nil, fmt.Errorf("%w: state %q", ErrNotFound, state)
	}
	step := steps[target]
	if step == nil {
		return nil, fmt.Errorf("%w: cannot transition from %q to %q", ErrInvalidEvent, state, target)
	}

	return proto.Clone(step).(*pb.StateflowStep), nil
}

func (f *Stateflow) HasNext(state string) (bool, error) {
	steps, exists := f.steps[state]
	if !exists {
		return false, fmt.Errorf("%w: state %q", ErrNotFound, state)
	}
	return len(steps) > 0, nil
}
