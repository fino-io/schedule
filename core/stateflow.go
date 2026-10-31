package schedule

import (
	"fmt"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

// Stateflow 仅索引独立业务状态流；实例只使用生成结构体，不维护第二份机器状态。
type Stateflow struct {
	definition *pb.StateflowDefinition
	steps      map[[2]string]*pb.StateflowStep
	states     map[string]bool
	outgoing   map[string]bool
}

func CompileStateflow(definition *pb.StateflowDefinition) (*Stateflow, error) {
	if definition == nil || definition.Init == "" || !validText(definition.Init) {
		return nil, fmt.Errorf("%w: initial state is required", ErrInvalidDefinition)
	}
	flow := &Stateflow{
		definition: proto.Clone(definition).(*pb.StateflowDefinition),
		steps:      make(map[[2]string]*pb.StateflowStep),
		states:     map[string]bool{definition.Init: true},
		outgoing:   make(map[string]bool),
	}
	for _, step := range flow.definition.Steps {
		if step == nil || step.From == "" || step.To == "" || !validText(step.From, step.To, step.Event) {
			return nil, fmt.Errorf("%w: transition endpoints are required", ErrInvalidDefinition)
		}
		key := [2]string{step.From, step.To}
		if _, exists := flow.steps[key]; exists {
			return nil, fmt.Errorf("%w: duplicate transition %q -> %q", ErrInvalidDefinition, step.From, step.To)
		}
		flow.steps[key] = step
		flow.states[step.From], flow.states[step.To] = true, true
		flow.outgoing[step.From] = true
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
	if instance == nil || !proto.Equal(instance.Definition, f.definition) || !f.states[instance.State] {
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
	step, exists := f.steps[[2]string{instance.State, target}]
	if !exists {
		return nil, nil, fmt.Errorf("%w: cannot transition from %q to %q", ErrInvalidEvent, instance.State, target)
	}
	next.State = target
	return next, proto.Clone(step).(*pb.StateflowStep), nil
}

func (f *Stateflow) HasNext(state string) (bool, error) {
	if !f.states[state] {
		return false, fmt.Errorf("%w: state %q", ErrNotFound, state)
	}
	return f.outgoing[state], nil
}
