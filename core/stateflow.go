package schedule

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/chaos-io/fsm"
	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

// Stateflow owns an isolated generated snapshot and delegates transitions to
// the source library's FSM engine. Callers must synchronize concurrent use.
type Stateflow struct {
	instance *pb.StateflowInstance
	machine  *fsm.Machine
	states   map[string]string
}

// DefaultStateflow returns the source flow.yaml transitions with debug disabled.
func DefaultStateflow() *pb.StateflowDefinition {
	return &pb.StateflowDefinition{
		Init: "created",
		Steps: []*pb.StateflowStep{
			{From: "created", To: "running"},
			{From: "running", To: "failed", Event: "failed"},
			{From: "running", To: "stopped", Event: "stopped"},
			{From: "running", To: "finished", Event: "finished"},
			{From: "running", To: "paused", Event: "paused"},
		},
	}
}

func ValidateStateflow(definition *pb.StateflowDefinition) error {
	if definition == nil || definition.Init == "" {
		return errors.New("initial state is required")
	}
	seen := make(map[[2]string]bool)
	for _, step := range definition.Steps {
		if step == nil || step.From == "" || step.To == "" {
			return errors.New("state transition endpoints are required")
		}
		key := [2]string{step.From, step.To}
		if seen[key] {
			return fmt.Errorf("duplicate state transition %q -> %q", step.From, step.To)
		}
		seen[key] = true
	}
	return nil
}

// NewStateflow uses the default definition when definition is nil.
func NewStateflow(definition *pb.StateflowDefinition) (*Stateflow, error) {
	if definition == nil {
		definition = DefaultStateflow()
	}
	return RestoreStateflow(&pb.StateflowInstance{
		Definition: definition,
		State:      definition.Init,
	})
}

func RestoreStateflow(instance *pb.StateflowInstance) (*Stateflow, error) {
	if err := ValidateStateflow(instance.GetDefinition()); err != nil {
		return nil, err
	}
	flow := &Stateflow{instance: proto.Clone(instance).(*pb.StateflowInstance)}
	flow.rebuild()
	if _, exists := flow.states[instance.State]; !exists {
		return nil, fmt.Errorf("unknown state %q", instance.State)
	}
	return flow, nil
}

// Orchestrate resets the machine from a fresh blueprint, without accumulating
// transitions when called repeatedly.
func (s *Stateflow) Orchestrate() {
	s.instance.State = s.instance.Definition.Init
	s.rebuild()
}

func (s *Stateflow) GetStart() string {
	return s.instance.Definition.Init
}

func (s *Stateflow) State() string {
	return s.instance.State
}

func (s *Stateflow) Goto(state string) error {
	target, known := s.states[state]
	if !known || !s.machine.Allows(target) {
		return fmt.Errorf("cannot transition from %q to %q", s.State(), state)
	}
	previous := s.State()
	if err := s.machine.Goto(target); err != nil {
		return err
	}
	s.instance.State = state
	if s.instance.Definition.Debug {
		for _, step := range s.instance.Definition.Steps {
			if step.From == previous && step.To == state {
				slog.Debug("state transition", "from", previous, "to", state, "event", step.Event)
				break
			}
		}
	}
	return nil
}

func (s *Stateflow) HasNext() bool {
	return s.machine.HasNext()
}

func (s *Stateflow) IsDone() bool {
	return !s.HasNext()
}

// Snapshot is detached from the live machine and can be serialized or restored.
func (s *Stateflow) Snapshot() *pb.StateflowInstance {
	return proto.Clone(s.instance).(*pb.StateflowInstance)
}

func (s *Stateflow) rebuild() {
	blueprint := fsm.New()
	s.states = make(map[string]string)
	// FSM internally joins names with underscores. Fixed-width numeric aliases
	// avoid key collisions and preserve its search ordering for custom names.
	alias := func(name string) string {
		if id, exists := s.states[name]; exists {
			return id
		}
		id := fmt.Sprintf("%020d", len(s.states))
		s.states[name] = id
		return id
	}
	alias(s.instance.Definition.Init)
	for _, step := range s.instance.Definition.Steps {
		blueprint.From(alias(step.From)).To(alias(step.To))
	}
	blueprint.Start(s.states[s.instance.State])
	s.machine = blueprint.Machine()
}
