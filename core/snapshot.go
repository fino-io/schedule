package schedule

import (
	"fmt"

	pb "github.com/fino-io/schedule/go/schedule"
	"google.golang.org/protobuf/proto"
)

func (w *Workflow) validate(instance *pb.Instance) error {
	if instance == nil || instance.Id == "" || !validText(instance.Id) ||
		instance.DefinitionHash != w.hash || instance.Revision == 0 {
		return fmt.Errorf("%w: instance identity, definition or revision", ErrInvalidSnapshot)
	}
	if instance.Mode < modeRunning || instance.Mode > modeStopped || len(instance.Nodes) != len(w.nodes) {
		return fmt.Errorf("%w: mode or node count", ErrInvalidSnapshot)
	}

	for i, state := range instance.Nodes {
		if state == nil || state.NodeId != w.nodes[i].Id ||
			state.Phase < pending || state.Phase > skipped || !validText(state.ResultRef, state.GetErrorMessage()) {
			return fmt.Errorf("%w: node order, identity or phase at %d", ErrInvalidSnapshot, i)
		}
	}
	if instance.Nodes[0].Phase == pending || instance.Nodes[0].Phase == skipped {
		return fmt.Errorf("%w: root was never activated", ErrInvalidSnapshot)
	}

	activated := make([]bool, len(w.nodes))
	for i := len(w.nodes) - 1; i >= 0; i-- {
		node, state := w.nodes[i], instance.Nodes[i]
		bad := func(reason string) error {
			return fmt.Errorf("%w: node %q: %s", ErrInvalidSnapshot, node.Id, reason)
		}

		if node.Kind == task {
			executed := state.Phase != pending && state.Phase != skipped
			if executed && state.ExecutionId != executionID(instance.Id, node.Id) ||
				!executed && state.ExecutionId != "" {
				return bad("execution identity")
			}
			activated[i] = executed

			if state.WinnerId != "" || state.Phase != skipped && state.CauseId != "" {
				return bad("leaf has combination metadata")
			}
			if !terminal(state.Phase) && (state.ErrorMessage != nil || state.ResultRef != "") ||
				state.Phase == succeeded && state.ErrorMessage != nil {
				return bad("result attached to a non-result state")
			}
		} else {
			for _, child := range w.children[i] {
				activated[i] = activated[i] || activated[child]
			}
			if state.ExecutionId != "" || state.Phase == ready {
				return bad("combination cannot be dispatched")
			}
			if state.Phase != pending && state.Phase != skipped && !activated[i] {
				return bad("combination has no activated descendant")
			}

			if node.Kind == seq {
				prefixSucceeded := true
				for _, child := range w.children[i] {
					childState := instance.Nodes[child]
					if activated[child] && !prefixSucceeded {
						return bad("later sequence item activated before its predecessor succeeded")
					}
					prefixSucceeded = prefixSucceeded && childState.Phase == succeeded
				}
			} else if state.Phase != pending && state.Phase != skipped {
				for _, child := range w.children[i] {
					if instance.Nodes[child].Phase == pending {
						return bad("parallel branch was not activated")
					}
				}
			}
			if state.Phase == pending || state.Phase == skipped {
				for _, child := range w.children[i] {
					if instance.Nodes[child].Phase != state.Phase {
						return bad("inactive subtree has active descendants")
					}
				}
			}

			if state.Phase == succeeded {
				if node.Kind == any {
					winner, exists := w.byID[state.WinnerId]
					if !exists || w.parent[winner] != i ||
						instance.Nodes[winner].Phase != succeeded || state.ResultRef != instance.Nodes[winner].ResultRef {
						return bad("invalid Any winner")
					}
				} else {
					for _, child := range w.children[i] {
						if instance.Nodes[child].Phase != succeeded {
							return bad("success with unsuccessful children")
						}
					}
				}
			} else if state.Phase == failed {
				if node.Kind == any {
					for _, child := range w.children[i] {
						phase := instance.Nodes[child].Phase
						if !terminal(phase) || phase == succeeded {
							return bad("failed Any still has a possible or actual success")
						}
					}

					if state.ErrorMessage == nil || *state.ErrorMessage != "no successful branch" || state.CauseId != "" {
						return bad("invalid Any failure")
					}
				} else {
					cause, exists := w.byID[state.CauseId]
					if !exists || w.parent[cause] != i ||
						!terminal(instance.Nodes[cause].Phase) || instance.Nodes[cause].Phase == succeeded {
						return bad("invalid failure source")
					}
					if node.Kind == seq {
						for _, child := range w.children[i] {
							if child == cause {
								break
							}
							if instance.Nodes[child].Phase != succeeded {
								return bad("sequence failure is ahead of its current item")
							}
						}
					}
				}
			}

			if state.WinnerId != "" && !(node.Kind == any && state.Phase == succeeded) {
				return bad("unexpected winner")
			}
			if state.ResultRef != "" && !(node.Kind == any && state.Phase == succeeded) {
				return bad("unexpected result reference")
			}
			if state.ErrorMessage != nil && !(node.Kind == any && state.Phase == failed) {
				return bad("unexpected combination error")
			}
			if state.CauseId != "" && state.Phase != failed && state.Phase != skipped && state.Phase != stopped {
				return bad("unexpected cause")
			}
			if state.Phase == stopped && (instance.Mode != modeStopped || state.CauseId != w.nodes[0].Id) {
				return bad("combination stopped without instance control")
			}
		}

		if state.Phase == skipped {
			cause, exists := w.byID[state.CauseId]
			validCause := exists && (instance.Nodes[cause].Phase == failed ||
				instance.Nodes[cause].Phase == stopped || instance.Mode == modeStopped && cause == 0)

			ancestor := false
			for parent := w.parent[i]; parent >= 0; parent = w.parent[parent] {
				ancestor = ancestor || parent == cause
			}

			if !validCause || !ancestor || state.ErrorMessage != nil || state.ResultRef != "" {
				return bad("invalid skip source")
			}
		}
	}

	// 通过同一归约规则检查是否遗漏汇总、激活或跳过，不维护另一套推进行为。
	normalized := proto.Clone(instance).(*pb.Instance)
	w.reconcile(normalized)
	if !proto.Equal(instance, normalized) {
		return fmt.Errorf("%w: snapshot has unapplied transitions", ErrInvalidSnapshot)
	}

	return nil
}
