package schedule

import (
	"context"

	pb "github.com/fino-io/schedule/go/schedule"
)

// Scheduler preserves the source callback boundary, including cancellation and
// Go errors. Task metadata comes from the definition; states come from the
// instance. The implementing caller owns the corresponding workflow definition.
// Defining these callbacks does not introduce a dispatch loop or aggregation.
type Scheduler interface {
	OnTask(ctx context.Context, task *pb.WorkItemDefinition, instance *pb.WorkflowInstance) error
	OnState(ctx context.Context, task *pb.WorkItemDefinition, instance *pb.WorkflowInstance) error
	OnEnd(ctx context.Context, instance *pb.WorkflowInstance) error
}
