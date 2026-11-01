# Schedule

基于 Fino proto 契约的 Go 编排库。用静态任务树表达依赖，通过执行事件计算新快照和待分派命令。核心不运行任务，不连接数据库或消息队列。

| 组合 | 含义 |
| --- | --- |
| `Task` | 一个实际执行的任务 |
| `Seq` | 依次执行，前项成功后启动后项 |
| `All` | 并行执行，全部成功才成功 |
| `Any` | 并行执行，任一成功即可成功 |

详细规则和接入边界见[架构设计](docs/architecture.md)。

## 使用

```go
import (
    schedule "github.com/fino-io/schedule/core"
    pb "github.com/fino-io/schedule/go/schedule"
)

workflow, err := schedule.Compile(schedule.Seq("root",
    schedule.Any("source", schedule.Task("primary"), schedule.Task("backup")),
    schedule.Task("publish"),
))
if err != nil {
    return err
}

initial, err := workflow.Start("run-42")
if err != nil {
    return err
}
// initial.Commands 包含 primary、backup 的任务命令及状态通知。
// 接入方原子保存 initial.Instance 与命令，然后幂等投递。

var primary *pb.Command
for _, command := range initial.Commands {
    if command.Kind == pb.CommandKind_COMMAND_KIND_TASK && command.State.NodeId == "primary" {
        primary = command
    }
}
next, err := workflow.Advance(initial.Instance, &pb.Event{
    Id:          "event-1",
    InstanceId:  "run-42",
    Kind:        pb.EventKind_EVENT_KIND_SUCCEEDED,
    NodeId:      "primary",
    ExecutionId: primary.Id,
    ResultRef:   "results/primary",
})
if err != nil {
    return err
}
// next.Commands 新增 publish 的任务命令；backup 仍继续执行。
// initial 保持不变。整个流程的结果确定后，仍需等待 backup 收尾。
```

`Task/Seq/All/Any` 返回生成的 `Node`，业务元数据由执行器解释。定义编译和查询见[调用路径](docs/architecture.md#调用路径)。

执行报告的 `Event.ExecutionId` 原样回传任务命令的 `Id`。接入方须原子保存事件去重记录、快照与 outbox，提交后可用 `Dispatch(ctx, handle, commands)` 幂等投递。完整约定见[接入方责任](docs/architecture.md#接入方责任)。

暂停、停止及 `ResultReady/Settled` 的含义见[固定执行规则](docs/architecture.md#固定执行规则)；持久化、恢复和命令处理见[快照与命令的一致性](docs/architecture.md#快照与命令的一致性)。

## 独立状态流

业务状态流使用独立的 `(from, to)` 转换表，当前状态由业务保存：

```go
flow, err := schedule.CompileStateflow(&pb.StateflowDefinition{
    Init: "queued",
    Steps: []*pb.StateflowStep{
        {From: "queued", To: "executing", Event: "started"},
        {From: "executing", To: "done", Event: "completed"},
    },
})
if err != nil {
    return err
}
state := flow.Initial()
step, err := flow.Step(state, "executing")
if err != nil {
    return err
}
state = step.To
```

`step.Event` 是转换输出标签；`HasNext(state)` 判断有无出边。支持任意状态名、自环、循环和单态图，输入定义及返回值均不与内部表共享可变数据。

## 开发与验证

代码职责和生成归属见[代码结构](docs/architecture.md#代码结构)。本地消费方需同时关联根模块和 `go/` API 模块，具体方式见[消费方接入](docs/architecture.md#消费方接入)。

实现改动的基本检查：

```bash
go test ./...
go vet ./...
```

竞态、API 模块、fuzz、基准和契约生成的检查方式见[验证](docs/architecture.md#验证)。
