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

workflow, err := schedule.Compile(&pb.Definition{
    Root: schedule.Seq("root",
        schedule.Any("source", schedule.Task("primary"), schedule.Task("backup")),
        schedule.Task("publish"),
    ),
})
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
    ExecutionId: primary.State.ExecutionId,
    ResultRef:   "results/primary",
})
if err != nil {
    return err
}
// next.Commands 新增 publish 的任务命令；backup 仍继续执行。
// initial 保持不变。整个流程的结果确定后，仍需等待 backup 收尾。
```

`Task/Seq/All/Any` 直接返回生成的 `Node`。可填写 `Name/Type/EntityId/OperationName` 等元数据，由执行器解释。组合节点本身不向 Worker 分派。

`Append` 仅做局部检查，允许先挂空组合再补齐子项。`Compile` 一次校验整树、复制定义并建立索引；拒绝空组合、重复 ID、共享节点和循环，深度上限 256、节点上限 10,000。编译后的 `Workflow` 不保存实例状态，可以并发复用。`Definition/Node/Path` 提供定义副本和路径查询，外部修改不会影响内部索引。

## 推进与恢复

- `workflow.Start(instanceID)` 初始化实例和首批命令。同 ID、同定义会计算出相同结果；是否已创建由接入方原子判断。
- `workflow.Advance(instance, event)` 校验快照及事件，在副本上计算。非法事件不修改输入，重复开始或相同终态不产生新命令，冲突终态返回 `ErrConflict`。
- `workflow.Restore(instance)` 验证定义绑定、节点状态和组合决策，返回独立快照与派生视图，不重新生成分派命令。

用 `proto.Marshal/Unmarshal` 或 `protojson.Marshal/Unmarshal` 保存生成类型。快照中的节点按定义前序排列，不可重排。定义指纹不匹配、依赖被绕过、执行标识错误或结果汇总缺失时，恢复会返回 `ErrInvalidSnapshot`。

`Transition.ResultReady` 表示根结果已经确定；`Transition.Settled` 表示所有分支均已收尾。`Any` 可以提前发布成功结果，其余已激活分支及内部串行后继继续执行。祖先失败或实例停止会阻止相应范围内的新分派，暂停则暂缓后继，恢复后继续推进。

控制通过 `Event` 的 `PAUSE/RESUME/STOP` 类型提交，实例 ID、事件 ID 必填，节点、执行标识和结果载荷留空。执行报告只接受 `Task` 节点，必须携带命令中的逻辑执行标识。停止不等于物理取消；已分派任务等待执行方的实际终态报告。节点重试由执行器完成，核心只接受最终逻辑结果；重跑整个流程使用新实例 ID。

## 命令与接入协议

`Command` 包含稳定 ID、实例与定义绑定、修订号、节点状态及路径。任务命令还包含完整叶子定义，可以直接保存到 outbox；同一次计算按状态通知、任务分派、结束通知的顺序输出。每种命令内部按定义顺序排列。

接入方必须保证：

1. 事件去重记录、新快照和待分派命令在同一事务中提交，更新实例时检查旧修订号或持有实例锁。
2. 使用命令 ID 幂等创建任务，保存后、入队前崩溃时补发原命令，不生成新 ID。
3. 以事件 ID 去重；同 ID 不同内容视为冲突。核心的状态幂等不能阻止旧暂停事件在恢复后被重新应用。
4. 可靠通知同样入库，重试投递由接入方负责。`END` 仅在首次全部收尾时生成，`Settled` 不代表通知已送达。

`Scheduler` 的 `OnTask/OnState/OnEnd` 接收命令，保留 `context.Context` 和 Go `error`。提交成功后可用 `Dispatch(ctx, scheduler, commands)` 按顺序分派。首个失败返回 `DispatchError`，包含索引、命令 ID 和原始错误，可用 `errors.Is/As` 判断；回调不能修改已记录命令，不自动回滚或重放整批命令。

内存接入协议测试覆盖丢失分派、重复投递、旧控制事件和并发提交；库不提供生产存储、超时回收或恰好一次副作用保证。

## 独立状态流

业务状态流与编排生命周期分离，使用本地 `(from, to)` 转换表，不依赖通用 FSM 框架：

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
instance := flow.Start()
nextState, step, err := flow.Advance(instance, "executing")
```

支持任意状态名、自环、循环和单态图。`step.Event` 是转换输出标签；`HasNext(state)` 判断有无出边，`Restore(instance)` 验证并复制快照。合法自环仍返回转换信息，失败不修改输入；编译定义和运行快照互不共享可变数据。

## 目录与验证

| 路径 | 归属 |
| --- | --- |
| `proto/schedule/` | 数据契约源，无 service、HTTP 或数据库模型 |
| `go/schedule/` | Fino 生成类型，不手工修改 |
| `core/` | 任务装配、编译、归约、快照校验、独立状态流及回调适配 |
| `docs/architecture.md` | 项目维护的架构与接入约定 |
| `docs/schedule/`、`openapi/` | Fino 生成的契约文档和 JSON Schema |

根 Go 模块通过 `replace github.com/fino-io/schedule/go => ./go` 使用同仓库 API。其他仓库接入时需发布对应 API 模块版本，或在自己的工作区关联本地模块。

```bash
fino doctor --output=. --targets=api
fino build --output=. --targets=api
go test ./...
go test -race ./...
go vet ./...
cd go && go test ./...
```

当前重构直接调整了契约和函数，不提供旧接口兼容层。仅生成 `api`，没有 backend、frontend、部署或应用二进制发布配置。
