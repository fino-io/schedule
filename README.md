# Schedule

基于 Fino proto 契约的 Go 任务编排基础库，由 `chaos-io/schedule` 重构而来。业务结构直接使用生成类型，不额外维护另一套任务、配置和状态 DTO。

## 架构设计

后续重构遵循[架构设计](docs/architecture.md)：用 `Task / Seq / All / Any` 表达任务树，用固定执行生命周期和纯事件推进器计算下一步，并区分结果确定与全部收尾。文档明确了执行规则、恢复与分派协议，以及成熟项目的取舍。

该文档是目标架构，完整推进器尚未实现。以下示例与说明描述当前代码行为。

## 目录

| 目录 | 内容 |
| --- | --- |
| `proto/schedule/` | 任务定义、状态流、运行快照和事件的契约源 |
| `go/schedule/` | Fino 生成的 Go 类型，属于独立的 API 模块，不手工修改 |
| `core/` | 项目维护的任务构建、校验、任务展开、路径查找、状态机和回调接口 |
| `docs/architecture.md` | 项目维护的目标架构与实施约定 |
| `docs/schedule/`、`openapi/` | Fino 生成的契约文档及 JSON Schema |

根 Go 模块通过 `replace github.com/fino-io/schedule/go => ./go` 使用同仓库生成类型。本地修改契约后不必先发布 API 模块；其他仓库使用时须先发布对应版本的 API 模块，或在自己的工作区关联本地模块。

## 使用

```go
import (
    schedule "github.com/fino-io/schedule/core"
    pb "github.com/fino-io/schedule/go/schedule"
)

definition := &pb.WorkflowDefinition{
    Root: schedule.NewSequence("root",
        schedule.NewParallel("prepare",
            schedule.NewTask("download"),
            schedule.NewTask("compile"),
        ),
        schedule.NewTask("publish"),
    ),
}

initial, err := schedule.Start(definition)
if err != nil {
    return err
}
// initial.ReadyTasks 包含 download、compile；publish 尚未激活。
// initial.Instance 与定义分离，节点初始状态为 created，不会自动转为 running。
```

`NewTask`、`NewSequence`、`NewParallel` 返回生成的 `WorkItemDefinition`，可直接填写 `Name/Type/EntityId/OperationName` 等元数据。`AppendItems` 统一接受三类子任务，失败不会修改父节点。构建过程中可暂时保留空组合，但执行校验或展开时会拒绝空组合、空节点、缺失/重复 ID、共享节点和循环引用。树深度上限为 256，节点数上限为 10,000。

`GetTasks` 保持原库的初始展开逻辑：串行只展开第一个子项，并行展开所有分支，结果顺序与定义一致，不根据运行状态筛选。`FindPath` 返回根至目标节点的路径；找不到时返回可用 `errors.Is(err, schedule.ErrTaskNotFound)` 判断的错误。两者返回定义树中的节点指针，按只读方式使用。

`Start` 仅初始化运行快照及首批任务，两个实例互不共享状态，激活不会产生隐式状态转换。`Stateflow` 未提供时使用原示例的默认状态流。

## 状态流

```go
flow, err := schedule.NewStateflow(&pb.StateflowDefinition{
    Init: "queued",
    Steps: []*pb.StateflowStep{
        {From: "queued", To: "executing", Event: "started"},
        {From: "executing", To: "done", Event: "completed"},
    },
})
if err != nil {
    return err
}
if err := flow.Goto("executing"); err != nil {
    return err
}
saved := flow.Snapshot()
restored, err := schedule.RestoreStateflow(saved)
```

状态机继续复用原依赖 `github.com/chaos-io/fsm`；适配层为状态名称分配内部编号，避免底层使用下划线拼接名称造成的冲突。状态名称、自环及循环均可自定义，不强制存在 `running/finished/failed`。定义与快照在进入状态机时复制，`Snapshot` 也返回副本，外部修改不会影响运行中的状态机。

`NewStateflow(nil)` 使用默认状态流：`created → running → failed/stopped/finished/paused`。`GetStart` 返回初始状态；`Goto` 只接受配置中存在的转换，失败时保持原状态；`HasNext/IsDone` 只判断是否有出边，因此默认配置中的 `paused` 为终态。`Orchestrate` 从初始状态重新构建，不累积转换。`Debug` 启用时通过 `log/slog` 输出转换及事件名称，日志级别由调用方控制。

`Stateflow` 是普通的有状态对象，同一实例的并发访问由调用方同步；不同实例没有全局配置或运行状态共享。

## 与原库的边界

原 `Scheduler` 的 `OnTask/OnState/OnEnd` 仍是 Go 接口，保留 `context.Context` 和 `error` 返回。回调参数改用生成的任务定义和运行快照：元数据从任务定义读取，节点状态从快照读取；实现方持有对应的工作流定义。节点的 `error_message` 只保存文本及存在性，不能恢复 Go 错误类型或错误链，实际 `error` 仍由调用方持有和处理。

原库没有实现调度循环、串行完成后的自动推进、并行结果汇总或回调驱动。当前代码只迁移其已有能力，尚未加入这些算法。`WorkflowTransition`、`WorkItemEvent`、`StateChange` 等类型为后续实现准备；`Start` 的输出也不会自动调用回调。后续完整推进规则及其与执行系统的边界以[架构设计](docs/architecture.md)为准。

原 `SkipBranchError`、`Master` 只声明了字段，没有执行行为；当前保留其数据，不推测它们的作用。并行任一成功的规则来自原 README；“提前成功后其他已启动分支继续执行”属于契约中标明的后续补充约定，尚无执行器实现。未迁移原库中未使用的 tracing 函数及 Redis 读取示例测试。

## 生成与验证

```bash
fino doctor --output=. --targets=api
fino build --output=. --targets=api
go test ./...
go test -race ./...
go vet ./...
cd go && go test ./...
```

仅生成 `api` 目标，不生成 backend、frontend、服务启动程序或部署资产。项目契约与生成归属见 `AGENTS.md` 和 `skills/fino/SKILL.md`。
