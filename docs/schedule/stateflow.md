## `schedule.StateflowDefinition`

原 Flow/Step 配置的类型化表达，替代进程全局 YAML 单例。
 状态保留字符串以支持自定义状态；原示例配置使用 paused，不自动转换状态名称。
 HasNext 取决于当前状态是否存在出边；无出边即 IsDone，包括原配置的 paused。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `init` | `string` |  | 是 |  |  |
| `steps` | `Array<schedule.StateflowStep>` |  | 否 |  |  |
| `debug` | `boolean` |  | 否 |  | 仅控制诊断输出，不影响转换结果；遍历时须处理环和重复状态。 |


## `schedule.StateflowStep`

转换由目标状态触发，event 是转换成功后的回调名称，不是输入触发器。
 同一 from/to 只能声明一次；循环转换合法，不能将状态流当作无环图校验。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `from` | `string` |  | 是 |  |  |
| `to` | `string` |  | 是 |  |  |
| `event` | `string` |  | 否 |  |  |


## `schedule.StateflowInstance`

独立状态机的运行数据，对应原 Stateflow 的配置及当前 State。
 不依赖任务树；GetStart 由 definition.init 取得，Goto/HasNext/IsDone 由 Go 实现。

| 字段 | 类型 | 格式 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|---|
| `definition` | `schedule.StateflowDefinition` |  | 是 |  |  |
| `state` | `string` |  | 是 |  | 必须属于 definition 的初始状态或转换端点；新建实例时使用 definition.init。 |
