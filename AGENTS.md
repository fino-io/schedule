# 项目协作指南

这是由 Fino 生成的项目。契约、生成目标、代码归属和发布流程的详细说明见 `skills/fino/SKILL.md`。团队可按需补充本文件。

## 文件归属

- 以 `proto/` 作为 API 的唯一事实来源。
- 不要手动修改标记为 `DO NOT EDIT` 的文件或 `.fino/ir/` 下的文件；应修改对应的 proto、配置或模板，然后重新生成。
- 项目自有的业务逻辑应放在后端和前端的非生成文件中。
- 重新生成后检查代码差异，确认契约变更与业务实现一致。

## Go 代码排版

- 采用分段型排版，参考 `../beacon/backend/modules/beacon-server/app/ingest.go` 和 `../beacon/backend/infra/http/client.go` 的职责分段。
- 长函数按校验、数据准备、执行或状态更新、结果返回分段，循环和回调内也按实际职责分段。
- 同组赋值和连续检查保持紧凑，调用与错误检查保持相邻；短函数不机械增加空行。
- 多字段构造结果逐字段换行，简单调用保持自然长度，复杂条件和嵌套调用按逻辑边界展开。

## 生成流程

- API 变更先修改 `proto/`，再从项目根目录运行 `fino build --output=. --targets=...`。
- 按变更范围选择生成目标，配置以 `fino.yaml` 为准；目标与依赖关系见 `skills/fino/SKILL.md`。

## 验证

- 环境配置或生成目标变更时，运行 `fino doctor --output=. --targets=...` 检查相关工具链与前置条件。
- 后端变更应运行相关 Go 测试；需要更全面的验证时，运行 `cd backend && go test ./...`。
- 前端变更应运行 `cd frontend && npm run build`。
