在 Claude Code 中输入：

> 调用 AMR 的 `memory_create`，在 `amr-demo` 项目保存这条我确认的决定：导出功能首版支持 CSV，JSON 放到下一版。返回工具结果中的 `memory_id` 和 `projection_status`。

`pending` 表示已落盘、索引进行中。记下返回的 `memory_id`。

在全新的 Codex 会话中输入：

> 用 AMR 的 `memory_search` 在 `amr-demo` 项目查询导出功能首版选择，返回工具结果中的 `memory_id`。若结果为空，等待约 1 秒再重试，最多 3 次。

比较两次返回的 `memory_id`。如果仍未搜到，在 Claude Code 中调用 `memory_get` 并传入该 ID。能读到表示已落盘，索引尚未可用或搜索未命中。
