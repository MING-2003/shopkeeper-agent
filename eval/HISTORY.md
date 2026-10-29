# 改进记录表

> 每完成一项改进，跑一次评测（`python eval/run_eval.py`），把 `eval/report.json`
> 的 summary 数字追加到下表，形成"每次改动 → 指标变化"的完整对照。
> 这既是回归保护，也是最直接的项目成果证明。

| 日期 | 改动 | 评测集 | pass_rate | recall_rate | sql_ok_rate | no_answer_pass_rate | retry_rate | P50 | P95 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-10-26 | 基线（首跑，判定含聚合空集缺陷） | 10 条 | 80% | 100% | 100% | 0% | 0% | 5.52s | 7.51s |
| 2026-10-27 | 修复评测判定后重测 | 10 条 | 90% | 100% | 100% | 50% | 0% | 6.36s | 9.54s |
|  | ↳ 备注：no_answer_antarctica 失败，模型本次用 COALESCE(SUM,0) 把无数据兜底成 0，按"编造"判定（正确）；改进方向：generate_sql Prompt 禁止 COALESCE 兜底 + 空结果友好提示 |  |  |  |  |  |  |  |  |
