# ADR: 容器工具面（container tool-plane sidecar）作为 Bench 被测环境入口

- 状态：已接受（2026-09-15，随 4 新基准接入）
- 关联：`evol_bench/`（GAIA / SkillsBench / SkillLearnBench / EarthBench 接入）；`pi-group-chat-host/cmd/bench-runner --extension-file --sidecar-url`
- 决策范围：harness 中被测 agent 如何拿到"官方任务环境"的**工具面**，用于在容器/MCP 内执行真实操作并被官方 verifier 判分。

## 背景

SKILLER 评测套件所用的四个基准——SkillsBench、SkillLearnBench、EarthBench（GAIA 走 room-bridge 无工具面）——其官方 agent 环境都是**容器或工具服务器**形态，而非 React 会话：
- SkillsBench：`<task>/environment/Dockerfile` + `verifier/test.sh`，官方 BenchFlow 在容器 shell 里跑。
- SkillLearnBench：guard 容器（`environment/Dockerfile` + `tests/test.sh`），Dockerfile 引用注入式 `skills/`。
- EarthBench：FastMCP 104 工具（Analysis/Index/Inversion/Perception/Statistics 五 kit），官方 agent 通过 MCP 调工具读遥感数据并作答。

harness 的基准通用 runner（bench-runner）原先只支持"room bridge 通道"（模型发 room_send，无执行能力），不足以驱动这类必须真执行的环境。需要在被测 pipeline 与官方任务容器/MCP 之间插入一个**工具面**。

## 备选方案

1. **MCP 原生直连**：Pi 扩展直接注册官方 FastMCP 客户端工具（EarthBench 天然是 MCP）。问题：SkillsBench/SkillLearnBench 是"shell 终端"而非 MCP 协议，无法统一；且官方 MCP 客户端把工具执行耦合在 biz-logic 内，难以按 episode 隔离双臂（需要 per-episode 环境键/目录切换）。
2. **容器/进程内嵌**：把环境构建逻辑放进 bench-runner（Go）。耦合度最高，且 Go 侧无法复用官方的 Python/FastMCP 栈；每个基准的构建差异会污染 runner 本体。
3. **独立 HTTP sidecar（选定）**：每个基准一个轻量 HTTP 服务（`*_env_server.py`），持有官方任务容器/工具引用的唯一真源，暴露幂等端点
   `/env /execute /verify`（容器型）或 `/env /execute /tools /close`（MCP 型）；Pi 扩展（`*_extension_gen.py` 生成）把工具注册进 Pi，`execute()` 代理到 sidecar。判分只认 sidecar 在官方容器里跑官方 verifier 写出的 `reward.txt` / 官方提取器结果。
4. **Extension 直接调 docker**：Pi 扩展直接跑 docker CLI。可行但把机密的容器编排（构建/挂载/隔离）塞进 agent 进程，且追踪（生命周期日志）零散。

## 决策

采用 **方案 3：独立 HTTP sidecar 工具面**。每个基准一个 `code/*_env_server.py`（进程外、无状态到 episode、幂等），配合 `code/*_extension_gen.py` 生成的 Pi 扩展注册官方工具面。设计要点：

- **同构契约**：容器型（SkillsBench / SkillLearnBench）共享同一个 `skillsbench_env_server.py --profile {skillsbench|skilllearnbench}`——两者都是"官方环境 Dockerfile + verifier/test.sh 写 reward.txt"；MCP 型（EarthBench）用 `earthbench_env_server.py`（104 工具 + shim 直调官方 kit 函数）。Tau2 的双控 sidecar 是既有先例，本 ADR 将其推广为**基准通用的工具面形态**。
- **隔离**：环境键 = `task_id@work-<arm>/episodes/<seq>-<episode>`（双臂互不串），sidecar 按 episode 幂等建容器/环境。
- **fail-closed 判分**：容器型判分前 `rm /logs/verifier/reward.txt`，官方 test.sh 重写，缺/不可解析/verifier 崩（exit≠0）一律 0 并披露 `verifier_exit`；MCP 型用官方 end-to-end 提取器 + 官方 gt whitelist sha256 校验。
- **无泄漏**：staged `sb-task.json` / `slb-task.json` / `eb-task.json` 只含 id 三元组，不含答案；判分输入（verifier 脚本 / gt whitelist）永不回流 prompt。
- **环境适配（非基准改动）**：docker build/run 通过 `~/.docker/config.json` proxies（`http://172.17.0.1:7893`）走本地代理，解决 jax 等大依赖构建直连超时（>30m→~5m）；不改官方 Dockerfile。

## 后果

- **收益**：四基准都能用官方 verifier 判分（进程外、可横向缩放、双臂隔离、生命周期日志即判分真源）；共享 profile 让 SkillsBench/SkillLearnBench 的 sidecar 零重复。
- **代价/责任**：sidecar 是 Python 轻服务，需确保与官方 FastMCP/容器版本 pin 一致（vendored 快照 + commit pin）；扩展 `build()` 曾丢 room bridge 的模块级 `toolProxyURL` 常量（已修并用 `node --check` 兜底）——这类拼接 bug 要靠 `node --check` + oracle sanity 守卫。
- **能力边界**：GAIA 走既有 room-bridge（无工具面），无浏览/检索工具下指标被系统性压低，全量前需单独接入浏览器/检索（不属本 ADR 范围）。

## 相关

- `SkillLearnBench` 的 `skills/` 注入：容器型 sidecar 按官方 `evaluate_skills.py` 的 `_prepare_base_build_env` 语义注入**空 skills 存根**（base 镜像保持 hermetric；真实 baseline skill 属被测"方法"层，全量前按被测方法注入）。
- 判分 driver（`*_grading.py`）读 sidecar `/verdicts` 生命周期日志 → `/verify`，确保 verdict 只来自官方 verifier。
