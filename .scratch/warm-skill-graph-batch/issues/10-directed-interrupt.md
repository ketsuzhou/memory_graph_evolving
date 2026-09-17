# 10 — TB-08 — Structured offer 原子投递并中断/续跑 exact task session

**What to build:** Skill Offer 用结构化 target 原子写 Room message + unique Delivery，安全停止 running task，并用同一 exact session 恢复；中断期间到达的 messages 一次有序注入，创建 continuation Segment。

**Blocked by:** 02 — PF-02 — 把 one-shot Pi process 扩展为可控 exact-session controller; 08 — TB-06 — Task 与 opening-only Memory Agent 在空图上真实并发

**Status:** ready-for-agent

- [ ] 自由文本 `@agent` 不路由；structured target 才创建 Delivery。
- [ ] 同 key/body 重放返回原 message/delivery；同 key不同 body 冲突。
- [ ] 两个并发 mentions 只触发一次 abort，按 Room sequence 一次 resume 注入。
- [ ] Resume 后 session ID/file 与中断前一致，已完成 tool results 不重放。
- [ ] 新 Segment 正确记录 `continuation_of` 和 directed-mention reason。

## Comments
