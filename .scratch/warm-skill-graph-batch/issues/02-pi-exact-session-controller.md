# 02 — PF-02 — 把 one-shot Pi process 扩展为可控 exact-session controller

**What to build:** Host 可以启动并识别 exact Pi session，保持 RPC 全双工控制，查询 state、abort、等待 settled，并在不改变 legacy one-shot turn 行为的情况下为后续 exact-session resume 提供 seam。

**Blocked by:** None — can start immediately.

**Status:** resolved

- [x] Controller 暴露 exact session file/id、process generation 和 settled state。
- [x] Agent running 时可发送 `get_state/clear_queue/abort`，stdout 只有一个 reader。
- [x] Launcher 能使用 exact `--session`，禁止用交互 `--resume` 或模糊 `--continue`。
- [x] Legacy launcher flags、one-shot runtime 行为与现有测试保持不变。
- [x] Race test 证明 RPC writer、response correlation 和 event stream 无并发读写竞争。

## Comments
模块：`pi-group-chat-host/internal/pi/sessionctrl/`（包 `sessionctrl`）；launcher seam：`Launcher.StartExactSession`（`pi-group-chat-host/internal/pi/launcher.go`）。未改 `Process.Prompt`、`internal/runtime/**`、`cmd/bench-runner/**`、`graph-memory-service/**`。

验证：`GOCACHE=/tmp/wsgb-pf02-go-cache $HOME/go/bin/go test ./internal/pi/... -count=1`；`GOCACHE=/tmp/wsgb-pf02-go-cache $HOME/go/bin/go test -race ./internal/pi/sessionctrl/ -count=1`。
