package effectsinterrupt

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	SignalCooperative = "cooperative_abort"
	SignalTERM        = "SIGTERM"
	SignalKILL        = "SIGKILL"
)

// ProcessTree is a controllable (non-Pi) process group the coordinator
// signals after interrupt grace.
type ProcessTree interface {
	Signal(sig syscall.Signal) error
	WaitDead(timeout time.Duration) bool
	Alive() []int
	Signals() []string
	AliveAfter(signal string) []int
}

// FakeProcessTree is a real OS process group that is not Pi. Tests use it
// to prove grace → SIGTERM → SIGKILL reclaims every member, including
// children that ignore SIGTERM.
type FakeProcessTree struct {
	cmd     *exec.Cmd
	pgid    int
	pids    []int
	pidFile string

	mu         sync.Mutex
	signals    []string
	aliveAfter map[string][]int
}

// StartFakeTree launches a bash process group under workDir. When
// uncooperative is true, the leader and children ignore SIGTERM so only
// SIGKILL can reclaim the tree.
func StartFakeTree(workDir string, children int, uncooperative bool) (*FakeProcessTree, error) {
	if children < 0 {
		children = 0
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, err
	}
	pidFile := filepath.Join(workDir, "pids")
	scriptPath := filepath.Join(workDir, "tree.sh")
	script := `#!/bin/bash
set -eu
PIDFILE="$1"
CHILDREN="$2"
UNCOOP="$3"
if [ "$UNCOOP" = "1" ]; then
  trap '' TERM
fi
echo $$ > "$PIDFILE"
n=0
while [ "$n" -lt "$CHILDREN" ]; do
  n=$((n+1))
  if [ "$UNCOOP" = "1" ]; then
    bash -c 'trap "" TERM; echo $$ >> "'"$PIDFILE"'"; while true; do sleep 0.05; done' &
  else
    bash -c 'echo $$ >> "'"$PIDFILE"'"; while true; do sleep 0.05; done' &
  fi
done
while true; do sleep 0.05; done
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		return nil, err
	}
	uncoop := "0"
	if uncooperative {
		uncoop = "1"
	}
	cmd := exec.Command("bash", scriptPath, pidFile, strconv.Itoa(children), uncoop)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = workDir
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, err
	}
	tree := &FakeProcessTree{
		cmd:        cmd,
		pgid:       pgid,
		pidFile:    pidFile,
		aliveAfter: map[string][]int{},
	}
	want := 1 + children
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pids, readErr := readPIDs(pidFile)
		if readErr == nil && len(pids) >= want && allAlive(pids) {
			tree.pids = pids
			return tree, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	tree.Kill()
	return nil, fmt.Errorf("effects interrupt: fake process tree did not publish %d live pids", want)
}

// Signal delivers sig to the whole process group and snapshots who is
// still alive shortly afterwards.
func (t *FakeProcessTree) Signal(sig syscall.Signal) error {
	if t == nil || t.pgid == 0 {
		return nil
	}
	name := signalName(sig)
	err := syscall.Kill(-t.pgid, sig)
	time.Sleep(15 * time.Millisecond)
	if sig == syscall.SIGKILL {
		t.reap()
	}
	alive := t.Alive()
	t.mu.Lock()
	t.signals = append(t.signals, name)
	t.aliveAfter[name] = append([]int(nil), alive...)
	t.mu.Unlock()
	return err
}

// WaitDead waits up to timeout for every recorded PID to exit.
func (t *FakeProcessTree) WaitDead(timeout time.Duration) bool {
	if t == nil {
		return true
	}
	deadline := time.Now().Add(timeout)
	for {
		t.reap()
		if len(t.Alive()) == 0 {
			return true
		}
		if !time.Now().Before(deadline) {
			return len(t.Alive()) == 0
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Alive returns recorded PIDs that still exist.
func (t *FakeProcessTree) Alive() []int {
	if t == nil {
		return nil
	}
	var live []int
	for _, pid := range t.snapshotPIDs() {
		if processAlive(pid) {
			live = append(live, pid)
		}
	}
	return live
}

func (t *FakeProcessTree) Signals() []string {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.signals))
	copy(out, t.signals)
	return out
}

func (t *FakeProcessTree) AliveAfter(signal string) []int {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]int(nil), t.aliveAfter[signal]...)
}

// Kill force-reclaims the group. Tests register it as cleanup.
func (t *FakeProcessTree) Kill() {
	if t == nil || t.pgid == 0 {
		return
	}
	_ = syscall.Kill(-t.pgid, syscall.SIGKILL)
	t.reap()
}

func (t *FakeProcessTree) reap() {
	if t.cmd != nil && t.cmd.Process != nil {
		var status syscall.WaitStatus
		_, _ = syscall.Wait4(t.cmd.Process.Pid, &status, syscall.WNOHANG, nil)
		go func() { _, _ = t.cmd.Process.Wait() }()
	}
}

func (t *FakeProcessTree) snapshotPIDs() []int {
	if pids, err := readPIDs(t.pidFile); err == nil && len(pids) > 0 {
		t.mu.Lock()
		t.pids = pids
		t.mu.Unlock()
		return pids
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]int(nil), t.pids...)
}

func readPIDs(path string) ([]int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pids []int
	seen := map[int]struct{}{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		pid, err := strconv.Atoi(line)
		if err != nil || pid <= 0 {
			continue
		}
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		pids = append(pids, pid)
	}
	return pids, nil
}

func allAlive(pids []int) bool {
	for _, pid := range pids {
		if !processAlive(pid) {
			return false
		}
	}
	return len(pids) > 0
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil
}

func signalName(sig syscall.Signal) string {
	switch sig {
	case syscall.SIGTERM:
		return SignalTERM
	case syscall.SIGKILL:
		return SignalKILL
	default:
		return sig.String()
	}
}
