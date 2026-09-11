---
name: proc-release-notes
version: 5
kind: human_procedure
---

# Release notes procedure

1. Collect merged PRs since the last tag.
2. Group them by conventional-commit type.
3. Emit the CHANGELOG section in deterministic (type, then id) order.
4. Fail closed when any PR lacks a conventional-commit type.
