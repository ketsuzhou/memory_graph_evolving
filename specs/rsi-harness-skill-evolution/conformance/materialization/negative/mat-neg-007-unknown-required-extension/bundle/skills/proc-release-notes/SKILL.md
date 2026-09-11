---
name: proc-release-notes
version: 4
kind: human_procedure
---

# Release notes procedure

1. Collect merged PRs since the last tag.
2. Group them by conventional-commit type.
3. Emit the CHANGELOG section in deterministic (type, then id) order.
