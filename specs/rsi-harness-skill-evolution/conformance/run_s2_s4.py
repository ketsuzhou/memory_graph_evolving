#!/usr/bin/env python3
"""INT-002 S2-S4 tracer -- entry point.

The tracer implementation lives in ``integration/run_s2_s4.py`` next to the
INT-001 harness; this launcher only delegates so the documented one-line
verification command

    python3 .../conformance/run_s2_s4.py --host ... --gms ... --rsih ... \
        --fixtures .../conformance

keeps working from the conformance root. All argument parsing, driver
spawning and evidence writing happen in the delegated module.
"""

import importlib.util
import sys
from pathlib import Path

_IMPL = Path(__file__).resolve().parent / "integration" / "run_s2_s4.py"

_spec = importlib.util.spec_from_file_location("int002_run_s2_s4_impl", _IMPL)
_impl = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _impl
_spec.loader.exec_module(_impl)

if __name__ == "__main__":
    sys.exit(_impl.main())
