"""Shared pytest configuration for cross-repository integration tracers."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

SPEC_ROOT = Path(__file__).resolve().parents[2]
