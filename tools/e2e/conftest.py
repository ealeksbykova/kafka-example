"""Make the harness modules importable from the test module and from a plain `pytest` run."""

import sys
from pathlib import Path

E2E_DIR = Path(__file__).resolve().parent

if str(E2E_DIR) not in sys.path:
    sys.path.insert(0, str(E2E_DIR))
