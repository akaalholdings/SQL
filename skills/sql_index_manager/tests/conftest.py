from __future__ import annotations

import pathlib
import sys

SKILL_DIR = pathlib.Path(__file__).resolve().parent.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))
