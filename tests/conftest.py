"""Add the parent directory to sys.path so Lambda modules can be imported."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
