import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def short_tmp():
    """A temporary directory with a short path: Unix socket paths are limited to ~100 bytes,
    and pytest's `tmp_path` plus a socket directory exceeds that."""
    path = Path(tempfile.mkdtemp(prefix="ap-", dir="/tmp"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
