"""Test streaming JSONL reader."""
import json
import sys, os, tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from io_utils import iter_parquet_placeholder, count_parquet_placeholder, read_parquet_placeholder


def test_iter_matches_read():
    """Streaming reader should yield the same rows as the batch reader."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".parquet", delete=False) as f:
        for i in range(5):
            f.write(json.dumps({"id": i, "value": f"row_{i}"}) + "\n")
        path = Path(f.name)

    try:
        batch = read_parquet_placeholder(path)
        streamed = list(iter_parquet_placeholder(path))
        assert batch == streamed
    finally:
        path.unlink()


def test_iter_missing_file():
    """Missing file should yield nothing."""
    assert list(iter_parquet_placeholder(Path("/nonexistent.parquet"))) == []


def test_count_matches_len():
    """Count should match len of batch read."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".parquet", delete=False) as f:
        for i in range(7):
            f.write(json.dumps({"id": i}) + "\n")
        path = Path(f.name)

    try:
        assert count_parquet_placeholder(path) == 7
        assert count_parquet_placeholder(path) == len(read_parquet_placeholder(path))
    finally:
        path.unlink()


def test_count_missing_file():
    """Missing file should return 0."""
    assert count_parquet_placeholder(Path("/nonexistent.parquet")) == 0
