"""Merge and sort step, run against the built-in sample."""

import importlib

import pyarrow.parquet as pq

from conftest import SAMPLE_DIR


def run(monkeypatch, in_dir, out_file):
    monkeypatch.setenv("IN_DIR", str(in_dir))
    monkeypatch.setenv("OUT_FILE", str(out_file))
    import prepare_replay

    importlib.reload(prepare_replay)  # paths are read at import
    return prepare_replay.main(), prepare_replay


def test_merges_and_sorts_sample(tmp_path, monkeypatch):
    out = tmp_path / "replay" / "events.parquet"
    rc, mod = run(monkeypatch, SAMPLE_DIR, out)
    assert rc == 0

    table = pq.read_table(out)
    inputs = list(SAMPLE_DIR.rglob("*.parquet"))
    assert len(inputs) == 16
    assert table.num_rows == sum(pq.read_metadata(f).num_rows for f in inputs)
    assert table.column_names == mod.COLUMNS

    rows = table.select(["Start", "Samplingpoint"]).to_pylist()
    keys = [(r["Start"], r["Samplingpoint"]) for r in rows]
    assert keys == sorted(keys)
    assert rows[0]["Start"].year == 2024 and rows[-1]["Start"].year == 2024


def test_fails_without_input(tmp_path, monkeypatch):
    empty = tmp_path / "empty"
    empty.mkdir()
    rc, _ = run(monkeypatch, empty, tmp_path / "events.parquet")
    assert rc == 1
    assert not (tmp_path / "events.parquet").exists()
