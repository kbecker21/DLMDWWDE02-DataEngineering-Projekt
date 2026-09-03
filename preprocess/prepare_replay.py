"""Merge the per-station parquet files into one file sorted globally by Start.

The replay producer streams this file sequentially, so events leave in
event-time order across all stations and pollutants.
"""

import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

IN_DIR = os.environ.get("IN_DIR", "/input")
OUT_FILE = os.environ.get("OUT_FILE", "/output/events.parquet")
COLUMNS = ["Samplingpoint", "Pollutant", "Start", "End", "Value", "Unit", "Validity", "Verification"]
ROW_GROUP_SIZE = 500_000


def main():
    files = sorted(str(p) for p in Path(IN_DIR).rglob("*.parquet"))
    if not files:
        print(f"No parquet files under {IN_DIR}. Run the downloader first or point DATA_DIR at data/sample.")
        return 1

    t0 = time.time()
    table = ds.dataset(files, format="parquet").to_table(columns=COLUMNS)
    print(f"Read {len(files)} files, {table.num_rows:,} rows", flush=True)

    table = table.sort_by([("Start", "ascending"), ("Samplingpoint", "ascending")])
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    pq.write_table(table, OUT_FILE, row_group_size=ROW_GROUP_SIZE, compression="zstd")

    start = pq.read_table(OUT_FILE, columns=["Start"])["Start"].combine_chunks()
    ticks = start.cast(pa.int64())
    steps = pc.subtract(ticks.slice(1), ticks.slice(0, len(ticks) - 1))
    monotonic = len(ticks) < 2 or pc.min(steps).as_py() >= 0

    size_mb = os.path.getsize(OUT_FILE) / 1e6
    print(f"Wrote {OUT_FILE}: {table.num_rows:,} rows, {size_mb:.0f} MB, "
          f"{start[0]} .. {start[-1]}, sorted={monotonic}, {time.time() - t0:.0f}s")
    return 0 if monotonic else 1


if __name__ == "__main__":
    sys.exit(main())
