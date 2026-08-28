"""One-off downloader for the EEA E1a dataset (Germany, 2024, hourly NO2/O3/PM10/PM2.5).

Tries the EEA download API, then the public blob container, then the project's
release mirror. Only row groups overlapping 2024 are fetched (HTTP range reads);
re-running skips stations that are already on disk.
"""

import os
import re
import sys
import time
import threading
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

API_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls"
BLOB_BASE = "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-e1a"

# EEA truncates pollutant tokens in blob names (PM10 -> "PM1", PM2.5 -> "PM2"),
# so files are routed into output dirs by the Pollutant code inside the file.
FILE_RE = re.compile(r"DE/SPO\.DE_([A-Z0-9]+)_(NO2|O3|PM1|PM2)_dataGroup1\.parquet$")
CODE_DIR = {8: "NO2", 7: "O3", 5: "PM10", 6001: "PM25"}
TOKEN_DIR = {"NO2": "NO2", "O3": "O3", "PM1": "PM10", "PM2": "PM25"}

YEAR_START = pd.Timestamp("2024-01-01")
YEAR_END = pd.Timestamp("2025-01-01")
EXPECTED_MIN_FILES = 1300  # the full 2024 cut has 1,380 station files

OUT_DIR = os.environ.get("OUT_DIR", "/data/eea_e1a_2024")
MIRROR_URL = os.environ.get(
    "EEA_MIRROR_URL",
    "https://github.com/kbecker21/DLMDWWDE02-DataEngineering-Projekt"
    "/releases/download/data-v1/eea_e1a_2024.zip",
)
WORKERS = int(os.environ.get("DOWNLOAD_WORKERS", "8"))

session_local = threading.local()
transferred = {"bytes": 0}
tlock = threading.Lock()


def get_session():
    if not hasattr(session_local, "s"):
        session_local.s = requests.Session()
    return session_local.s


class LazyHTTPFile:
    """Random-access read-only file over HTTP using Range requests."""

    def __init__(self, url, size):
        self.url = url
        self._size = size
        self._pos = 0
        self.closed = False

    def size(self):
        return self._size

    def tell(self):
        return self._pos

    def seek(self, pos, whence=0):
        if whence == 0:
            self._pos = pos
        elif whence == 1:
            self._pos += pos
        else:
            self._pos = self._size + pos
        return self._pos

    def read(self, nbytes=-1):
        if nbytes is None or nbytes < 0:
            nbytes = self._size - self._pos
        if nbytes == 0:
            return b""
        end = min(self._pos + nbytes, self._size) - 1
        r = get_session().get(self.url, headers={"Range": f"bytes={self._pos}-{end}"}, timeout=60)
        r.raise_for_status()
        data = r.content
        with tlock:
            transferred["bytes"] += len(data)
        self._pos += len(data)
        return data

    def close(self):
        self.closed = True

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    def flush(self):
        pass


def list_via_api():
    payload = {
        "countries": ["DE"],
        "cities": [],
        "pollutants": ["NO2", "O3", "PM10", "PM2.5"],
        "dataset": 2,  # E1a (validated)
        "source": "API",
    }
    r = requests.post(API_URL, json=payload, timeout=90)
    r.raise_for_status()
    urls = re.findall(r"https?://\S+?\.parquet", r.text)
    targets = []
    for url in urls:
        path = url.split(".net/", 1)[-1].split("/", 1)[-1]
        if FILE_RE.search(path):
            targets.append((url, path, None))  # size resolved later via HEAD
    return targets


def list_via_blob():
    targets, marker = [], None
    while True:
        params = {"restype": "container", "comp": "list", "prefix": "DE/", "maxresults": "5000"}
        if marker:
            params["marker"] = marker
        r = requests.get(BLOB_BASE, params=params, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for b in root.iter("Blob"):
            name = b.findtext("Name")
            if FILE_RE.search(name):
                size = int(b.find("Properties").findtext("Content-Length"))
                targets.append((f"{BLOB_BASE}/{name}", name, size))
        marker = root.findtext("NextMarker")
        if not marker:
            break
    return targets


def fetch_2024(url, size):
    """Read row groups from the end until rows are older than 2024; return the 2024 rows."""
    if size is None:
        h = get_session().head(url, timeout=60)
        h.raise_for_status()
        size = int(h.headers["Content-Length"])
    f = pq.ParquetFile(pa.PythonFile(LazyHTTPFile(url, size), mode="r"))
    parts = []
    for i in range(f.metadata.num_row_groups - 1, -1, -1):
        df = f.read_row_group(i).to_pandas()
        sel = df[(df["Start"] >= YEAR_START) & (df["Start"] < YEAR_END)]
        if len(sel):
            parts.append(sel)
        if df["Start"].min() < YEAR_START:
            break
    if not parts:
        return None
    return pd.concat(parts, ignore_index=True).sort_values("Start")


def count_parquet(root):
    n = 0
    for _, _, files in os.walk(root):
        n += sum(1 for f in files if f.endswith(".parquet"))
    return n


def download_targets(targets, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    done = skipped = empty = errors = 0
    rows_total = 0
    lock = threading.Lock()

    def work(target):
        nonlocal done, skipped, empty, errors, rows_total
        url, name, size = target
        m = FILE_RE.search(name)
        station, token = m.group(1), m.group(2)
        try:
            out_guess = os.path.join(out_dir, TOKEN_DIR[token], f"{station}.parquet")
            if os.path.exists(out_guess):
                with lock:
                    skipped += 1
                return
            df = fetch_2024(url, size)
        except Exception as e:
            with lock:
                errors += 1
            print(f"ERROR {name}: {e}", flush=True)
            return
        with lock:
            if df is None or len(df) == 0:
                empty += 1  # station stopped reporting before 2024
            else:
                code = int(df["Pollutant"].iloc[0])
                if code not in CODE_DIR:
                    print(f"SKIPPED (unexpected pollutant code {code}): {name}", flush=True)
                else:
                    pdir = os.path.join(out_dir, CODE_DIR[code])
                    os.makedirs(pdir, exist_ok=True)
                    df.to_parquet(os.path.join(pdir, f"{station}.parquet"), index=False)
                    rows_total += len(df)
            done += 1
            if done % 100 == 0:
                mb = transferred["bytes"] / 1e6
                print(f"{done}/{len(targets)} processed | {rows_total:,} rows 2024 | "
                      f"{mb:.0f} MB transferred", flush=True)

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(as_completed(ex.submit(work, t) for t in targets))
    mb = transferred["bytes"] / 1e6
    print(f"\nDone in {time.time() - t0:.0f}s: {done} processed, {skipped} skipped (resume), "
          f"{empty} without 2024 data, {errors} errors", flush=True)
    print(f"2024 rows this session: {rows_total:,} | transferred: {mb:.0f} MB", flush=True)
    attempted = done + errors
    return attempted > 0 and errors / attempted < 0.1


def download_mirror(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    tmp = os.path.join(os.path.dirname(out_dir.rstrip("/")) or ".", "eea_e1a_2024_mirror.zip")
    print(f"Downloading mirror {MIRROR_URL} ...", flush=True)
    with get_session().get(MIRROR_URL, stream=True, timeout=120) as r:
        r.raise_for_status()
        got = 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                got += len(chunk)
                if got % (50 << 20) < (1 << 20):
                    print(f"  {got / 1e6:.0f} MB ...", flush=True)
    print("Extracting ...", flush=True)
    with zipfile.ZipFile(tmp) as z:
        z.extractall(out_dir)
    os.remove(tmp)


def summarize(out_dir):
    total_rows = total_files = 0
    for pdir in sorted(CODE_DIR.values()):
        full = os.path.join(out_dir, pdir)
        if not os.path.isdir(full):
            continue
        files = [f for f in os.listdir(full) if f.endswith(".parquet")]
        rows = sum(pq.ParquetFile(os.path.join(full, f)).metadata.num_rows for f in files)
        print(f"  {pdir}: {len(files)} stations, {rows:,} rows")
        total_files += len(files)
        total_rows += rows
    print(f"  TOTAL: {total_files} files, {total_rows:,} measurement rows")


def main():
    existing = count_parquet(OUT_DIR)
    if existing >= EXPECTED_MIN_FILES:
        print(f"Found {existing} parquet files in {OUT_DIR}, nothing to do.")
        summarize(OUT_DIR)
        return 0

    targets = None
    for label, lister in (("EEA download API", list_via_api),
                          ("blob container listing", list_via_blob)):
        try:
            found = lister()
            if found:
                print(f"Source: {label} ({len(found)} candidate files)", flush=True)
                targets = found
                break
            print(f"{label}: returned no files, trying next source", flush=True)
        except Exception as e:
            print(f"{label} failed ({e}), trying next source", flush=True)

    if targets and download_targets(targets, OUT_DIR):
        summarize(OUT_DIR)
        return 0

    print("EEA sources unavailable, falling back to the release mirror", flush=True)
    try:
        download_mirror(OUT_DIR)
        summarize(OUT_DIR)
        return 0
    except Exception as e:
        print(f"Mirror download failed: {e}")
        print("The sample in data/sample/ works without any download, see README.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
