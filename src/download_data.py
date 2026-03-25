from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

from config import DATA_RAW, OUTPUTS, PipelineConfig, ensure_directories

LOG_PATH = OUTPUTS / "download_log.csv"


def _safe_name(name: str) -> str:
    return name.replace("/", "_").replace(" ", "_").lower()


def fetch_url_to_raw(dataset: str, url: str) -> dict:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    folder = DATA_RAW / _safe_name(dataset)
    folder.mkdir(parents=True, exist_ok=True)
    out = folder / f"{_safe_name(dataset)}_{ts}.html"

    row = {
        "dataset": dataset,
        "source_url": url,
        "download_timestamp_utc": ts,
        "status": "failed",
        "file_path": "",
        "file_size_bytes": 0,
        "http_status": "",
        "content_type": "",
        "note": "",
    }

    req = Request(url, headers={"User-Agent": "property-sandbox-pipeline/1.0"})
    try:
        with urlopen(req, timeout=60) as resp:
            body = resp.read()
            content_type = resp.headers.get("Content-Type", "")
            if "json" in content_type.lower():
                out = out.with_suffix(".json")
            out.write_bytes(body)
            row.update({
                "status": "ok",
                "file_path": str(out),
                "file_size_bytes": out.stat().st_size,
                "http_status": getattr(resp, "status", 200),
                "content_type": content_type,
            })
    except HTTPError as exc:
        row["http_status"] = exc.code
        row["note"] = f"HTTPError: {exc.reason}"
    except URLError as exc:
        row["note"] = f"URLError: {exc.reason}"
    except Exception as exc:
        row["note"] = str(exc)

    return row


def run_downloads(cfg: PipelineConfig) -> list[dict]:
    ensure_directories()
    rows = [fetch_url_to_raw(name, url) for name, url in cfg.source_urls.items()]

    exists = LOG_PATH.exists()
    with LOG_PATH.open("a", encoding="utf-8", newline="") as f:
        fields = [
            "dataset", "source_url", "download_timestamp_utc", "status", "file_path",
            "file_size_bytes", "http_status", "content_type", "note",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return rows


if __name__ == "__main__":
    run_downloads(PipelineConfig())
