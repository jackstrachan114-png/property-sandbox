from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen

from config import DATA_RAW, OUTPUTS, PipelineConfig, ensure_directories

LOG_PATH = OUTPUTS / "download_log.csv"
ALLOWED_EXTENSIONS = (".csv", ".zip", ".xls", ".xlsx", ".json")


class DownloadDiscoveryError(RuntimeError):
    """Raised when a strict dataset page has no downloadable links."""


def _safe_name(name: str) -> str:
    return name.replace("/", "_").replace(" ", "_").lower()


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _fetch_bytes(url: str) -> tuple[bytes, str, int]:
    req = Request(url, headers={"User-Agent": "property-sandbox-pipeline/1.0"})
    with urlopen(req, timeout=90) as resp:
        return resp.read(), resp.headers.get("Content-Type", ""), getattr(resp, "status", 200)


def _extract_links(html: str, base_url: str) -> list[str]:
    raw_links = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    links = []
    for href in raw_links:
        if href.startswith("mailto:"):
            continue
        links.append(urljoin(base_url, href))

    # Deduplicate while preserving order
    seen, deduped = set(), []
    for link in links:
        if link not in seen:
            deduped.append(link)
            seen.add(link)
    return deduped


def _looks_like_download(url: str, keywords: tuple[str, ...]) -> bool:
    low = url.lower()

    if any(low.endswith(ext) for ext in ALLOWED_EXTENSIONS):
        return True

    parsed = urlparse(low)
    query = parse_qs(parsed.query)

    path_hint = any(token in parsed.path for token in ("download", "attachment", "dataset"))
    query_hint = any(token in query for token in ("file", "format", "download"))
    keyword_hint = any(k in low for k in keywords)

    return (path_hint or query_hint) and keyword_hint


def _filename_for_url(url: str, fallback_prefix: str) -> str:
    path_name = Path(urlparse(url).path).name
    if path_name and any(path_name.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS):
        return path_name
    digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
    return f"{fallback_prefix}_{digest}.dat"


def _manifest_path(folder: Path) -> Path:
    return folder / "download_manifest.json"


def _load_manifest(folder: Path) -> dict[str, str]:
    path = _manifest_path(folder)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _save_manifest(folder: Path, manifest: dict[str, str]) -> None:
    _manifest_path(folder).write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _landing_row(dataset: str, source_url: str, file_path: str) -> dict:
    return {
        "dataset": dataset,
        "entry_type": "landing_page",
        "source_url": source_url,
        "download_timestamp_utc": _utc_timestamp(),
        "status": "failed",
        "file_path": file_path,
        "file_size_bytes": 0,
        "http_status": "",
        "content_type": "",
        "note": "",
    }


def _fetch_and_save_landing_page(dataset: str, page_url: str, folder: Path) -> tuple[str, dict]:
    landing_path = folder / f"{_utc_timestamp()}_landing.html"
    row = _landing_row(dataset, page_url, str(landing_path))

    body, content_type, http_status = _fetch_bytes(page_url)
    landing_path.write_bytes(body)
    row.update(
        {
            "status": "ok",
            "file_size_bytes": landing_path.stat().st_size,
            "http_status": http_status,
            "content_type": content_type,
        }
    )
    return body.decode("utf-8", errors="ignore"), row


def _download_discovered_files(dataset: str, links: list[str], folder: Path) -> list[dict]:
    manifest = _load_manifest(folder)
    rows: list[dict] = []

    for idx, link in enumerate(links, start=1):
        filename = _filename_for_url(link, f"{dataset}_{idx:03d}")
        out_path = folder / filename
        row = {
            "dataset": dataset,
            "entry_type": "dataset_file",
            "source_url": link,
            "download_timestamp_utc": _utc_timestamp(),
            "status": "failed",
            "file_path": str(out_path),
            "file_size_bytes": 0,
            "http_status": "",
            "content_type": "",
            "note": "",
        }

        # URL-level dedupe
        prior_name = manifest.get(link)
        if prior_name and (folder / prior_name).exists():
            prior_path = folder / prior_name
            row.update(
                {
                    "status": "skipped_duplicate",
                    "file_path": str(prior_path),
                    "file_size_bytes": prior_path.stat().st_size,
                    "note": "URL already downloaded in previous run.",
                }
            )
            rows.append(row)
            continue

        try:
            body, content_type, http_status = _fetch_bytes(link)
            out_path.write_bytes(body)
            manifest[link] = out_path.name
            row.update(
                {
                    "status": "ok",
                    "file_size_bytes": out_path.stat().st_size,
                    "http_status": http_status,
                    "content_type": content_type,
                }
            )
        except HTTPError as exc:
            row["http_status"] = exc.code
            row["note"] = f"HTTPError: {exc.reason}"
        except URLError as exc:
            row["note"] = f"URLError: {exc.reason}"
        except Exception as exc:
            row["note"] = str(exc)

        rows.append(row)

    _save_manifest(folder, manifest)
    return rows


def _process_dataset_page(
    dataset: str,
    page_url: str,
    keywords: tuple[str, ...],
    limit: int,
    strict: bool,
) -> list[dict]:
    folder = DATA_RAW / _safe_name(dataset)
    folder.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []

    try:
        html, landing_row = _fetch_and_save_landing_page(dataset, page_url, folder)
        rows.append(landing_row)

        all_links = _extract_links(html, page_url)
        data_links = [u for u in all_links if _looks_like_download(u, keywords)]

        landing_row["note"] = f"Discovered {len(data_links)} downloadable links; limit={limit}."

        if not data_links:
            message = f"No downloadable links found for {dataset} ({page_url})."
            rows.append(
                {
                    "dataset": dataset,
                    "entry_type": "discovery_error",
                    "source_url": page_url,
                    "download_timestamp_utc": _utc_timestamp(),
                    "status": "failed",
                    "file_path": "",
                    "file_size_bytes": 0,
                    "http_status": "",
                    "content_type": "",
                    "note": message,
                }
            )
            if strict:
                raise DownloadDiscoveryError(message)
            return rows

        selected_links = data_links[:limit]
        rows.extend(_download_discovered_files(dataset, selected_links, folder))

    except Exception as exc:
        rows.append(
            {
                "dataset": dataset,
                "entry_type": "discovery_error",
                "source_url": page_url,
                "download_timestamp_utc": _utc_timestamp(),
                "status": "failed",
                "file_path": "",
                "file_size_bytes": 0,
                "http_status": "",
                "content_type": "",
                "note": str(exc),
            }
        )
        if strict:
            raise

    return rows


def _download_land_property_api(cfg: PipelineConfig) -> list[dict]:
    dataset = "land_property_api"
    folder = DATA_RAW / dataset
    folder.mkdir(parents=True, exist_ok=True)

    out_path = folder / f"{_utc_timestamp()}_api_root.json"
    row = {
        "dataset": dataset,
        "entry_type": "dataset_file",
        "source_url": cfg.source_urls["land_property_api"],
        "download_timestamp_utc": _utc_timestamp(),
        "status": "failed",
        "file_path": str(out_path),
        "file_size_bytes": 0,
        "http_status": "",
        "content_type": "",
        "note": "",
    }

    try:
        body, content_type, http_status = _fetch_bytes(cfg.source_urls["land_property_api"])
        out_path.write_bytes(body)
        row.update(
            {
                "status": "ok",
                "file_size_bytes": out_path.stat().st_size,
                "http_status": http_status,
                "content_type": content_type,
            }
        )
    except Exception as exc:
        row["note"] = str(exc)

    return [row]


def _write_log(rows: list[dict]) -> None:
    fields = [
        "dataset",
        "entry_type",
        "source_url",
        "download_timestamp_utc",
        "status",
        "file_path",
        "file_size_bytes",
        "http_status",
        "content_type",
        "note",
    ]
    with LOG_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def run_downloads(cfg: PipelineConfig) -> list[dict]:
    """Run two-stage downloader for core and contextual sources.

    Stage 1: fetch and save landing page.
    Stage 2: parse and download actual data links.
    """
    ensure_directories()
    rows: list[dict] = []

    # Core sources (strict for PPD + UKHPI, best-effort for EPC)
    rows.extend(
        _process_dataset_page(
            dataset="price_paid",
            page_url=cfg.source_urls["price_paid"],
            keywords=("price", "paid", "landregistry", "pp-"),
            limit=cfg.ppd_download_limit,
            strict=True,
        )
    )
    rows.extend(
        _process_dataset_page(
            dataset="ukhpi",
            page_url=cfg.source_urls["ukhpi"],
            keywords=("hpi", "house", "price", "ukhpi"),
            limit=cfg.ukhpi_download_limit,
            strict=True,
        )
    )
    rows.extend(
        _process_dataset_page(
            dataset="epc",
            page_url=cfg.source_urls["epc_collection"],
            keywords=("epc", "energy", "performance", "certificate"),
            limit=cfg.epc_download_limit,
            strict=False,
        )
    )

    # API handled separately
    rows.extend(_download_land_property_api(cfg))

    # Contextual pages (best-effort one file each)
    contextual_names = [
        "land_property_api_info",
        "land_property_portal",
        "os_gb_address",
        "planning_data",
        "dwelling_stock",
        "rents_lettings",
        "ehs_tables",
        "house_building",
        "fire_stats",
    ]
    for name in contextual_names:
        rows.extend(
            _process_dataset_page(
                dataset=name,
                page_url=cfg.source_urls[name],
                keywords=("csv", "zip", "xls", "xlsx", "json"),
                limit=1,
                strict=False,
            )
        )

    _write_log(rows)
    return rows


if __name__ == "__main__":
    run_downloads(PipelineConfig())
