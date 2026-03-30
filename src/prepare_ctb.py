from __future__ import annotations

import csv
from pathlib import Path

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, write_parquet_placeholder

# Column indices for the CTB empty properties CSV (Table 5.08)
# The file has 5 header rows, row 6 is column headers, data starts row 7.
# Fixed columns: 0=E-code, 1=ONS Code, 2=Region, 3=Local Authority, 4=Notes
# Table 5.08 ("total including dwellings receiving no discount") Band H is column 88.
_HEADER_ROWS = 5
_COL_ECODE = 0
_COL_ONS = 1
_COL_REGION = 2
_COL_LA = 3
_COL_T508_BAND_H = 88
_COL_T508_TOTAL = 89


def prepare_ctb_empty(cfg: PipelineConfig) -> dict:
    """Parse CTB Table 5.08 to extract Band H empty property counts by LA.

    Returns dict with keys: national_band_h_empty, by_la (dict of LA → count).
    """
    folder = DATA_RAW / "voa"
    ctb_files = sorted([
        *folder.glob("ctb*.csv"),
        *folder.glob("CTB*.csv"),
    ])

    if not ctb_files:
        print("INFO: No CTB file found in data/raw/voa/. Skipping empty property analysis.")
        return {"national_band_h_empty": 0, "by_la": {}}

    by_la: dict[str, int] = {}
    national_h = 0
    national_all = 0

    for path in ctb_files:
        with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                # Skip header rows and column header row
                if i <= _HEADER_ROWS:
                    continue
                if len(row) <= _COL_T508_BAND_H:
                    continue

                la = row[_COL_LA].strip()
                region = row[_COL_REGION].strip()
                if not la or la == "Local Authority":
                    continue
                # Skip aggregate rows (England, regions)
                if region in ("ENG", "") and la in ("England", "Wales"):
                    # Capture national total
                    try:
                        national_h = int(row[_COL_T508_BAND_H].replace(",", "").strip() or "0")
                        national_all = int(row[_COL_T508_TOTAL].replace(",", "").strip() or "0")
                    except (ValueError, IndexError):
                        pass
                    continue
                if la.startswith("TOTAL") or la.startswith("Region"):
                    continue

                try:
                    h_empty = int(row[_COL_T508_BAND_H].replace(",", "").strip() or "0")
                except (ValueError, IndexError):
                    h_empty = 0

                by_la[clean_text(la)] = h_empty

    result = {
        "national_band_h_empty": national_h,
        "national_all_empty": national_all,
        "by_la": by_la,
    }
    print(f"CTB: {national_h:,} empty Band H properties nationally, {len(by_la)} LAs.")

    # Save for downstream use
    rows = [{"la": k, "band_h_empty": v} for k, v in by_la.items()]
    write_parquet_placeholder(DATA_INTERIM / "ctb_band_h_empty.parquet", rows)

    return result


if __name__ == "__main__":
    print(prepare_ctb_empty(PipelineConfig()))
