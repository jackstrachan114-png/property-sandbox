# Evidence Base Improvements Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve the evidence base for the owner-occupation estimate by expanding the PPD transaction window, using EPC UPRNs for deduplication, adding council tax discount data as a contextual signal, creating a validation framework, and documenting a data procurement roadmap.

**Architecture:** Tasks 1-3 modify data preparation and require a single pipeline re-run at the end (Task 6). Task 4 runs on pipeline output. Task 5 is documentation only. All tasks are independent of each other except the final re-run.

**Tech Stack:** Python 3 stdlib only (csv, json, zipfile, io, pathlib, random, statistics). No new dependencies.

---

## Feasibility Assessment

| Recommendation | Feasibility | Impact | Approach |
|----------------|-------------|--------|----------|
| Council tax SPD (property-level) | Not feasible | High | GDPR prevents property-level release. Use LA-level aggregate instead. |
| Electoral register | Not feasible without agreement | High | Restricted. Document procurement path. |
| UPRN linking (full, via AddressBase) | Requires licence (free via PSGA) | High | Use EPC UPRNs as partial substitute (97.6% coverage). |
| UPRN linking (via NPRN Title→UPRN) | £20K/year | Medium | Document as procurement option. |
| Expand PPD window | Implementable now | High | Download 15 yearly files instead of 5. |
| Validate against ground truth | Implementable now | High (credibility) | Create stratified sample for title inspections. |
| CT discount aggregate rates | Implementable now | Medium | Download CTB stats, use Band H SPD rates as contextual prior. |

---

## User action required before running

**Download needed:** The CTB (Council Tax Base) statistics must be downloaded manually because the gov.uk publication page requires navigating to the correct table. Visit:

> https://www.gov.uk/government/statistics/council-taxbase-2025-in-england

Download the **"Council Taxbase local authority level data"** table (Excel/CSV). This file contains SPD (single-person discount) counts by council tax band and local authority. Save it to `data/raw/voa/ctb_2025.csv` (or `.xlsx`).

All other data (expanded PPD, VOA CTSOP) is auto-downloadable.

---

### Task 1: Expand PPD download window to capture older transactions

Currently only 3 yearly files (2023-2025) are downloaded. Expanding to 15 files (roughly 2010-2025) will capture properties that transacted at £2M+ in earlier years — these are longer-held properties more likely to be owner-occupied, directly addressing the transaction bias identified in the stakeholder report.

**Files:**
- Modify: `src/config.py`

**Step 1: Change ppd_download_limit**

```python
# In PipelineConfig:
ppd_download_limit: int = 15  # was 5; captures ~2010-2025
```

**Step 2: Clear the PPD download manifest to force re-download**

The downloader skips URLs it's already fetched. To download additional yearly files:

```bash
rm data/raw/price_paid/download_manifest.json
```

**Step 3: Run the downloader**

```bash
cd /mnt/c/Users/court/mets/property-sandbox && python3 src/download_data.py
```

Expected: downloads ~12 additional yearly CSV files (pp-2010.csv through pp-2022.csv). Each is ~100-300MB.

**Step 4: Verify**

```bash
ls data/raw/price_paid/pp-20*.csv | wc -l
```

Expected: 15+ files.

**Step 5: Commit**

```bash
git add src/config.py
git commit -m "feat: expand PPD window to 15 yearly files to reduce transaction bias"
```

---

### Task 2: Capture EPC UPRN and use as primary dedup key

EPC records have a `UPRN` field with 97.6% coverage. UPRN is a unique identifier per addressable location — more reliable than BUILDING_REFERENCE_NUMBER for deduplication. Capturing it also future-proofs for UPRN-based cross-referencing if AddressBase or NPRN become available.

**Files:**
- Modify: `src/prepare_epc.py`

**Step 1: Add UPRN to the EPC output row**

In `prepare_epc()`, after extracting `brn`, add:

```python
        uprn = r.get("uprn") or ""
```

And update the `out.append({...})` to include:

```python
            "uprn": str(uprn).strip(),
```

**Step 2: Use UPRN as primary dedup key (before BRN)**

Replace the dedup block with:

```python
    # Deduplicate: keep latest EPC per property
    # Priority: UPRN (most reliable) > BRN > postcode+address
    dedup: dict[str, dict] = {}
    for rec in out:
        uprn = rec.get("uprn", "")
        brn = rec.get("building_reference_number", "")
        if uprn:
            key = f"uprn:{uprn}"
        elif brn:
            key = f"brn:{brn}"
        else:
            key = f"addr:{rec['postcode_clean']}|{rec['address_clean']}"
        existing = dedup.get(key)
        if not existing or rec.get("lodgement_date", "") > existing.get("lodgement_date", ""):
            dedup[key] = rec
    out = list(dedup.values())
    print(f"EPC: {len(out):,} unique properties after deduplication (UPRN/BRN/address).")
```

**Step 3: Verify**

```bash
cd /mnt/c/Users/court/mets/property-sandbox/src && python3 -c "
from prepare_epc import prepare_epc
from config import PipelineConfig
# Quick test with no postcode filter to check dedup works
result = prepare_epc(PipelineConfig(), candidate_postcodes={'sw1a1aa'})
for r in result[:3]:
    print(r.get('uprn', 'NO UPRN'), r.get('postcode_clean'))
"
```

**Step 4: Commit**

```bash
git add src/prepare_epc.py
git commit -m "feat: capture EPC UPRN and use as primary dedup key

UPRN has 97.6% coverage and is more reliable than BUILDING_REFERENCE_NUMBER
for deduplication. Stored for future cross-referencing."
```

---

### Task 3: Download and integrate CTB Band H single-person discount rates

The Council Tax Base (CTB) statistics include SPD (single-person discount) counts by council tax band and local authority. SPD indicates a single adult resides at the property — a strong occupancy signal. Band H SPD rates by LA provide a calibrated prior for uncertain cases.

**Files:**
- Create: `src/prepare_ctb.py`
- Modify: `src/config.py` — add CTB URL
- Modify: `src/sensitivity_analysis.py` — add SPD-based calibration scenario

**Step 1: Add CTB URL to config**

```python
        "ctb_stats": "https://www.gov.uk/government/statistics/council-taxbase-2025-in-england",
```

**Step 2: Create `src/prepare_ctb.py`**

This parser handles the CTB Excel/CSV file which has SPD counts by band and LA.

```python
from __future__ import annotations

import csv
import io
from pathlib import Path

from config import DATA_INTERIM, DATA_RAW, PipelineConfig
from io_utils import clean_text, write_parquet_placeholder


def prepare_ctb_spd(cfg: PipelineConfig) -> dict:
    """Parse CTB statistics to extract Band H single-person discount rates by LA.

    Returns dict with keys: national_band_h_spd_rate, by_la (dict of LA→rate).
    """
    folder = DATA_RAW / "voa"
    # Look for CTB file (user downloads manually)
    ctb_files = sorted([
        *folder.glob("ctb*.csv"),
        *folder.glob("CTB*.csv"),
        *folder.glob("ctb*.xlsx"),
        *folder.glob("CTB*.xlsx"),
    ])

    if not ctb_files:
        print(
            "INFO: No CTB file found in data/raw/voa/. "
            "Download from https://www.gov.uk/government/statistics/council-taxbase-2025-in-england "
            "and save as data/raw/voa/ctb_2025.csv"
        )
        return {"national_band_h_spd_rate": 0.0, "by_la": {}}

    # Parse CSV (Excel support would require openpyxl — not available)
    rows: list[dict] = []
    for path in ctb_files:
        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows.append({k.lower().strip(): v.strip() for k, v in r.items()})

    if not rows:
        print("INFO: CTB file found but no rows parsed. May need CSV format.")
        return {"national_band_h_spd_rate": 0.0, "by_la": {}}

    # Identify columns: look for LA name, Band H total, Band H with SPD
    sample = rows[0]
    la_col = None
    band_h_total_col = None
    band_h_spd_col = None

    for k in sample:
        kl = k.lower()
        if la_col is None and any(x in kl for x in ["authority", "la name", "area", "billing"]):
            la_col = k
        if "band h" in kl or "band_h" in kl or kl == "h":
            if "spd" in kl or "single" in kl or "discount" in kl:
                band_h_spd_col = k
            elif band_h_total_col is None:
                band_h_total_col = k

    if not (la_col and band_h_total_col):
        print(f"INFO: Could not identify CTB columns. Available: {list(sample.keys())[:15]}")
        return {"national_band_h_spd_rate": 0.0, "by_la": {}}

    by_la: dict[str, float] = {}
    total_h = 0
    total_spd = 0

    for r in rows:
        la = clean_text(r.get(la_col, ""))
        if not la:
            continue
        try:
            h_total = float(r.get(band_h_total_col, "0").replace(",", "").strip() or "0")
        except (ValueError, TypeError):
            continue
        h_spd = 0.0
        if band_h_spd_col:
            try:
                h_spd = float(r.get(band_h_spd_col, "0").replace(",", "").strip() or "0")
            except (ValueError, TypeError):
                pass

        if h_total > 0:
            by_la[la] = h_spd / h_total if band_h_spd_col else 0.0
            total_h += h_total
            total_spd += h_spd

    national_rate = total_spd / total_h if total_h else 0.0
    print(f"CTB: Band H SPD rate = {national_rate*100:.1f}% nationally, {len(by_la)} LAs.")

    # Save for downstream use
    spd_rows = [{"la": k, "band_h_spd_rate": v} for k, v in by_la.items()]
    write_parquet_placeholder(DATA_INTERIM / "ctb_band_h_spd.parquet", spd_rows)

    return {"national_band_h_spd_rate": national_rate, "by_la": by_la}


if __name__ == "__main__":
    print(prepare_ctb_spd(PipelineConfig()))
```

**Step 3: Add SPD scenario to sensitivity analysis**

In `src/sensitivity_analysis.py`, after the VOA block, add:

```python
    # CTB SPD calibration
    spd_rows = read_parquet_placeholder(DATA_INTERIM / "ctb_band_h_spd.parquet")
    if spd_rows:
        national_spd_rate = sum(float(r.get("band_h_spd_rate", 0)) for r in spd_rows) / len(spd_rows) if spd_rows else 0.0
        # SPD means single adult at property — strong owner-occupation signal
        # If X% of Band H have SPD, those are very likely owner-occupied
        # Remaining (1-X)% could be multi-adult owner-occupied or non-owner-occupied
        scenarios.append({"scenario": "ctb_spd_informed_estimate", "owner_share": min(1.0, national_spd_rate + central * (1 - national_spd_rate))})
```

Also add to the sensitivity note.

**Step 4: Wire into pipeline**

In `src/run_pipeline.py`, add import and call:

```python
from prepare_ctb import prepare_ctb_spd
# In run_pipeline():
    ctb = prepare_ctb_spd(cfg); stage_counts["ctb_spd"] = len(ctb.get("by_la", {}))
```

**Step 5: Commit**

```bash
git add src/prepare_ctb.py src/config.py src/sensitivity_analysis.py src/run_pipeline.py
git commit -m "feat: integrate CTB Band H single-person discount rates as contextual signal"
```

---

### Task 4: Create stratified validation sample with title references

Create a rigorous validation sample that could be used to commission Land Registry title register inspections (£3/title) to ground-truth the pipeline's classifications.

**Files:**
- Create: `src/create_validation_sample.py`

**Step 1: Create the validation sample script**

```python
from __future__ import annotations

import csv
import random
from pathlib import Path

from config import OUTPUTS, PipelineConfig
from io_utils import read_parquet_placeholder


def create_validation_sample(cfg: PipelineConfig, sample_size: int = 200) -> list[dict]:
    """Create a stratified random sample for manual validation.

    Stratified by: confidence tier (50% high, 30% low, 20% uncertain),
    then within each stratum by property_type and geographic spread.
    """
    rows = read_parquet_placeholder(Path("data/processed/classified_owner_occupation.parquet"))
    if not rows:
        print("No classified data found.")
        return []

    random.seed(cfg.random_seed)

    # Group by confidence tier + classification
    strata = {
        "high_owner": [],
        "high_not_owner": [],
        "low_owner": [],
        "low_not_owner": [],
        "uncertain": [],
    }
    for r in rows:
        tier = r.get("confidence_tier", "low")
        status = r.get("owner_occupation_status", "uncertain")
        if status == "uncertain":
            strata["uncertain"].append(r)
        elif tier == "high" and "owner_occupied" in status:
            strata["high_owner"].append(r)
        elif tier == "high":
            strata["high_not_owner"].append(r)
        elif "owner_occupied" in status:
            strata["low_owner"].append(r)
        else:
            strata["low_not_owner"].append(r)

    # Target allocation (oversample uncertain and low for validation value)
    allocation = {
        "high_owner": int(sample_size * 0.20),
        "high_not_owner": int(sample_size * 0.20),
        "low_owner": int(sample_size * 0.15),
        "low_not_owner": int(sample_size * 0.15),
        "uncertain": int(sample_size * 0.30),
    }

    sample = []
    for stratum_name, target_n in allocation.items():
        pool = strata[stratum_name]
        n = min(target_n, len(pool))
        selected = random.sample(pool, n) if n else []
        for r in selected:
            sample.append({
                "stratum": stratum_name,
                "property_key": r.get("property_key", ""),
                "postcode_clean": r.get("postcode_clean", ""),
                "address_clean": r.get("address_clean", ""),
                "property_type": r.get("property_type", ""),
                "tenure_type": r.get("tenure_type", ""),
                "price": r.get("price", ""),
                "transfer_date": r.get("transfer_date", ""),
                "pipeline_status": r.get("owner_occupation_status", ""),
                "pipeline_confidence": r.get("confidence_tier", ""),
                "pipeline_evidence": r.get("evidence_basis", ""),
                "ownership_type": r.get("ownership_type", ""),
                "epc_category": r.get("epc_category", ""),
                "match_stage": r.get("match_stage", ""),
                # Validation fields (to be filled manually)
                "lr_title_number": "",
                "lr_proprietor_name": "",
                "lr_proprietor_type": "",
                "validated_owner_occupied": "",
                "validation_notes": "",
            })

    # Write sample
    fields = list(sample[0].keys()) if sample else []
    with (OUTPUTS / "validation_sample.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sample)

    # Write instructions
    (OUTPUTS / "validation_instructions.md").write_text(
        "# Validation Sample Instructions\n\n"
        f"**Sample size:** {len(sample)} properties\n"
        f"**Strata:** {', '.join(f'{k}={len([s for s in sample if s[\"stratum\"]==k])}' for k in allocation)}\n\n"
        "## Process\n\n"
        "1. For each property in `validation_sample.csv`:\n"
        "   - Search the Land Registry at https://search-property-information.service.gov.uk/\n"
        "   - Enter the postcode and select the matching address\n"
        "   - Purchase the title register (£3 per title)\n"
        "   - Record the title number, proprietor name, and proprietor type\n"
        "   - Assess whether the property is owner-occupied based on:\n"
        "     - Proprietor is a named individual (likely owner-occupied)\n"
        "     - Proprietor is a company (likely not owner-occupied)\n"
        "     - Property has a registered charge to a residential mortgage lender (strong owner signal)\n"
        "   - Fill in the `lr_*` and `validated_owner_occupied` columns\n\n"
        "2. Compare pipeline predictions against validated results:\n"
        "   - Calculate accuracy by stratum\n"
        "   - Identify systematic misclassification patterns\n"
        "   - Use results to recalibrate confidence tier weights\n\n"
        f"**Estimated cost:** £{len(sample) * 3} (£3 per title register)\n\n"
        "## Expected outcomes\n\n"
        "- High-confidence classifications should be >90% accurate\n"
        "- Low-confidence classifications are the key calibration target\n"
        "- Uncertain cases will establish the empirical prior for the 0.5 weight\n",
        encoding="utf-8",
    )

    print(f"Validation sample: {len(sample)} properties across {len(allocation)} strata.")
    print(f"Saved to outputs/validation_sample.csv and outputs/validation_instructions.md")
    return sample


if __name__ == "__main__":
    create_validation_sample(PipelineConfig())
```

**Step 2: Verify**

```bash
cd /mnt/c/Users/court/mets/property-sandbox/src && python3 create_validation_sample.py
```

Expected: creates `outputs/validation_sample.csv` with 200 rows and `outputs/validation_instructions.md`.

**Step 3: Commit**

```bash
git add src/create_validation_sample.py
git commit -m "feat: create stratified validation sample for ground-truth verification"
```

---

### Task 5: Create data procurement roadmap

**Files:**
- Create: `docs/data_procurement_roadmap.md`

```markdown
# Data Procurement Roadmap

## Datasets that would improve the evidence base

### 1. AddressBase Core (OS) — UPRN-to-address mapping

**What it provides:** Maps every UPRN to a full postal address, enabling reliable cross-dataset linking.
**Current workaround:** Fuzzy address matching at 53% match rate; EPC UPRNs at 97.6% coverage for EPC-side dedup.
**Expected impact:** Could increase match rate from 53% to 80%+.
**How to obtain:** Free for public sector bodies under the Public Sector Geospatial Agreement (PSGA). Apply via https://www.ordnancesurvey.co.uk/customers/public-sector/public-sector-geospatial-agreement
**Cost:** Free (PSGA) or commercial licence (priced by use case).
**Priority:** HIGH — single biggest improvement for match quality.

### 2. National Polygon + Title Number to UPRN (HM Land Registry)

**What it provides:** Maps every Land Registry title number to a UPRN. Enables direct CCOD Title Number → UPRN → EPC linkage.
**Current workaround:** Address-based CCOD-to-PPD matching.
**Expected impact:** Would make ownership matching near-deterministic for registered titles.
**How to obtain:** https://use-land-property-data.service.gov.uk/datasets/nps
**Cost:** £20,000/year + VAT.
**Priority:** MEDIUM — high value but expensive. Consider if project is funded.

### 3. Electoral Register (Open Register)

**What it provides:** Names of adults registered to vote at each address.
**Expected impact:** Direct evidence of individual occupancy. Would resolve most uncertain cases.
**How to obtain:** The Open Register is available from local Electoral Registration Officers. Contact each billing authority's ERO. For bulk national coverage, a data sharing agreement with the Cabinet Office may be needed.
**Cost:** Free (Open Register is public) but fragmented across 380+ EROs.
**Priority:** HIGH if bulk access can be arranged.

### 4. Council Tax Property-Level Data

**What it provides:** Which properties have single-person discount, empty-property surcharge, second-home premium, etc.
**Expected impact:** Would directly identify occupied vs empty vs second-home properties at the property level.
**How to obtain:** Not available via FOI (GDPR). Requires a formal data sharing agreement (DSA) with each billing authority under the Digital Economy Act 2017, Part 5.
**Cost:** Administrative only, but requires legal framework.
**Priority:** MEDIUM — very valuable but procurement is complex.

### 5. Companies House Bulk Data

**What it provides:** Company name, registration number, status (active/dissolved), SIC codes, registered address.
**Expected impact:** Would confirm CCOD company classifications and identify dissolved companies (property may have reverted to individual ownership).
**How to obtain:** Free bulk download from https://download.companieshouse.gov.uk/en_output.html
**Cost:** Free. ~468MB download.
**Priority:** LOW — CCOD Proprietorship Category already provides most of this signal. Main value is checking for dissolved companies.

## Recommended procurement sequence

1. **AddressBase Core** (if public sector) — apply for PSGA access
2. **Electoral Register** (Open Register) — contact EROs for key London boroughs first (Westminster, K&C, Camden)
3. **Companies House** — download immediately (free)
4. **NPRN** — consider if project funding allows £20K/year
5. **Council Tax DSA** — explore pilot with one willing billing authority
```

**Step 1: Write the file**

As above.

**Step 2: Commit**

```bash
git add docs/data_procurement_roadmap.md
git commit -m "docs: add data procurement roadmap for evidence base improvements"
```

---

### Task 6: Re-run full pipeline with expanded PPD window

After Tasks 1-3 are implemented and the additional PPD files are downloaded.

**Step 1: Run the full pipeline**

```bash
cd /mnt/c/Users/court/mets/property-sandbox && python3 src/run_pipeline.py
```

Expected runtime: ~50-60 minutes (more PPD files to scan).

**Step 2: Verify improved results**

Check:
- `outputs/audit_summary.md` — V1 count should be significantly higher (35K-50K vs 23K)
- `outputs/headline_metrics.csv` — range may shift
- `outputs/headline_metrics_v2.csv` — V2 count should be much higher
- `outputs/sensitivity_scenarios.csv` — new CTB-based scenario if CTB data present
- EPC dedup should show fewer duplicates (UPRN-based is more reliable)

**Step 3: Run validation sample**

```bash
cd /mnt/c/Users/court/mets/property-sandbox/src && python3 create_validation_sample.py
```

**Step 4: Commit any output changes and final state**

```bash
git add -A
git commit -m "feat: pipeline re-run with expanded PPD window and evidence base improvements"
```

---

## Summary of changes by file

| File | Task | Changes |
|------|------|---------|
| `src/config.py` | 1, 3 | ppd_download_limit → 15, CTB URL |
| `src/prepare_epc.py` | 2 | Capture UPRN, UPRN-primary dedup |
| `src/prepare_ctb.py` | 3 | New — parse CTB Band H SPD rates |
| `src/sensitivity_analysis.py` | 3 | CTB SPD calibration scenario |
| `src/run_pipeline.py` | 3 | Wire in CTB preparation |
| `src/create_validation_sample.py` | 4 | New — stratified validation sample |
| `docs/data_procurement_roadmap.md` | 5 | New — procurement guide |

## Datasets

| Dataset | Status | Action |
|---------|--------|--------|
| PPD yearly files (2010-2022) | Auto-downloadable | Pipeline will fetch them |
| CTB 2025 statistics | Manual download required | User downloads from gov.uk, saves to `data/raw/voa/ctb_2025.csv` |
| All other data | Already present | No action needed |
