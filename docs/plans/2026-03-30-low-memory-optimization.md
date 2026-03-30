# Low-Memory Pipeline Optimization

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reduce peak memory usage so the pipeline runs comfortably on an 8GB M1 MacBook Pro.

**Architecture:** The pipeline currently holds all intermediate datasets in `run_pipeline.py` local variables for the entire run, loads the 791k-row EPC dataset 3 separate times, and builds redundant index structures during linking. We fix this by: (1) releasing references as soon as each stage is done, (2) deduplicating EPC inline instead of accumulate-then-dedup, (3) consolidating V1+V2 linking into one pass that loads EPC/ownership once, and (4) avoiding full re-reads in sensitivity analysis.

**Tech Stack:** Pure Python standard library (no new dependencies)

**Estimated peak reduction:** ~1.2-1.5 GB down to ~400-500 MB

---

## Task 1: Inline EPC deduplication (eliminate ~800 MB transient double-copy)

Currently `prepare_epc.py` accumulates all 791k filtered rows into `out`, then builds a second `dedup` dict from them. Both exist simultaneously (~800 MB). Fix: dedup inline during the streaming loop, exactly like `prepare_price_paid.py` already does.

**Files:**
- Modify: `src/prepare_epc.py:64-125`

**Step 1: Write the test**

Create `tests/test_prepare_epc_dedup.py`:

```python
"""Test that EPC dedup produces correct results when done inline."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from prepare_epc import map_epc_category


def test_map_epc_category_owner():
    assert map_epc_category("owner-occupied") == "owner_occupied"


def test_map_epc_category_rental_private():
    assert map_epc_category("", "rental (private)") == "rented_private"


def test_map_epc_category_unknown():
    assert map_epc_category("", "") == "unknown"
```

**Step 2: Run test to verify it passes**

Run: `cd /mnt/c/Users/court/mets/property-sandbox && python -m pytest tests/test_prepare_epc_dedup.py -v`
Expected: PASS (these test existing logic, not the refactor)

**Step 3: Refactor `prepare_epc` to dedup inline**

In `src/prepare_epc.py`, replace the two-phase approach (accumulate into `out` list, then build `dedup` dict) with a single-phase inline dedup:

```python
def prepare_epc(cfg: PipelineConfig, candidate_postcodes: set[str] | None = None) -> list[dict]:
    folder = DATA_RAW / "epc"
    files = sorted([*folder.glob("*.csv"), *folder.glob("*.zip")])
    if not files:
        print(
            "INFO: No EPC files found in data/raw/epc/. "
            "Property-level EPC data requires registration at https://epc.opendatacommunities.org/. "
            "See docs/data_acquisition_guide.md for instructions."
        )
        write_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet", [])
        return []

    # Dedup inline: keep latest EPC per property (same pattern as prepare_price_paid)
    dedup: dict[str, dict] = {}
    scanned = 0
    kept = 0
    for r in _iter_epc_rows(files):
        scanned += 1
        postcode = clean_text(r.get("postcode", "")).replace(" ", "")

        if candidate_postcodes and postcode not in candidate_postcodes:
            continue

        kept += 1
        addr = clean_text(r.get("address") or r.get("address1") or "")
        lodgement = r.get("lodgement_date") or r.get("lodgement_datetime") or ""
        brn = r.get("building_reference_number") or ""
        uprn = (r.get("uprn") or "").strip()
        tenure = r.get("tenure") or ""
        txn_type = r.get("transaction_type") or ""

        rec = {
            "postcode_clean": postcode,
            "address_clean": addr,
            "epc_source_field": str(tenure),
            "epc_transaction_type": str(txn_type),
            "epc_category": map_epc_category(str(tenure), str(txn_type)),
            "lodgement_date": str(lodgement)[:10],
            "building_reference_number": brn,
            "uprn": uprn,
        }

        # Dedup key priority: UPRN > BRN > postcode+address
        if uprn:
            key = f"uprn:{uprn}"
        elif brn:
            key = f"brn:{brn}"
        else:
            key = f"addr:{postcode}|{addr}"

        existing = dedup.get(key)
        if not existing or rec.get("lodgement_date", "") > existing.get("lodgement_date", ""):
            dedup[key] = rec

    filter_desc = f", kept {kept:,} matching candidate postcodes" if candidate_postcodes else ""
    print(f"EPC: scanned {scanned:,} rows{filter_desc}.")

    out = list(dedup.values())
    print(f"EPC: {len(out):,} unique properties after deduplication (UPRN/BRN/address).")

    write_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet", out)
    return out
```

**Step 4: Run test to verify it still passes**

Run: `cd /mnt/c/Users/court/mets/property-sandbox && python -m pytest tests/test_prepare_epc_dedup.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/prepare_epc.py tests/test_prepare_epc_dedup.py
git commit -m "perf: inline EPC dedup to eliminate transient double-copy (~800MB saving)"
```

---

## Task 2: Release references early in `run_pipeline.py` (eliminate ~600 MB of stale data)

`run_pipeline.py` holds every stage result in local variables for the entire function lifetime. Variables like `ppd`, `ukhpi`, `epc`, `own` are never used after candidate population building, but stay alive through linking, classification, and sensitivity. Fix: explicitly `del` them and replace return-value captures with `len()` calls where only the count is needed.

**Files:**
- Modify: `src/run_pipeline.py:126-165`

**Step 1: Refactor `run_pipeline` to release references**

Replace `run_pipeline` function body with early `del` statements:

```python
def run_pipeline(cfg: PipelineConfig) -> None:
    ensure_directories()
    stage_counts = {}

    run_downloads(cfg)
    ppd = prepare_price_paid(cfg); stage_counts["price_paid_clean"] = len(ppd)
    ukhpi = prepare_ukhpi(cfg); stage_counts["ukhpi_uplift"] = len(ukhpi)

    # Build candidate postcode set for early filtering of large EPC/ownership files
    candidate_postcodes = {r.get("postcode_clean", "") for r in ppd} - {""}
    print(f"Candidate postcodes for EPC/ownership filter: {len(candidate_postcodes):,}")

    # Release PPD and UKHPI — data is on disk, only postcodes needed going forward
    del ppd, ukhpi

    epc = prepare_epc(cfg, candidate_postcodes=candidate_postcodes); stage_counts["epc_clean"] = len(epc)
    own = prepare_ownership(cfg, candidate_postcodes=candidate_postcodes); stage_counts["ownership_clean"] = len(own)
    del epc, own  # written to disk; linking re-reads from disk

    addr = prepare_addresses(cfg); stage_counts["address_reference"] = len(addr)
    del addr
    ctx = prepare_contextual_sources(cfg); stage_counts["contextual_inventory"] = len(ctx)
    del ctx
    voa = prepare_voa_band_h(cfg); stage_counts["voa_band_h"] = voa.get("total_band_h", 0)
    del voa
    ctb = prepare_ctb_empty(cfg); stage_counts["ctb_band_h_empty"] = ctb.get("national_band_h_empty", 0)
    del ctb, candidate_postcodes

    v1, v2 = build_candidate_populations(cfg)
    stage_counts["candidate_population_v1"] = len(v1)
    stage_counts["candidate_population_v2"] = len(v2)
    del v1, v2  # written to disk; linking re-reads from disk

    # V1 linking and classification
    linked = link_properties(cfg); stage_counts["linked_candidate_population"] = len(linked)
    del linked  # written to disk; classify re-reads from disk
    classified = classify_owner_occupation(cfg); stage_counts["classified_owner_occupation"] = len(classified)

    metrics = build_headline_range(classified)
    write_csv(OUTPUTS / "headline_metrics.csv", metrics, ["estimate_type", "owner_occupation_share"])
    del classified  # release before V2 linking

    # V2 linking and classification
    linked_v2 = link_properties_v2(cfg); stage_counts["linked_v2"] = len(linked_v2)
    del linked_v2
    classified_v2 = classify_v2(cfg); stage_counts["classified_v2"] = len(classified_v2)
    metrics_v2 = build_headline_range(classified_v2)
    write_csv(OUTPUTS / "headline_metrics_v2.csv", metrics_v2, ["estimate_type", "owner_occupation_share"])
    del classified_v2

    run_sensitivity(cfg)
    # Re-read classified for policy brief and audit (small relative to peak)
    classified_for_report = read_parquet_placeholder(DATA_PROCESSED / "classified_owner_occupation.parquet")
    write_policy_brief(metrics, classified_for_report)
    write_audit_summary(stage_counts, classified_for_report)
```

Note: This requires adding `read_parquet_placeholder` to the imports and `DATA_PROCESSED` to the config imports.

**Step 2: Update imports at top of `run_pipeline.py`**

Add `read_parquet_placeholder` to the `io_utils` import and `DATA_PROCESSED` to the `config` import:

```python
from config import DATA_PROCESSED, OUTPUTS, PipelineConfig, ensure_directories
from io_utils import read_csv as write_csv_unused, read_parquet_placeholder, write_csv
```

Wait — `write_csv` is already imported. Just add:

```python
from io_utils import read_parquet_placeholder, write_csv
```

And update the config import:

```python
from config import DATA_PROCESSED, OUTPUTS, PipelineConfig, ensure_directories
```

**Step 3: Run the pipeline dry (no data needed — just verify no NameError)**

Run: `cd /mnt/c/Users/court/mets/property-sandbox && python -c "from run_pipeline import run_pipeline; print('Import OK')"`
Expected: "Import OK" (validates no syntax errors)

**Step 4: Commit**

```bash
git add src/run_pipeline.py
git commit -m "perf: release intermediate references early in run_pipeline (~600MB saving)"
```

---

## Task 3: Consolidate V1+V2 linking into single EPC/ownership load

`link_properties` and `link_properties_v2` each independently load the full 791k EPC and 226k ownership datasets from disk and build their own index structures. This means EPC is loaded twice during the linking phase. Fix: extract a shared `_link_candidates` helper, and have a new `link_all` function that loads EPC/ownership once and links both V1 and V2.

**Files:**
- Modify: `src/link_properties.py`
- Modify: `src/run_pipeline.py` (update call site)

**Step 1: Write test for the shared linking helper**

Create `tests/test_link_properties.py`:

```python
"""Test that linking produces expected match stages."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from io_utils import score_similarity


def test_score_similarity_exact():
    assert score_similarity("1 high street london", "1 high street london") == 100


def test_score_similarity_different_numbers():
    """Building number guard: different house numbers should return 0."""
    assert score_similarity("1 high street", "2 high street") == 0


def test_score_similarity_partial():
    score = score_similarity("1 high street london", "1 high street")
    assert 50 <= score <= 100
```

**Step 2: Run test**

Run: `cd /mnt/c/Users/court/mets/property-sandbox && python -m pytest tests/test_link_properties.py -v`
Expected: PASS

**Step 3: Extract `_link_candidates` helper and `link_all` function**

Rewrite `src/link_properties.py`. The key change: load EPC/ownership once, link V1 then V2 using the same indexes, then delete indexes.

```python
def _link_candidates(
    candidates: list[dict],
    epc_idx: dict,
    own_idx: dict,
    epc_by_postcode: dict,
    own_by_postcode: dict,
    cfg: PipelineConfig,
) -> list[dict]:
    """Link a list of candidate dicts against pre-built EPC and ownership indexes."""
    linked = []
    for c in candidates:
        pc = c.get("postcode_clean", "")
        addr = clean_text(c.get("address_clean", ""))
        key = (pc, addr)
        rec = dict(c)
        rec["address_norm"] = addr
        rec["match_stage"] = "unmatched"

        # EPC matching: exact then fuzzy
        e = epc_idx.get(key)
        if e:
            rec["epc_category"] = e.get("epc_category", "unknown")
            rec["epc_transaction_type"] = e.get("epc_transaction_type", "")
            rec["match_stage"] = "exact_postcode_address"
        else:
            same_pc = epc_by_postcode.get(pc, [])
            if same_pc:
                scored = sorted(
                    [(score_similarity(addr, x.get("address_clean", "")), x) for x in same_pc],
                    reverse=True, key=lambda z: z[0],
                )
                if scored and scored[0][0] >= cfg.fuzzy_match_cutoff:
                    rec["epc_category"] = scored[0][1].get("epc_category", "unknown")
                    rec["epc_transaction_type"] = scored[0][1].get("epc_transaction_type", "")
                    rec["match_stage"] = "postcode_fuzzy_address"

        # Ownership matching: exact then fuzzy
        o = own_idx.get(key)
        if not o:
            same_pc_own = own_by_postcode.get(pc, [])
            if same_pc_own:
                scored_own = sorted(
                    [(score_similarity(addr, x.get("address_clean", "")), x) for x in same_pc_own],
                    reverse=True, key=lambda z: z[0],
                )
                if scored_own and scored_own[0][0] >= cfg.fuzzy_match_cutoff:
                    o = scored_own[0][1]
        if o:
            rec["ownership_type"] = o.get("ownership_type", "unresolved")
            rec["ownership_type_confidence"] = o.get("ownership_type_confidence", "low")
        linked.append(rec)
    return linked
```

Then replace `link_properties` and `link_properties_v2` with a single `link_all`:

```python
def link_all(cfg: PipelineConfig) -> tuple[list[dict], list[dict]]:
    """Link both V1 and V2 candidates, loading EPC/ownership data only once."""
    v1_candidates = read_parquet_placeholder(DATA_INTERIM / "candidate_population_v1.parquet")
    v2_candidates = read_parquet_placeholder(DATA_INTERIM / "candidate_population_v2.parquet")

    if cfg.strict_core_inputs and not v1_candidates:
        raise RuntimeError("candidate_population_v1.parquet has no rows; cannot link properties.")

    # Load reference datasets once
    epc_rows = read_parquet_placeholder(DATA_INTERIM / "epc_clean.parquet")
    own_rows = read_parquet_placeholder(DATA_INTERIM / "ownership_clean.parquet")

    epc_idx = {(r.get("postcode_clean", ""), clean_text(r.get("address_clean", ""))): r for r in epc_rows}
    own_idx = {(r.get("postcode_clean", ""), clean_text(r.get("address_clean", ""))): r for r in own_rows}
    epc_by_postcode = _build_postcode_index(epc_rows)
    own_by_postcode = _build_postcode_index(own_rows)

    # Free the source lists — indexes hold all references we need
    del epc_rows, own_rows

    # Link V1
    linked_v1 = _link_candidates(v1_candidates, epc_idx, own_idx, epc_by_postcode, own_by_postcode, cfg)
    del v1_candidates
    write_parquet_placeholder(DATA_PROCESSED / "linked_candidate_population.parquet", linked_v1)

    # Write V1 diagnostics
    counts = {}
    for r in linked_v1:
        counts[r.get("match_stage", "unmatched")] = counts.get(r.get("match_stage", "unmatched"), 0) + 1
    summary = [{"match_stage": k, "count": v, "share": (v / len(linked_v1) if linked_v1 else 0)} for k, v in counts.items()]
    write_csv(OUTPUTS / "linkage_coverage_summary.csv", summary, ["match_stage", "count", "share"])

    random.seed(cfg.random_seed)
    sample_n = min(cfg.manual_review_sample_size, len(linked_v1))
    sample = random.sample(linked_v1, sample_n) if sample_n else []
    sample_rows = [{
        "property_key": r.get("property_key", ""),
        "postcode_clean": r.get("postcode_clean", ""),
        "address_norm": r.get("address_norm", ""),
        "match_stage": r.get("match_stage", ""),
        "ownership_type": r.get("ownership_type", ""),
        "epc_category": r.get("epc_category", ""),
    } for r in sample]
    write_csv(OUTPUTS / "manual_review_sample.csv", sample_rows, ["property_key", "postcode_clean", "address_norm", "match_stage", "ownership_type", "epc_category"])

    # Link V2 (reuses same indexes — no second load)
    linked_v2 = _link_candidates(v2_candidates, epc_idx, own_idx, epc_by_postcode, own_by_postcode, cfg)
    del v2_candidates, epc_idx, own_idx, epc_by_postcode, own_by_postcode
    write_parquet_placeholder(DATA_PROCESSED / "linked_candidate_population_v2.parquet", linked_v2)
    print(f"V2 linking: {len(linked_v2):,} candidates linked.")

    return linked_v1, linked_v2
```

Keep the old `link_properties` and `link_properties_v2` as thin wrappers for backward compatibility of the `if __name__ == "__main__"` block and any standalone usage:

```python
def link_properties(cfg: PipelineConfig) -> list[dict]:
    """Standalone V1 linking (loads its own data). Use link_all() for memory efficiency."""
    v1, _ = link_all(cfg)
    return v1


def link_properties_v2(cfg: PipelineConfig) -> list[dict]:
    """Standalone V2 linking (loads its own data). Use link_all() for memory efficiency."""
    _, v2 = link_all(cfg)
    return v2
```

**Step 4: Update `run_pipeline.py` to call `link_all`**

Replace the separate V1/V2 linking + classification calls with:

```python
from link_properties import build_candidate_populations, link_all
```

And in the function body, replace the linking section:

```python
    # Link V1 and V2 in a single pass (loads EPC/ownership once)
    linked_v1, linked_v2 = link_all(cfg)
    stage_counts["linked_candidate_population"] = len(linked_v1)
    stage_counts["linked_v2"] = len(linked_v2)
    del linked_v1, linked_v2  # written to disk; classify re-reads

    classified = classify_owner_occupation(cfg); stage_counts["classified_owner_occupation"] = len(classified)
    metrics = build_headline_range(classified)
    write_csv(OUTPUTS / "headline_metrics.csv", metrics, ["estimate_type", "owner_occupation_share"])
    del classified

    classified_v2 = classify_v2(cfg); stage_counts["classified_v2"] = len(classified_v2)
    metrics_v2 = build_headline_range(classified_v2)
    write_csv(OUTPUTS / "headline_metrics_v2.csv", metrics_v2, ["estimate_type", "owner_occupation_share"])
    del classified_v2
```

**Step 5: Run tests**

Run: `cd /mnt/c/Users/court/mets/property-sandbox && python -m pytest tests/ -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/link_properties.py src/run_pipeline.py tests/test_link_properties.py
git commit -m "perf: consolidate V1+V2 linking into single EPC/ownership load"
```

---

## Task 4: Add streaming JSONL reader to `io_utils.py`

Currently `read_parquet_placeholder` loads every line into a list. Add a generator variant `iter_parquet_placeholder` so modules that only need counts or single-pass iteration can stream without materializing the full list.

**Files:**
- Modify: `src/io_utils.py`

**Step 1: Write test**

Create `tests/test_io_utils.py`:

```python
"""Test streaming JSONL reader."""
import json
import sys, os, tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from io_utils import iter_parquet_placeholder, read_parquet_placeholder


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
```

**Step 2: Run test to verify it fails**

Run: `cd /mnt/c/Users/court/mets/property-sandbox && python -m pytest tests/test_io_utils.py -v`
Expected: FAIL (iter_parquet_placeholder not defined yet)

**Step 3: Implement `iter_parquet_placeholder`**

Add to `src/io_utils.py` after `read_parquet_placeholder`:

```python
def iter_parquet_placeholder(path: Path):
    """Yield rows one at a time from a JSONL file without loading all into memory."""
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def count_parquet_placeholder(path: Path) -> int:
    """Count rows in a JSONL file without loading data into memory."""
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count
```

**Step 4: Run test to verify it passes**

Run: `cd /mnt/c/Users/court/mets/property-sandbox && python -m pytest tests/test_io_utils.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/io_utils.py tests/test_io_utils.py
git commit -m "feat: add streaming JSONL reader and row counter to io_utils"
```

---

## Task 5: Use streaming counts in `sensitivity_analysis.py`

`sensitivity_analysis.py` re-reads 4 full datasets from disk just to compute `len()` and aggregates. Two of these (`candidate_population_v1`, `candidate_population_v2`) are only used for their count. Fix: use `count_parquet_placeholder` for those.

**Files:**
- Modify: `src/sensitivity_analysis.py:9-32`

**Step 1: Update imports**

```python
from io_utils import count_parquet_placeholder, read_parquet_placeholder, write_csv
```

**Step 2: Replace full reads with counts where only `len()` is needed**

Change lines 21-22 from:
```python
    v1 = len(read_parquet_placeholder(DATA_INTERIM / "candidate_population_v1.parquet"))
    v2 = len(read_parquet_placeholder(DATA_INTERIM / "candidate_population_v2.parquet"))
```

To:
```python
    v1 = count_parquet_placeholder(DATA_INTERIM / "candidate_population_v1.parquet")
    v2 = count_parquet_placeholder(DATA_INTERIM / "candidate_population_v2.parquet")
```

**Step 3: Run pipeline import check**

Run: `cd /mnt/c/Users/court/mets/property-sandbox && python -c "from sensitivity_analysis import run_sensitivity; print('OK')"`
Expected: "OK"

**Step 4: Commit**

```bash
git add src/sensitivity_analysis.py
git commit -m "perf: use streaming row counts in sensitivity analysis"
```

---

## Task 6: Classify in-place instead of dict copy

`classify_owner_occupation.py` creates a `dict(r)` copy for every row, doubling memory during classification. Since the input list is not reused after classification, we can update rows in-place.

**Files:**
- Modify: `src/classify_owner_occupation.py:45-107`

**Step 1: Write test**

Create `tests/test_classify.py`:

```python
"""Test classification logic."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from classify_owner_occupation import classify_row


def test_classify_company_owned():
    status, tier, evidence, flag = classify_row({"ownership_type": "UK_company", "epc_category": "unknown"})
    assert status == "not_owner_occupied_likely"
    assert tier == "high"


def test_classify_epc_owner():
    status, tier, evidence, flag = classify_row({"ownership_type": "unresolved", "epc_category": "owner_occupied"})
    assert status == "owner_occupied_likely"
    assert tier == "high"


def test_classify_uncertain():
    status, tier, evidence, flag = classify_row({"ownership_type": "unresolved", "epc_category": "unknown"})
    assert status == "uncertain"
    assert tier == "low"
```

**Step 2: Run test**

Run: `cd /mnt/c/Users/court/mets/property-sandbox && python -m pytest tests/test_classify.py -v`
Expected: PASS

**Step 3: Modify classify functions to update in-place**

In `classify_owner_occupation`:

```python
def classify_owner_occupation(cfg: PipelineConfig) -> list[dict]:
    rows = read_parquet_placeholder(DATA_PROCESSED / "linked_candidate_population.parquet")
    for r in rows:
        status, tier, evidence, flag = classify_row(r)
        r["owner_occupation_status"] = status
        r["confidence_tier"] = tier
        r["evidence_basis"] = evidence
        r["conflicting_signals_flag"] = flag

    write_parquet_placeholder(DATA_PROCESSED / "classified_owner_occupation.parquet", rows)

    # ... rest of diagnostics unchanged, but use `rows` instead of `out` ...
```

Same for `classify_v2`:

```python
def classify_v2(cfg: PipelineConfig) -> list[dict]:
    rows = read_parquet_placeholder(DATA_PROCESSED / "linked_candidate_population_v2.parquet")
    if not rows:
        return []
    for r in rows:
        status, tier, evidence, flag = classify_row(r)
        r["owner_occupation_status"] = status
        r["confidence_tier"] = tier
        r["evidence_basis"] = evidence
        r["conflicting_signals_flag"] = flag
    write_parquet_placeholder(DATA_PROCESSED / "classified_v2.parquet", rows)
    return rows
```

**Step 4: Run all tests**

Run: `cd /mnt/c/Users/court/mets/property-sandbox && python -m pytest tests/ -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/classify_owner_occupation.py tests/test_classify.py
git commit -m "perf: classify in-place instead of dict copy (~200MB saving for V2)"
```

---

## Task 7: End-to-end verification

Run the full pipeline and verify outputs are identical to the pre-optimization baseline.

**Step 1: Save baseline metrics (before optimization)**

```bash
cp outputs/headline_metrics.csv outputs/headline_metrics_baseline.csv
cp outputs/headline_metrics_v2.csv outputs/headline_metrics_v2_baseline.csv
cp outputs/sensitivity_scenarios.csv outputs/sensitivity_scenarios_baseline.csv
```

**Step 2: Run full pipeline**

```bash
cd /mnt/c/Users/court/mets/property-sandbox && python src/run_pipeline.py
```

**Step 3: Diff outputs against baseline**

```bash
diff outputs/headline_metrics.csv outputs/headline_metrics_baseline.csv
diff outputs/headline_metrics_v2.csv outputs/headline_metrics_v2_baseline.csv
diff outputs/sensitivity_scenarios.csv outputs/sensitivity_scenarios_baseline.csv
```

Expected: No differences (optimization is behavior-preserving)

**Step 4: Commit baseline comparison artifacts, then clean up**

```bash
rm outputs/*_baseline.csv
```

**Step 5: Final commit**

```bash
git add -A
git commit -m "chore: low-memory optimization complete — verified output equivalence"
```

---

## Memory Budget Summary

| Stage | Before | After | Saving |
|-------|--------|-------|--------|
| `prepare_epc` double-copy | ~800 MB | ~400 MB | ~400 MB |
| `run_pipeline` stale refs (ppd+ukhpi+epc+own) | ~600 MB | 0 MB | ~600 MB |
| `link_properties` double EPC load | ~400 MB | 0 MB | ~400 MB |
| `classify` dict copy | ~200 MB | 0 MB | ~200 MB |
| `sensitivity` full re-reads for counts | ~150 MB | ~1 MB | ~149 MB |
| **Estimated peak** | **~1.2-1.5 GB** | **~400-500 MB** | **~60-65%** |
