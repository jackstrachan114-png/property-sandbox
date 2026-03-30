"""Create a stratified random sample of 200 properties for manual
ground-truth validation via Land Registry title register inspection.

Each title register costs £3, so the total cost for the full sample is
200 * £3 = £600.
"""
from __future__ import annotations

import random
from pathlib import Path

from config import DATA_PROCESSED, OUTPUTS, PipelineConfig
from io_utils import read_parquet_placeholder, write_csv

SAMPLE_SIZE = 200

# Strata definitions: (name, filter_fn, share_of_sample)
# "high confidence" = confidence_tier == "high"
# "low confidence"  = confidence_tier in {"medium", "low"}
STRATA: list[tuple[str, dict, float]] = [
    (
        "high_owner",
        {"status": "owner_occupied_likely", "tiers": {"high"}},
        0.20,
    ),
    (
        "high_not_owner",
        {"status": "not_owner_occupied_likely", "tiers": {"high"}},
        0.20,
    ),
    (
        "low_owner",
        {"status": "owner_occupied_likely", "tiers": {"medium", "low"}},
        0.15,
    ),
    (
        "low_not_owner",
        {"status": "not_owner_occupied_likely", "tiers": {"medium", "low"}},
        0.15,
    ),
    (
        "uncertain",
        {"status": "uncertain", "tiers": {"high", "medium", "low"}},
        0.30,
    ),
]

CSV_FIELDS = [
    "stratum",
    "property_key",
    "postcode_clean",
    "address_clean",
    "property_type",
    "tenure_type",
    "price",
    "transfer_date",
    "district",
    "pipeline_status",
    "pipeline_confidence",
    "pipeline_evidence",
    "ownership_type",
    "epc_category",
    "match_stage",
    # Empty columns for manual validation
    "lr_title_number",
    "lr_proprietor_name",
    "lr_proprietor_type",
    "validated_owner_occupied",
    "validation_notes",
]


def _assign_stratum(row: dict) -> str | None:
    """Return the stratum name a row belongs to, or None if it fits none."""
    status = row.get("owner_occupation_status", "")
    tier = row.get("confidence_tier", "")
    for name, filt, _ in STRATA:
        if status == filt["status"] and tier in filt["tiers"]:
            return name
    return None


def _build_sample_row(row: dict, stratum: str) -> dict:
    return {
        "stratum": stratum,
        "property_key": row.get("property_key", ""),
        "postcode_clean": row.get("postcode_clean", ""),
        "address_clean": row.get("address_clean", ""),
        "property_type": row.get("property_type", ""),
        "tenure_type": row.get("tenure_type", ""),
        "price": row.get("price", ""),
        "transfer_date": row.get("transfer_date", ""),
        "district": row.get("district", ""),
        "pipeline_status": row.get("owner_occupation_status", ""),
        "pipeline_confidence": row.get("confidence_tier", ""),
        "pipeline_evidence": row.get("evidence_basis", ""),
        "ownership_type": row.get("ownership_type", ""),
        "epc_category": row.get("epc_category", ""),
        "match_stage": row.get("match_stage", ""),
        # Manual validation columns left blank
        "lr_title_number": "",
        "lr_proprietor_name": "",
        "lr_proprietor_type": "",
        "validated_owner_occupied": "",
        "validation_notes": "",
    }


def _write_instructions(strata_counts: dict[str, int], total: int) -> None:
    """Write a markdown file with validation instructions."""
    cost = total * 3
    lines = [
        "# Validation Sample Instructions",
        "",
        "## Sample overview",
        "",
        f"Total properties sampled: **{total}**",
        "",
        f"Estimated cost: **{total} x £3 = £{cost:,}**",
        "",
        "### Strata breakdown",
        "",
        "| Stratum | Target % | Count |",
        "| ------- | -------: | ----: |",
    ]
    for name, _, share in STRATA:
        count = strata_counts.get(name, 0)
        lines.append(f"| {name} | {share:.0%} | {count} |")
    lines += [
        "",
        "## Step-by-step Land Registry title register lookup",
        "",
        "1. Go to https://search-property-information.service.gov.uk/",
        "2. Search by postcode (`postcode_clean` column) and select the correct address.",
        "3. Purchase the title register (£3 per title).",
        "4. Record the **title number** in the `lr_title_number` column.",
        "5. In the Proprietorship Register (Section B):",
        "   - Record the proprietor name in `lr_proprietor_name`.",
        "   - Record the proprietor type in `lr_proprietor_type`:",
        "     - `individual` — one or more named persons",
        "     - `company` — a limited company, LLP, or overseas entity",
        "     - `public_body` — local authority, government department, NHS trust, etc.",
        "     - `housing_association` — registered provider of social housing",
        "     - `trust_or_other` — trustees, charities, or other entities",
        "",
        "## What to look for",
        "",
        "- **Individual proprietor** = strong signal the property is owner-occupied,",
        "  especially if there is a mortgage charge (Section C) from a residential",
        "  lender (e.g. Nationwide, Halifax, NatWest). Buy-to-let mortgages are less",
        "  common on registered charges for high-value properties.",
        "- **Company proprietor** = strong signal the property is NOT owner-occupied",
        "  (unless it is a personal holding company — note this in `validation_notes`).",
        "- **Mortgage / charge entries** in Section C: a residential mortgage from a",
        "  high-street lender is a positive owner-occupation signal.",
        "- **Restrictions** mentioning a trust or corporate structure suggest investment",
        "  ownership.",
        "",
        "## Recording your validation",
        "",
        "For each row in the CSV:",
        "",
        "1. Fill in `lr_title_number`, `lr_proprietor_name`, `lr_proprietor_type`.",
        "2. Set `validated_owner_occupied` to one of:",
        "   - `yes` — evidence supports owner-occupation",
        "   - `no` — evidence supports non-owner-occupation",
        "   - `unclear` — title register is ambiguous",
        "3. Add any relevant notes to `validation_notes` (e.g. 'SPV company',",
        "   'residential mortgage from Barclays', 'overseas entity').",
        "",
        "## Comparing results back to the pipeline",
        "",
        "After completing all lookups:",
        "",
        "1. Load the completed CSV alongside the pipeline output.",
        "2. For each stratum, compute:",
        "   - **Accuracy**: share of rows where `validated_owner_occupied` agrees with",
        "     `pipeline_status` (mapping `owner_occupied_likely` -> `yes`,",
        "     `not_owner_occupied_likely` -> `no`, `uncertain` -> either).",
        "   - **Precision / Recall** for the `owner_occupied_likely` class.",
        "3. Aggregate across strata weighted by their population share to get an",
        "   overall accuracy estimate.",
        "4. Identify systematic error patterns (e.g. pipeline over-counts owner-",
        "   occupation for leasehold flats) and feed these back into the classification",
        "   rules.",
        "",
    ]
    path = OUTPUTS / "validation_instructions.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def create_validation_sample(cfg: PipelineConfig) -> list[dict]:
    """Create a stratified validation sample from classified output."""
    rows = read_parquet_placeholder(
        DATA_PROCESSED / "classified_owner_occupation.parquet"
    )
    if not rows:
        print("No classified data found — skipping validation sample.")
        return []

    random.seed(cfg.random_seed)

    # Bucket rows by stratum
    buckets: dict[str, list[dict]] = {name: [] for name, _, _ in STRATA}
    for r in rows:
        stratum = _assign_stratum(r)
        if stratum is not None:
            buckets[stratum].append(r)

    # Draw stratified sample
    sample_rows: list[dict] = []
    strata_counts: dict[str, int] = {}
    for name, _, share in STRATA:
        target_n = round(SAMPLE_SIZE * share)
        pool = buckets[name]
        draw_n = min(target_n, len(pool))
        drawn = random.sample(pool, draw_n) if draw_n else []
        strata_counts[name] = draw_n
        for r in drawn:
            sample_rows.append(_build_sample_row(r, name))

    total = len(sample_rows)

    # Write CSV
    write_csv(OUTPUTS / "validation_sample.csv", sample_rows, CSV_FIELDS)

    # Write instructions
    _write_instructions(strata_counts, total)

    print(f"Validation sample: {total} properties written to outputs/validation_sample.csv")
    for name, count in strata_counts.items():
        print(f"  {name}: {count}")

    return sample_rows


if __name__ == "__main__":
    create_validation_sample(PipelineConfig())
