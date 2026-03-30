"""Test EPC category mapping and inline dedup logic."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from prepare_epc import map_epc_category


def test_map_epc_category_owner():
    assert map_epc_category("owner-occupied") == "owner_occupied"


def test_map_epc_category_rental_private():
    assert map_epc_category("", "rental (private)") == "rented_private"


def test_map_epc_category_unknown():
    assert map_epc_category("", "") == "unknown"


def test_dedup_uprn_priority_and_latest_wins():
    """Simulate the inline dedup logic: UPRN takes priority, latest lodgement wins."""
    rows = [
        {"uprn": "123", "building_reference_number": "B1", "postcode_clean": "SW1A1AA",
         "address_clean": "1 downing st", "lodgement_date": "2020-01-01", "epc_category": "old"},
        {"uprn": "123", "building_reference_number": "B2", "postcode_clean": "SW1A1AA",
         "address_clean": "1 downing st", "lodgement_date": "2023-06-15", "epc_category": "new"},
        {"uprn": "", "building_reference_number": "B3", "postcode_clean": "SW1A1AA",
         "address_clean": "2 downing st", "lodgement_date": "2022-01-01", "epc_category": "brn_only"},
    ]

    # Replicate the inline dedup logic from prepare_epc
    dedup: dict[str, dict] = {}
    for rec in rows:
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

    result = list(dedup.values())
    # Two UPRNs collapse to one (latest wins), BRN row kept separately
    assert len(result) == 2
    uprn_row = [r for r in result if r.get("uprn") == "123"][0]
    assert uprn_row["epc_category"] == "new"  # latest lodgement won
    assert uprn_row["lodgement_date"] == "2023-06-15"
    brn_row = [r for r in result if r.get("building_reference_number") == "B3"][0]
    assert brn_row["epc_category"] == "brn_only"
