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
