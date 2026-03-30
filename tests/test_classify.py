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
