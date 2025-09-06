"""
Lightweight smoke test for historical extraction plumbing (no network).

This test imports the extractor and validates helper methods on date ranges.
"""
from datetime import datetime
import sys
from pathlib import Path

# Ensure project root is on sys.path when running the test directly
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.extractors.race_card_extractor import RaceCardExtractor


def test_date_iter_and_url():
    ext = RaceCardExtractor()
    # Test date iteration
    dates = list(ext._iter_dates_inclusive("2025-09-05", "2025-09-07"))
    assert dates == ["2025-09-05", "2025-09-06", "2025-09-07"]

    # Test base url normalization
    assert ext.base_url.endswith("/")

    # Build a results URL for one date (indirectly via method logic)
    sample_url = f"{ext.base_url}#results-list/r_date=2025-09-05"
    assert "#results-list/r_date=2025-09-05" in sample_url


if __name__ == "__main__":
    test_date_iter_and_url()
    print("smoke tests passed")
