"""Canonical pipeline entry point for the Greyhound Racing Data Pipeline.

Usage:
    python pipeline.py

This orchestrates:
 1. Race card extraction
 2. Dog statistics extraction (best-effort; continues if partial)
 3. Feature engineering (produces daily + historical datasets)
 4. Summary report + logging
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Ensure src on path
sys.path.append(str(Path(__file__).parent / "src"))

from src.extractors.race_card_extractor import (
    extract_todays_races,
    extract_historical_races,
)
from src.extractors.dog_stats_extractor import extract_dog_statistics
from src.processors.feature_engineer import engineer_features, FeatureEngineer
from src.utils.config import config
from src.utils.logging_utils import get_logger, configure_root_logging


class GreyhoundPipeline:
    """Main pipeline orchestrator."""

    def __init__(self) -> None:
        configure_root_logging()
        self.logger = get_logger(__name__)

    # --- Public API -----------------------------------------------------------------
    def run_full_pipeline(self, mode: str = "today", start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        self.logger.info("=" * 60)
        self.logger.info("GREYHOUND RACING DATA PIPELINE STARTED")
        self.logger.info("=" * 60)
        start_time = datetime.now()

        try:
            race_data = self._extract_race_cards(mode=mode, start_date=start_date, end_date=end_date)
            if race_data is None or race_data.empty:
                self.logger.error("Race card extraction produced no data; aborting")
                return False

            dog_stats = self._extract_dog_statistics(race_data)
            if dog_stats is None:
                self.logger.warning("Proceeding without dog statistics (feature coverage reduced)")

            if not self._engineer_features(race_data, dog_stats):
                self.logger.error("Feature engineering failed; aborting")
                return False

            self._generate_summary_report(race_data, dog_stats)
            duration = datetime.now() - start_time
            self.logger.info("Pipeline completed successfully in %s", duration)
            return True
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Pipeline failed: %s", exc, exc_info=True)
            return False
        finally:
            self.logger.info("=" * 60)
            self.logger.info("PIPELINE EXECUTION COMPLETED")
            self.logger.info("=" * 60)

    # --- Internal Steps -------------------------------------------------------------
    def _extract_race_cards(self, mode: str = "today", start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[object]:
        self.logger.info("Step 1: Extracting race cards ...")
        try:
            date_label: Optional[str] = None
            if mode == "historical":
                if not start_date and not end_date:
                    raise ValueError("Historical mode requires start_date and/or end_date (YYYY-MM-DD)")
                if start_date and not end_date:
                    end_date = start_date
                if end_date and not start_date:
                    start_date = end_date
                self.logger.info("Historical extraction for %s to %s", start_date, end_date)
                race_data = extract_historical_races(start_date=start_date, end_date=end_date)
                # Build a date-aware label for the results file name
                if start_date and end_date:
                    if start_date == end_date:
                        date_label = start_date
                    else:
                        date_label = f"{start_date}_to_{end_date}"
            else:
                race_data = extract_todays_races()
            if race_data is None or race_data.empty:
                return None
            if mode == "historical":
                race_file = config.get_file_path("race_results", date_label)
            else:
                race_file = config.get_file_path("race_cards")
            race_data.to_csv(race_file, index=False)
            self.logger.info(
                "Race cards extracted: %d entries | Tracks=%d | Races=%d | File=%s",
                len(race_data),
                race_data['Track'].nunique(),
                race_data['Race_Number'].nunique(),
                race_file,
            )
            return race_data
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Race card extraction error: %s", exc, exc_info=True)
            return None

    def _extract_dog_statistics(self, race_data) -> Optional[object]:  # type: ignore[override]
        self.logger.info("Step 2: Extracting dog statistics ... (best effort)")
        try:
            dog_stats = extract_dog_statistics(race_data)
            if dog_stats is None or dog_stats.empty:
                return None
            stats_file = config.get_file_path("dog_stats")
            dog_stats.to_csv(stats_file, index=False)
            self.logger.info(
                "Dog statistics extracted for %d dogs | File=%s",
                len(dog_stats),
                stats_file,
            )
            return dog_stats
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Dog statistics extraction error: %s", exc, exc_info=True)
            return None

    def _engineer_features(self, race_data, dog_stats) -> bool:  # type: ignore[override]
        self.logger.info("Step 3: Engineering features ...")
        try:
            featured = engineer_features(race_data, dog_stats)
            if featured is None or featured.empty:
                self.logger.error("No features engineered (empty result)")
                return False
            engineer = FeatureEngineer()
            daily_file, historical_file = engineer.create_dual_outputs(featured)
            self.logger.info(
                "Features engineered: rows=%d cols=%d | daily=%s | historical=%s",
                len(featured),
                len(featured.columns),
                daily_file,
                historical_file,
            )
            return True
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Feature engineering error: %s", exc, exc_info=True)
            return False

    def _generate_summary_report(self, race_data, dog_stats) -> None:  # type: ignore[override]
        self.logger.info("Step 4: Generating summary report ...")
        try:
            report = {
                "Pipeline Execution Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Race Cards": {
                    "Total Entries": len(race_data) if race_data is not None else 0,
                    "Unique Tracks": race_data['Track'].nunique() if race_data is not None else 0,
                    "Unique Dogs": race_data['Dog_Name'].nunique() if race_data is not None else 0,
                    "Total Races": race_data[['Track', 'Race_Number']].drop_duplicates().shape[0]
                    if race_data is not None else 0,
                },
                "Dog Statistics": {
                    "Dogs with Stats": len(dog_stats) if dog_stats is not None else 0,
                    "Coverage %": (
                        f"{(len(dog_stats) / race_data['Dog_Name'].nunique() * 100):.1f}%"
                        if (dog_stats is not None and race_data is not None and len(race_data) > 0)
                        else "0%"
                    ),
                },
            }
            for section, details in report.items():
                self.logger.info("%s:", section)
                if isinstance(details, dict):
                    for k, v in details.items():
                        self.logger.info("  %s: %s", k, v)
            summary_file = config.LOGS_DIR / f"summary_{datetime.now().strftime('%Y-%m-%d')}.txt"
            with summary_file.open("w", encoding="utf-8") as fh:
                fh.write("GREYHOUND RACING PIPELINE SUMMARY\n" + "=" * 40 + "\n\n")
                for section, details in report.items():
                    fh.write(f"{section}:\n")
                    if isinstance(details, dict):
                        for k, v in details.items():
                            fh.write(f"  {k}: {v}\n")
                    fh.write("\n")
            self.logger.info("Summary report saved: %s", summary_file)
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Summary generation error: %s", exc, exc_info=True)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Greyhound Racing Data Pipeline")
    parser.add_argument("--mode", choices=["today", "historical"], default="today", help="Extraction mode")
    parser.add_argument("--start-date", dest="start_date", help="Start date YYYY-MM-DD for historical mode", default=None)
    parser.add_argument("--end-date", dest="end_date", help="End date YYYY-MM-DD for historical mode", default=None)
    args = parser.parse_args()

    pipeline = GreyhoundPipeline()
    success = pipeline.run_full_pipeline(mode=args.mode, start_date=args.start_date, end_date=args.end_date)
    if success:
        print("\nPipeline completed successfully. See logs & data/processed.")
    else:
        print("\nPipeline failed. Check logs for details.")
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
