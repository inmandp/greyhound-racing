# Code Review Report

Project: Greyhound Racing Data Pipeline  
Date: 2025-08-19  
Reviewer: Automated Assistant

## 1. Executive Summary
The project establishes a clear end-to-end pipeline (extract → enrich → feature engineer → persist) with a sensible directory structure. Core improvement areas: (1) logging consistency, (2) robustness of Selenium scraping, (3) correctness & completeness of dog statistics parsing, (4) reliance on magic numbers not tied to configuration, (5) data validation & testing gaps, and (6) placeholder feature values that risk misleading downstream models.

## 2. High-Priority Issues & Recommendations
| Priority | Area | Issue | Impact | Recommendation |
|----------|------|-------|--------|----------------|
| Critical | Duplicate Detection | `_check_for_duplicates` uses last N runner rows instead of race groupings, causing false positives | Unnecessary retries, latency | Track recent race-level dog sets (deque of sets) and compute overlap ratio per race |
| Critical | Feature Correctness | Placeholder dog stats fields (Places, times, form) silently treated as true zeros | Model bias / misinformation | Add explicit `Has_Dog_Stats` & missing flags; differentiate NaN vs 0 |
| High | Performance & Accuracy | `Race_Size` via row-wise `apply` with grouped Series | O(n²) scaling | Use `groupby.transform('count')` |
| High | Time Features | `Best_Time` default scalar creates uniform `Time_Per_Meter` | Low feature variance | Parse real times or set NaN then impute; add `Has_Time_Data` flag |
| High | Logging | Mixed `print` and `logging` across modules | Fragmented observability | Standardize logger per module; pipeline config drives level/format |
| High | Config Usage | Hard-coded sleeps, thresholds not using `Config` | Hard to tune | Pass config into extractors; remove inline literals |
| High | Scraping Robustness | Heavy `sleep` reliance; minimal explicit waits | Flaky extraction | Use `WebDriverWait` for key elements; retry with backoff |
| High | Error Handling | Broad bare `except:` blocks swallow structural changes | Silent failures | Narrow exception types; log warnings; escalate critical parsing errors |

## 3. Medium-Priority Improvements
1. Dog stats rate limiting: implement token bucket to smooth request cadence.
2. Historical deduplication: use stable key (`Track`, `Race_Number`, `Dog_Name`, `Race_Time`) plus run date; avoid substring date filtering on `Feature_Creation_Date`.
3. Schema validation: enforce expected columns/dtypes pre- and post-feature engineering.
4. Unit tests: add fixtures for HTML parsing, tests for feature set integrity, duplicate detection correctness, historical append logic.
5. Refactor large methods (`_extract_from_race_cards`) into smaller pure functions for parsing vs navigation.
6. Consolidate trap/track advantage constants exclusively in `config.FEATURE_SETTINGS`.
7. Add structured metrics (counts, retry stats) to summary report JSON section.

## 4. Low-Priority / Style
- Replace emojis in core library logs; keep only in CLI output.
- Add type hints to internal helper methods (returns of dict schemas, DataFrame shapes where feasible).
- Prefer f-strings consistently; remove commented-out or placeholder code where not planned.
- Use `urljoin` for building race URLs robustly.

## 5. Potential Bugs / Edge Cases
| Area | Scenario | Risk | Mitigation |
|------|----------|------|-----------|
| Distance Parsing | Missing distance text | NaN categories | Default bucket or explicit `Unknown_Distance` |
| Duplicate Detection | Common dog repetition across adjacent races | False positive | Overlap ratio threshold with race-level sets |
| Rate Limiting | Parallel dog stats threads burst | 429 loops | Central rate limiter / staggered start |
| Time Division | Zero/NaN distance | Inf speed scores | Guard & set fallback + flag |
| Historical Append | Multi-run same day | Stale earlier rows kept | Deduplicate on composite key with latest timestamp |

## 6. Suggested Refactor Path
Phase 1 (Quick Wins): logging unification, config parameterization, race size optimization, duplicate detection fix, add missing flags, schema validation utility, minimal tests.
Phase 2: robust waits & retry policy, richer dog stats parsing, rate limiter, improved historical append logic.
Phase 3: modular extractor architecture, performance profiling, data quality reporting, CI integration.

## 7. Implementation Sketches
### Duplicate Detection (Race-Level)
```python
from collections import deque
recent_races = deque(maxlen=6)  # store sets of dog names per race
...
current_set = {r['Dog_Name'] for r in runners}
overlap = max((len(current_set & prev)/len(current_set) for prev in recent_races), default=0)
if overlap > config.EXTRACTION_SETTINGS.get('duplicate_overlap_threshold', 0.6):
    # trigger cache bust / retry
recent_races.append(current_set)
```

### Race Size Efficient Computation
```python
df['Race_Size'] = df.groupby(['Track','Race_Number'])['Dog_Name'].transform('count')
```

### Time Feature Guard
```python
df['Best_Time'] = pd.to_numeric(df.get('Best_Time'), errors='coerce')
df['Time_Per_Meter'] = df['Best_Time'] / df['Distance_Meters']
mask = ~np.isfinite(df['Time_Per_Meter'])
df.loc[mask, 'Time_Per_Meter'] = df['Time_Per_Meter'].median()
df['Has_Time_Data'] = (~mask).astype(int)
```

## 8. Proposed New Tests
| Test | Purpose |
|------|---------|
| `test_duplicate_detection_overlaps` | Validates overlap threshold logic |
| `test_feature_engineer_columns` | Ensures required feature columns present & non-null policy |
| `test_historical_append_dedup` | Confirms older duplicate rows replaced |
| `test_distance_parsing_missing` | Distance missing handled gracefully |
| `test_config_file_override` | Custom config merges and applied |

## 9. Data Validation Checklist (to implement)
- Column presence: Track, Race_Number, Dog_Name, Trap, Grade, Distance.
- Trap numeric in 0–6; flag anomalies.
- Distance_Meters parsed ratio > 95%; log otherwise.
- No duplicated (Track, Race_Number, Dog_Name) combinations after merge.
- Feature value ranges (Win_Rate 0–1, Success_Rate 0–1.5 expected, Speed_Score finite).

## 10. Metrics to Log
- races_total, runners_total
- cache_bust_light_count, cache_bust_aggressive_count
- dog_stats_requests, dog_stats_success, dog_stats_fail_403, dog_stats_fail_429
- duplicate_race_retries
- feature_rows_output, feature_columns_output

## 11. Risk Assessment & Mitigation Timeline
| Week | Focus | Outcome |
|------|-------|---------|
| 1 | Quick wins + tests | Stabilized base, measurable reliability |
| 2 | Robust waits + dog stats parsing enhancements | Reduced flakiness, richer features |
| 3 | Rate limiting + historical dedup + metrics | Scalable extraction with observability |
| 4 | Modular refactor + documentation | Easier future feature additions |

## 12. Documentation Updates Needed
- Clarify placeholder vs real dog stats features.
- Add configuration override usage examples (load + merge).
- Describe data validation & metrics once added.

## 13. Summary of Actions (Immediate)
1. Unify logging & remove prints.
2. Parameterize extractor behavior from `Config`.
3. Optimize `Race_Size` & fix duplicate detection logic.
4. Add missing data flags and guard time features.
5. Create initial test suite with fixtures.
6. Improve historical append strategy.

---
This document can be iteratively updated as improvements land. Let me know if you want me to begin implementing the Phase 1 quick wins.
