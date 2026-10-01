# Data Issues Found: yellow_tripdata_2025-01

Explored on 2026-10-01 with DuckDB (`scripts/explore_phase2*.py`).
Total rows: 3,475,226.

| # | Issue | Count | Share | Proposed handling (to be finalized in Phase 6) |
|---|-------|-------|-------|------------------------------------------------|
| 1 | Negative `fare_amount` | 144,118 | ~4.1% | Likely refunds/adjustments. Quarantine with reason code, do not silently drop |
| 2 | Dropoff before pickup | 124 | ~0.004% | Reject (invalid record) |
| 3 | Pickup outside the file's month | 22 | ~0.0006% | Reject, since the row belongs to another month |
| 4 | NULL `passenger_count` | 540,149 | ~15.5% | Keep and flag. Five columns are NULL together, so this is a source pattern |
| 5 | `trip_distance` = 0 | 90,893 | ~2.6% | Flag, decide later whether to keep |
| 6 | `trip_distance` > 100 miles | 162 | ~0.005% | Quarantine. Max value is 276,423.57 miles, which is impossible |

## Negative fares by payment_type
Type 0: 84,822. Type 4: 37,405. Type 2: 13,802. Type 3: 8,075. Type 1: 14.
(Code meanings to be confirmed against the TLC data dictionary.)

## Schema observations
- `Airport_fee` is capitalized differently from the other column names.
- `cbd_congestion_fee` exists in this file and will not exist in older files (schema drift).
- Mixed integer types: `VendorID` is INTEGER, while `passenger_count`, `RatecodeID` and `payment_type` are BIGINT.

## Open question
Where do the NULL-passenger rows come from (vendor / payment type)? See `explore_phase2c.py`.

## Answer: source of the NULL passenger_count rows
All 540,149 rows have `payment_type = 0` (VendorID 2: 451,456; VendorID 1: 88,204;
VendorID 6: 489). The same payment type also holds the most negative fares (84,822).
This is a distinct trip category, not random missing data. Decision: keep these rows
and add a flag column in the cleaning step, rather than dropping them.
(Meaning of payment_type 0 to be confirmed against the TLC data dictionary.)
