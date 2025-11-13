#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Load + light-clean + merge DfT Road Safety tables into analysis-ready Parquet files.

Outputs (under data/processed/):
  - collisions_clean.parquet         (cleaned collisions; PK: collision_index)
  - vehicles_enriched.parquet        (vehicles + selected collision columns)
  - casualties_enriched.parquet      (casualties + selected collision columns, and vehicle_type if available)

Run:
    python scripts/load_merge.py

Notes:
- Uses the same auto-detection pattern as premerge_inspect.py.
- Respects UK_RS_DATA_DIR to override data location.
- Keeps cleaning minimal to avoid overfitting; we only fix the stuff that breaks joins/EDA.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

# ------------------ Config ------------------
RAW_BASE_REL = Path("data/raw")
PROCESSED_REL = Path("data/processed")

# Minimal domain rules (safe & general)
UK_LAT_BOUNDS = (49.8, 60.9)
UK_LON_BOUNDS = (-8.7, 1.8)
ALLOWED_SPEEDS = {5, 10, 15, 20, 30, 40, 50, 60, 70, 80}
SENTINELS = {-1, 97, 98, 99, 997, 998, 999}

# Keys and expected files
COLLISION_KEY = "collision_index"
VEH_KEYS = [COLLISION_KEY, "vehicle_reference"]
CAS_KEYS = [COLLISION_KEY, "vehicle_reference", "casualty_reference"]

EXPECTED = {
    "collisions": ["Collisions.csv"],
    "vehicles":   ["Vehicles.csv"],
    "casualties": ["Casualties.csv"],
}

# Carry these collision columns into child tables (keeps width reasonable)
COLLISION_CONTEXT = [
    "collision_index", "collision_year", "collision_ref_no",
    "latitude", "longitude", "police_force", "collision_severity",
    "number_of_vehicles", "number_of_casualties", "speed_limit",
    "light_conditions", "weather_conditions", "urban_or_rural_area",
]

# ------------------ Utils ------------------
def repo_root(start: Path | None = None) -> Path:
    cur = start or Path.cwd()
    for p in [cur, *cur.parents]:
        if (p / ".git").exists() or (p / "README.md").exists():
            return p
    return cur

def find_dataset_root(raw_base: Path) -> Path | None:
    """Pick the first child folder that contains any expected file."""
    if not raw_base.exists():
        return None
    for d in raw_base.iterdir():
        if d.is_dir():
            hits = 0
            for names in EXPECTED.values():
                for nm in names:
                    if d.joinpath(nm).exists():
                        hits += 1
                        break
            if hits >= 1:
                return d
    return None

def find_file(root: Path, names: List[str]) -> Path | None:
    """Exact match first, then substring (e.g., Vehicles_2024.csv)."""
    all_csvs = list(root.rglob("*.csv")) + list(root.rglob("*.CSV"))
    for nm in names:
        for p in all_csvs:
            if p.name.lower() == nm.lower():
                return p
    for nm in names:
        stem = nm.lower().replace(".csv", "")
        for p in all_csvs:
            if stem in p.name.lower():
                return p
    return None

def read_csv_any(path: Path) -> pd.DataFrame:
    """Use Arrow if available; fallback to default engine."""
    try:
        return pd.read_csv(path, engine="pyarrow")
    except Exception:
        return pd.read_csv(path)

def coerce_sentinels_to_na(df: pd.DataFrame) -> pd.DataFrame:
    """Turn DfT sentinel codes into proper NA for integer-like columns."""
    for c in df.columns:
        if pd.api.types.is_integer_dtype(df[c]):
            df[c] = df[c].replace(list(SENTINELS), pd.NA)
    return df

def within_uk_mask(lat: pd.Series, lon: pd.Series) -> pd.Series:
    """Keep rows inside the UK bounding box; do not drop rows with NA coords."""
    ok_lat = lat.between(UK_LAT_BOUNDS[0], UK_LAT_BOUNDS[1], inclusive="both")
    ok_lon = lon.between(UK_LON_BOUNDS[0], UK_LON_BOUNDS[1], inclusive="both")
    out_of_box = (~lat.isna()) & (~lon.isna()) & (~(ok_lat & ok_lon))
    return ~out_of_box  # True = keep

# ------------------ Pipeline ------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    ROOT = repo_root()
    RAW_BASE = ROOT / RAW_BASE_REL
    PROCESSED = ROOT / PROCESSED_REL
    PROCESSED.mkdir(parents=True, exist_ok=True)

    override = os.getenv("UK_RS_DATA_DIR")
    DATA_ROOT = Path(override) if override else find_dataset_root(RAW_BASE)
    if not DATA_ROOT or not DATA_ROOT.exists():
        raise FileNotFoundError(
            f"Could not find raw data under {RAW_BASE}. "
            f"Unzip into e.g. data/raw/dft_road_safety_last_5_years/ "
            f"or set UK_RS_DATA_DIR to the directory."
        )

    acc_path = find_file(DATA_ROOT, EXPECTED["collisions"])
    veh_path = find_file(DATA_ROOT, EXPECTED["vehicles"])
    cas_path = find_file(DATA_ROOT, EXPECTED["casualties"])
    missing = [nm for nm, p in [("Collisions.csv", acc_path), ("Vehicles.csv", veh_path), ("Casualties.csv", cas_path)] if p is None]
    if missing:
        raise FileNotFoundError(f"Missing expected files: {', '.join(missing)}")

    logging.info(f"Loading\n- {acc_path}\n- {veh_path}\n- {cas_path}")

    acc = read_csv_any(acc_path)
    veh = read_csv_any(veh_path)
    cas = read_csv_any(cas_path)

    # ---- Light clean: sentinels -> NA, key dtypes, dedupe, geo ----
    acc = coerce_sentinels_to_na(acc)
    veh = coerce_sentinels_to_na(veh)
    cas = coerce_sentinels_to_na(cas)

    # Ensure key columns are strings (prevents join dtype mismatches)
    for df in (acc, veh, cas):
        for k in [COLLISION_KEY, "vehicle_reference", "casualty_reference"]:
            if k in df.columns:
                df[k] = df[k].astype(str)

    # Drop exact duplicates on key combos (safe & minimal)
    if set(VEH_KEYS).issubset(veh.columns):
        before = len(veh)
        veh = veh.drop_duplicates(subset=VEH_KEYS, keep="first")
        logging.info(f"Vehicles: dropped {before - len(veh)} duplicate key-rows")

    if set(CAS_KEYS).issubset(cas.columns):
        before = len(cas)
        cas = cas.drop_duplicates(subset=CAS_KEYS, keep="first")
        logging.info(f"Casualties: dropped {before - len(cas)} duplicate key-rows")

    # Gentle UK geo filter (only drop clearly wrong points; keep NA coords)
    if {"latitude", "longitude"}.issubset(acc.columns):
        keep = within_uk_mask(acc["latitude"], acc["longitude"])
        dropped = int((~keep).sum())
        if dropped:
            logging.info(f"Collisions: dropping {dropped} rows outside UK bounds {UK_LAT_BOUNDS}/{UK_LON_BOUNDS}")
        acc = acc.loc[keep].reset_index(drop=True)

    # Normalize speed_limit domain (set weird values to NA; don't drop)
    if "speed_limit" in acc.columns:
        s = pd.to_numeric(acc["speed_limit"], errors="coerce")
        acc.loc[~s.isin(list(ALLOWED_SPEEDS)), "speed_limit"] = pd.NA

    # ---- Collision context for enriching children ----
    ctx_cols = [c for c in COLLISION_CONTEXT if c in acc.columns]
    acc_ctx = acc[ctx_cols].copy()

    # ---- Save collisions (clean) ----
    out_acc = PROCESSED / "collisions_clean.parquet"
    acc.to_parquet(out_acc, index=False)
    logging.info(f"Saved {out_acc} ({len(acc):,} rows)")

    # ---- Enrich vehicles with collision context ----
    if COLLISION_KEY in veh.columns:
        veh_en = veh.merge(acc_ctx, on=COLLISION_KEY, how="left", validate="many_to_one")
    else:
        veh_en = veh.copy()

    out_veh = PROCESSED / "vehicles_enriched.parquet"
    veh_en.to_parquet(out_veh, index=False)
    logging.info(f"Saved {out_veh} ({len(veh_en):,} rows)")

    # ---- Enrich casualties with collision context (+optional vehicle attribute) ----
    cas_en = cas.merge(acc_ctx, on=COLLISION_KEY, how="left", validate="many_to_one")
    if {"vehicle_type"}.issubset(veh.columns):  # handy extra field, if present
        veh_pick = veh[VEH_KEYS + ["vehicle_type"]].drop_duplicates(VEH_KEYS)
        cas_en = cas_en.merge(veh_pick, on=VEH_KEYS, how="left", validate="many_to_one")

    out_cas = PROCESSED / "casualties_enriched.parquet"
    cas_en.to_parquet(out_cas, index=False)
    logging.info(f"Saved {out_cas} ({len(cas_en):,} rows)")

    # ---- Post-merge sanity logs ----
    assert veh_en[COLLISION_KEY].notna().all()
    assert cas_en[COLLISION_KEY].notna().all()

    if "number_of_vehicles" in acc.columns and "vehicle_reference" in veh.columns:
        veh_counts = veh.groupby(COLLISION_KEY, dropna=False)["vehicle_reference"].nunique()
        mismatches = (acc.set_index(COLLISION_KEY)["number_of_vehicles"].sub(veh_counts, fill_value=0) != 0).sum()
        logging.info(f"Vehicle-count parity mismatches: {int(mismatches)}")

    if "number_of_casualties" in acc.columns and "casualty_reference" in cas.columns:
        cas_counts = cas.groupby(COLLISION_KEY, dropna=False)["casualty_reference"].nunique()
        mismatches = (acc.set_index(COLLISION_KEY)["number_of_casualties"].sub(cas_counts, fill_value=0) != 0).sum()
        logging.info(f"Casualty-count parity mismatches: {int(mismatches)}")

    logging.info("Load + merge complete.")

if __name__ == "__main__":
    main()
