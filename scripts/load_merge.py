from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "dft_road_safety_last_5_years"
OUT = ROOT / "data" / "processed"
OUT.mkdir(parents=True, exist_ok=True)

# --- helpers ---
def read_any(p, usecols=None, dtype=None):
    try:
        return pd.read_csv(p, engine="pyarrow", usecols=usecols, dtype=dtype)
    except Exception:
        return pd.read_csv(p, usecols=usecols, dtype=dtype)

# --- columns we actually need for the first “star schema” style build ---
collisions_cols = [
    "collision_index","collision_year","longitude","latitude","police_force",
    "collision_severity","number_of_vehicles","number_of_casualties",
    "day_of_week","local_authority_district","urban_or_rural_area",
    "light_conditions","road_type","speed_limit","weather_conditions"
]

vehicles_cols = [
    "collision_index","vehicle_reference","vehicle_type","vehicle_manoeuvre",
    "age_of_driver","age_band_of_driver","engine_capacity_cc","propulsion_code",
    "age_of_vehicle","generic_make_model","driver_imd_decile"
]

casualties_cols = [
    "collision_index","vehicle_reference","casualty_reference","casualty_class",
    "sex_of_casualty","age_of_casualty","age_band_of_casualty","casualty_severity",
    "casualty_type","casualty_imd_decile"
]

# code-like fields -> category to save RAM
cat_fields = {
    "police_force","collision_severity","day_of_week","local_authority_district",
    "urban_or_rural_area","light_conditions","road_type","speed_limit","weather_conditions",
    "vehicle_type","vehicle_manoeuvre","age_band_of_driver","propulsion_code",
    "driver_imd_decile","casualty_class","sex_of_casualty","age_band_of_casualty",
    "casualty_severity","casualty_type","casualty_imd_decile"
}

def make_dtype(cols):
    dt = {}
    for c in cols:
        if c in cat_fields:
            dt[c] = "category"
        elif c.endswith("_year") or c in {"number_of_vehicles","number_of_casualties","age_of_vehicle","age_of_driver","engine_capacity_cc"}:
            dt[c] = "Int32"
    return dt

# --- load ---
collisions = read_any(RAW/"Collisions.csv", usecols=collisions_cols, dtype=make_dtype(collisions_cols))
vehicles   = read_any(RAW/"Vehicles.csv",   usecols=vehicles_cols,   dtype=make_dtype(vehicles_cols))
casualties = read_any(RAW/"Casualties.csv", usecols=casualties_cols, dtype=make_dtype(casualties_cols))

# --- basic keys & integrity checks ---
assert collisions["collision_index"].is_unique, "collision_index should be unique in Collisions"
missing_v = vehicles[~vehicles["collision_index"].isin(collisions["collision_index"])]
missing_c = casualties[~casualties["collision_index"].isin(collisions["collision_index"])]
if len(missing_v):
    print(f"Warning: {len(missing_v)} vehicle rows have no matching collision")
if len(missing_c):
    print(f"Warning: {len(missing_c)} casualty rows have no matching collision")

# --- merges ---
veh_wide = vehicles.merge(collisions, on="collision_index", how="left", validate="many_to_one")
cas_wide = casualties.merge(vehicles[["collision_index","vehicle_reference","vehicle_type"]], 
                            on=["collision_index","vehicle_reference"], how="left", validate="many_to_one") \
                     .merge(collisions, on="collision_index", how="left", validate="many_to_one")

# --- save ---
collisions.to_parquet(OUT/"collisions.parquet", index=False)
vehicles.to_parquet(OUT/"vehicles.parquet", index=False)
casualties.to_parquet(OUT/"casualties.parquet", index=False)
veh_wide.to_parquet(OUT/"vehicles_with_collision.parquet", index=False)
cas_wide.to_parquet(OUT/"casualties_with_collision.parquet", index=False)

print("Wrote:", [p.name for p in OUT.glob("*.parquet")])
