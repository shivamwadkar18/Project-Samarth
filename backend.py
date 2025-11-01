# backend.py
import pandas as pd
import streamlit as st
from pathlib import Path
import re

# Local data folder
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# CSV file paths
RAINFALL_CSV = DATA_DIR / "rainfall_data.csv"
CROP_CSV = DATA_DIR / "crop_production.csv"


# --- Load Rainfall Data ---
def get_rainfall_data():
    """
    Load rainfall data from CSV file.
    Must have columns like: STATE_UT_NAME, YEAR, ANNUAL, etc.
    """
    if not RAINFALL_CSV.exists():
        print(f"❌ Rainfall CSV not found at: {RAINFALL_CSV}")
        return pd.DataFrame()

    df = pd.read_csv(RAINFALL_CSV)
    print(f"✅ Loaded {len(df)} rainfall records")

    # Clean column names
    df.columns = df.columns.str.strip().str.upper()

    # Convert numeric columns
    for col in df.columns:
        if any(x in col for x in ["ANNUAL", "YEAR", "RAIN"]):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# --- Load Crop Data ---
def get_crop_data():
    """
    Load crop production data from CSV file.
    Must have columns like: State, Crop, Year, Production, etc.
    """
    if not CROP_CSV.exists():
        print(f"❌ Crop CSV not found at: {CROP_CSV}")
        return pd.DataFrame()

    df = pd.read_csv(CROP_CSV)
    print(f"✅ Loaded {len(df)} crop production records")

    df.columns = df.columns.str.strip().str.title()
    return df


# --- Compare Rainfall Function ---
def compare_average_rainfall(state_x, state_y, last_n_years=5):
    """
    Compare average rainfall between two states for the last N years.
    Works fully offline using your rainfall_data.csv.
    """
    df = get_rainfall_data()
    if df.empty:
        return {"error": "Rainfall data not available."}

    # Guess year and rainfall columns
    year_col = next((c for c in df.columns if "YEAR" in c.upper()), None)
    rain_col = next((c for c in df.columns if "ANNUAL" in c.upper()), None)
    state_col = next((c for c in df.columns if "STATE" in c.upper()), None)

    if not all([year_col, rain_col, state_col]):
        return {"error": "Missing expected columns in rainfall dataset."}

    # Filter last N years
    if df[year_col].dtype == "float64" or df[year_col].dtype == "int64":
        recent_years = sorted(df[year_col].dropna().unique())[-last_n_years:]
        df = df[df[year_col].isin(recent_years)]

    # Compare both states
    avg_x = df[df[state_col].str.lower() == state_x.lower()][rain_col].mean()
    avg_y = df[df[state_col].str.lower() == state_y.lower()][rain_col].mean()

    return {
        "State X": state_x,
        "State Y": state_y,
        "Average Rainfall X (mm)": round(avg_x, 2) if pd.notna(avg_x) else "N/A",
        "Average Rainfall Y (mm)": round(avg_y, 2) if pd.notna(avg_y) else "N/A",
        "Years Considered": last_n_years,
    }


# --- Top Crops Function ---
def top_crops_in_state(state, top_m=3, last_n_years=5):
    """
    Return top crops by production in a given state.
    Works fully offline using your crop_production.csv.
    """
    df = get_crop_data()
    if df.empty:
        return {"error": "Crop production data not available."}

    # Normalize columns
    state_col = next((c for c in df.columns if "State" in c), None)
    crop_col = next((c for c in df.columns if "Crop" in c), None)
    year_col = next((c for c in df.columns if "Year" in c), None)
    prod_col = next((c for c in df.columns if "Production" in c), None)

    if not all([state_col, crop_col, year_col, prod_col]):
        return {"error": "Required columns missing in crop data."}

    # Filter last N years and state
    if df[year_col].dtype in ["int64", "float64"]:
        recent_years = sorted(df[year_col].dropna().unique())[-last_n_years:]
        df = df[df[year_col].isin(recent_years)]

    df_state = df[df[state_col].str.lower() == state.lower()]
    if df_state.empty:
        return {"error": f"No data found for state: {state}"}

    # Group by crop
    summary = (
        df_state.groupby(crop_col)[prod_col]
        .sum()
        .sort_values(ascending=False)
        .head(top_m)
        .reset_index()
    )

    return {
        "State": state,
        "Top Crops": summary.to_dict(orient="records"),
        "Years Considered": last_n_years,
    }


if __name__ == "__main__":
    print("Testing locally...")
    print(compare_average_rainfall("Maharashtra", "Kerala"))
    print(top_crops_in_state("Punjab"))
