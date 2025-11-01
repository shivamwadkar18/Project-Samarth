# backend.py
import pandas as pd
from pathlib import Path
import re

# === Folder and file setup ===
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RAINFALL_CSV = DATA_DIR / "rainfall_data.csv"
CROP_CSV = DATA_DIR / "crop_production.csv"


def get_rainfall_data():
    """Load and clean rainfall data with flexible column detection."""
    if not RAINFALL_CSV.exists():
        print(f"❌ Rainfall CSV not found at: {RAINFALL_CSV}")
        return pd.DataFrame()

    df = pd.read_csv(RAINFALL_CSV)
    print(f"✅ Loaded {len(df)} rainfall records")

    # Normalize column names
    df.columns = df.columns.str.strip().str.upper().str.replace(" ", "_")

    # Try to detect the right columns automatically
    state_col = next((c for c in df.columns if "STATE" in c or "UT" in c), None)
    year_col = next((c for c in df.columns if "YEAR" in c), None)
    rain_col = next((c for c in df.columns if "RAIN" in c and "AVG" in c), None)

    if not all([state_col, year_col, rain_col]):
        print(f"⚠️ Missing columns! Found: {df.columns.tolist()}")
        return pd.DataFrame()

    df = df[[state_col, year_col, rain_col]].copy()
    df.columns = ["STATE", "YEAR", "AVG_RAINFALL"]

    # Convert types
    df["AVG_RAINFALL"] = pd.to_numeric(df["AVG_RAINFALL"], errors="coerce")
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce")
    df.dropna(subset=["STATE", "YEAR", "AVG_RAINFALL"], inplace=True)

    print(f"✅ Cleaned rainfall data: {df.shape[0]} rows")
    return df


def compare_average_rainfall(state_x, state_y, last_n_years=5):
    """Compare average rainfall between two states."""
    df = get_rainfall_data()
    if df.empty:
        return {"error": "Rainfall data not available or invalid."}

    # Get recent years safely
    if "YEAR" in df.columns:
        years_sorted = sorted(df["YEAR"].dropna().unique())
        if len(years_sorted) == 0:
            return {"error": "No valid year data found."}
        recent_years = years_sorted[-last_n_years:]
        df = df[df["YEAR"].isin(recent_years)]

    # Compute averages
    avg_x = df[df["STATE"].str.lower().str.strip() == state_x.lower()]["AVG_RAINFALL"].mean()
    avg_y = df[df["STATE"].str.lower().str.strip() == state_y.lower()]["AVG_RAINFALL"].mean()

    if pd.isna(avg_x) or pd.isna(avg_y):
        print(f"⚠️ Could not find rainfall data for {state_x} or {state_y}")
        print("Available states:", df["STATE"].unique().tolist())
        return {"error": "Could not compute rainfall averages for given states."}

    return {
        "State X": state_x,
        "State Y": state_y,
        "Average Rainfall X (mm)": round(avg_x, 2),
        "Average Rainfall Y (mm)": round(avg_y, 2),
        "Years Considered": last_n_years,
    }



# --- Load Crop Data ---
def get_crop_data():
    """Load and clean crop production data."""
    if not CROP_CSV.exists():
        print(f"❌ Crop CSV not found at: {CROP_CSV}")
        return pd.DataFrame()

    df = pd.read_csv(CROP_CSV)
    print(f"✅ Loaded {len(df)} crop records")

    # Standardize and rename
    df.columns = df.columns.str.strip().str.title()
    for col in df.columns:
        if "State" in col:
            df.rename(columns={col: "State"}, inplace=True)

    # Convert to long format
    df_long = df.melt(id_vars=["State"], var_name="Crop_Year", value_name="Production")

    # Extract year (e.g., 2009-10)
    df_long["Year"] = df_long["Crop_Year"].str.extract(r"(\d{4}-\d{2})")

    # Extract and clean crop name
    df_long["Crop"] = (
        df_long["Crop_Year"]
        .str.replace(r"\(.*?\)", "", regex=True)
        .str.replace(r"Food Grains|Oilseeds|Cereals|Production", "", regex=True)
        .str.replace("-", " ")
        .str.strip()
    )
    df_long["Crop"] = df_long["Crop"].apply(lambda x: re.sub(r"\s+\d{4}\s*\d{2}", "", x).strip())

    df_long["Production"] = pd.to_numeric(df_long["Production"], errors="coerce")
    df_long.dropna(subset=["State", "Crop", "Production"], inplace=True)

    return df_long


# --- Compare Rainfall ---
def compare_average_rainfall(state_x, state_y, last_n_years=5):
    df = get_rainfall_data()
    if isinstance(df, dict):
        return df
    if df.empty:
        return {"error": "Rainfall data not available or invalid."}

    # Filter last N years
    recent_years = sorted(df["YEAR"].dropna().unique())[-last_n_years:]
    df = df[df["YEAR"].isin(recent_years)]

    avg_x = df[df["STATE"].str.lower() == state_x.lower()]["AVG_RAINFALL"].mean()
    avg_y = df[df["STATE"].str.lower() == state_y.lower()]["AVG_RAINFALL"].mean()

    if pd.isna(avg_x) or pd.isna(avg_y):
        return {"error": "Could not compute rainfall averages for given states."}

    return {
        "State X": state_x,
        "State Y": state_y,
        "Average Rainfall X (mm)": round(avg_x, 2),
        "Average Rainfall Y (mm)": round(avg_y, 2),
        "Years Considered": last_n_years,
    }


# --- Top Crops ---
def top_crops_in_state(state, top_m=3, last_n_years=5):
    df = get_crop_data()
    if df.empty:
        return {"error": "Crop production data not available or invalid."}

    df_state = df[df["State"].str.lower() == state.lower()]
    if df_state.empty:
        return {"error": f"No crop data found for state: {state}"}

    # Filter last N years (if numeric-like)
    if df_state["Year"].notna().any():
        recent_years = sorted(df_state["Year"].dropna().unique())[-last_n_years:]
        df_state = df_state[df_state["Year"].isin(recent_years)]

    summary = (
        df_state.groupby("Crop")["Production"]
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
    print("Testing backend locally...")
    print(compare_average_rainfall("Maharashtra", "Kerala"))
    print(top_crops_in_state("Punjab"))


