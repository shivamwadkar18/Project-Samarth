# backend.py
import pandas as pd
from pathlib import Path

# Local data folder
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# CSV file paths
RAINFALL_CSV = DATA_DIR / "rainfall_data.csv"
CROP_CSV = DATA_DIR / "crop_production.csv"


# --- Load Rainfall Data ---
def get_rainfall_data():
    """Load and clean rainfall data."""
    if not RAINFALL_CSV.exists():
        print(f"❌ Rainfall CSV not found at: {RAINFALL_CSV}")
        return pd.DataFrame()

    df = pd.read_csv(RAINFALL_CSV)
    print(f"✅ Loaded {len(df)} rainfall records")

    # Standardize column names
    df.columns = df.columns.str.strip().str.upper()

    # Try to rename columns to standard ones
    rename_map = {}
    for col in df.columns:
        if "STATE" in col:
            rename_map[col] = "STATE"
        elif "YEAR" in col:
            rename_map[col] = "YEAR"
        elif "AVG" in col and "RAIN" in col:
            rename_map[col] = "AVG_RAINFALL"
    df.rename(columns=rename_map, inplace=True)

    # Check required columns
    required = ["STATE", "YEAR", "AVG_RAINFALL"]
    if not all(col in df.columns for col in required):
        print("⚠️ Missing required columns in rainfall dataset.")
        return pd.DataFrame()

    # Convert to numeric safely
    df["AVG_RAINFALL"] = pd.to_numeric(df["AVG_RAINFALL"], errors="coerce")
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce")

    return df


# --- Load Crop Data ---
def get_crop_data():
    """Load and clean crop production data."""
    if not CROP_CSV.exists():
        print(f"❌ Crop CSV not found at: {CROP_CSV}")
        return pd.DataFrame()

    df = pd.read_csv(CROP_CSV)
    print(f"✅ Loaded {len(df)} crop records")

    # Normalize columns
    df.columns = df.columns.str.strip().str.title()

    # Rename “State/ Ut Name” → “State”
    for col in df.columns:
        if "State" in col:
            df.rename(columns={col: "State"}, inplace=True)

    # Extract year from last part of column names (e.g., 2009-10)
    df_long = df.melt(id_vars=["State"], var_name="Crop_Year", value_name="Production")

    # Extract crop name and year
    df_long["Year"] = df_long["Crop_Year"].str.extract(r"(\d{4}-\d{2})")
    df_long["Crop"] = (
        df_long["Crop_Year"]
        .str.replace(r"\(.*?\)", "", regex=True)
        .str.replace(r"Food Grains-|Oilseeds-|Cereals-|Production", "", regex=True)
        .str.replace("-", " ")
        .str.strip()
    )

    df_long["Production"] = pd.to_numeric(df_long["Production"], errors="coerce")

    return df_long.dropna(subset=["State", "Crop", "Production"])


# --- Compare Rainfall Function ---
def compare_average_rainfall(state_x, state_y, last_n_years=5):
    df = get_rainfall_data()
    if df.empty:
        return {"error": "Rainfall data not available or invalid."}

    # Filter last N years
    if "YEAR" in df.columns:
        recent_years = sorted(df["YEAR"].dropna().unique())[-last_n_years:]
        df = df[df["YEAR"].isin(recent_years)]

    # Compute averages
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


# --- Top Crops Function ---
def top_crops_in_state(state, top_m=3, last_n_years=5):
    df = get_crop_data()
    if df.empty:
        return {"error": "Crop production data not available or invalid."}

    df_state = df[df["State"].str.lower() == state.lower()]
    if df_state.empty:
        return {"error": f"No crop data found for state: {state}"}

    # Filter last N year-like values (latest seasons)
    if "Year" in df_state.columns:
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
