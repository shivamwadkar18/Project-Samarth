
# backend.py
import os
from dotenv import load_dotenv
import pandas as pd
import requests
from pathlib import Path
import re

load_dotenv()

# Directory for storing CSV files
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RAINFALL_CSV = DATA_DIR / "rainfall_data.csv"
CROP_CSV = DATA_DIR / "crop_production.csv"

# Resource IDs and download URLs
RAINFALL_RESOURCE_ID = os.getenv("RAINFALL_RESOURCE_ID")
CROP_PRODUCTION_RESOURCE_ID = os.getenv("CROP_PRODUCTION_RESOURCE_ID")

# Direct download URLs (CSV format - more reliable)
RAINFALL_DOWNLOAD_URL = f"https://data.gov.in/api/datastore/resource.json?resource_id={RAINFALL_RESOURCE_ID}&format=csv&limit=50000"
CROP_DOWNLOAD_URL = f"https://data.gov.in/api/datastore/resource.json?resource_id={CROP_PRODUCTION_RESOURCE_ID}&format=csv&limit=50000"


def download_csv_with_retry(url, output_path, max_retries=3, timeout=60):
    """
    Download CSV with retry logic and longer timeout
    """
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries}: Downloading from {url[:100]}...")
            response = requests.get(url, timeout=timeout, stream=True)
            
            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"✓ Successfully downloaded to {output_path}")
                return True
            else:
                print(f"✗ HTTP {response.status_code}: {response.text[:200]}")
                
        except requests.exceptions.Timeout:
            print(f"✗ Timeout on attempt {attempt + 1}")
        except Exception as e:
            print(f"✗ Error: {e}")
    
    return False


def download_datasets_if_missing():
    """
    Download datasets if they don't exist locally
    """
    if not RAINFALL_CSV.exists():
        print(f"\n[Downloading Rainfall Data]")
        if not download_csv_with_retry(RAINFALL_DOWNLOAD_URL, RAINFALL_CSV):
            print("⚠ Failed to download rainfall data. Using manual download instructions below.")
            print(f"Please download manually from:")
            print(f"https://data.gov.in/resource/daily-district-wise-rainfall-data")
            print(f"Save as: {RAINFALL_CSV}")
            return False
    
    if not CROP_CSV.exists():
        print(f"\n[Downloading Crop Production Data]")
        if not download_csv_with_retry(CROP_DOWNLOAD_URL, CROP_CSV):
            print("⚠ Failed to download crop data. Using manual download instructions below.")
            print(f"Please download manually from:")
            print(f"https://data.gov.in/resource/state-ut-wise-production-principal-crops-2009-10-2015-16")
            print(f"Save as: {CROP_CSV}")
            return False
    
    return True


def get_rainfall_data():
    """
    Load rainfall data from CSV file
    """
    if not RAINFALL_CSV.exists():
        print(f"Rainfall CSV not found at {RAINFALL_CSV}")
        print("Attempting to download...")
        if not download_datasets_if_missing():
            return pd.DataFrame()
    
    try:
        df = pd.read_csv(RAINFALL_CSV)
        print(f"✓ Loaded {len(df)} rainfall records from local CSV")
        print(f"Columns: {list(df.columns)}")
        
        # Convert numeric columns
        for col in df.columns:
            if 'year' in col.lower() or 'rain' in col.lower():
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    except Exception as e:
        print(f"Error loading rainfall CSV: {e}")
        return pd.DataFrame()


def get_crop_data():
    """
    Load crop production data from CSV file
    The data is in wide format: rows are states, columns are crops/years
    """
    if not CROP_CSV.exists():
        print(f"Crop CSV not found at {CROP_CSV}")
        print("Attempting to download...")
        if not download_datasets_if_missing():
            return pd.DataFrame()
    
    try:
        df = pd.read_csv(CROP_CSV)
        print(f"✓ Loaded {len(df)} crop production records from local CSV")
        print(f"Columns: {list(df.columns)[:10]}...")  # Show first 10 columns
        
        # Convert all numeric columns (except state name)
        for col in df.columns:
            if col != 'State/ UT Name' and 'state' not in col.lower() and 'name' not in col.lower():
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    except Exception as e:
        print(f"Error loading crop CSV: {e}")
        return pd.DataFrame()


def compare_average_rainfall(state_x, state_y, last_n_years=5):
    """
    Compare average rainfall between two states
    """
    df = get_rainfall_data()
    
    if df.empty:
        return {
            "error": "No rainfall data available",
            "suggestion": "Please download the dataset manually and place it in the 'data' folder",
            "download_link": "https://data.gov.in/resource/daily-district-wise-rainfall-data"
        }
    
    print(f"Available columns: {list(df.columns)}")
    
    # Find relevant columns (case-insensitive)
    state_col = next((c for c in df.columns if 'state' in c.lower()), None)
    year_col = next((c for c in df.columns if 'year' in c.lower()), None)
    rain_col = next((c for c in df.columns if 'rain' in c.lower() or 'annual' in c.lower()), None)
    
    if not all([state_col, year_col, rain_col]):
        return {
            "error": "Required columns not found in dataset",
            "available_columns": list(df.columns),
            "needed": ["state", "year", "rainfall"]
        }
    
    # Get unique states
    unique_states = df[state_col].unique()
    print(f"Available states: {list(unique_states[:20])}")
    
    # Get recent years
    valid_years = df[year_col].dropna()
    if len(valid_years) == 0:
        return {"error": "No valid year data found"}
    
    years = sorted(valid_years.unique())[-last_n_years:]
    print(f"Analyzing years: {years}")
    
    # Filter data
    df_x = df[(df[state_col].str.lower().str.strip() == state_x.lower().strip()) & 
              (df[year_col].isin(years))]
    df_y = df[(df[state_col].str.lower().str.strip() == state_y.lower().strip()) & 
              (df[year_col].isin(years))]
    
    if df_x.empty:
        return {
            "error": f"No data found for state: {state_x}",
            "available_states_sample": list(unique_states[:30])
        }
    
    if df_y.empty:
        return {
            "error": f"No data found for state: {state_y}",
            "available_states_sample": list(unique_states[:30])
        }
    
    avg_x = df_x[rain_col].mean()
    avg_y = df_y[rain_col].mean()
    
    return {
        "state_x": state_x,
        "state_y": state_y,
        "avg_rainfall_x": round(avg_x, 2) if pd.notna(avg_x) else "N/A",
        "avg_rainfall_y": round(avg_y, 2) if pd.notna(avg_y) else "N/A",
        "years_analyzed": list(years),
        "comparison": f"Average rainfall in {state_x}: {avg_x:.2f} mm, {state_y}: {avg_y:.2f} mm over last {last_n_years} years",
        "data_source": f"Local CSV file (originally from data.gov.in resource {RAINFALL_RESOURCE_ID})"
    }


def top_crops_in_state(state, top_m=3, last_n_years=5):
    """
    Get top crops by production in a state.
    Data format: rows = states, columns = crop production values by year
    """
    df = get_crop_data()
    
    if df.empty:
        return {
            "error": "No crop production data available",
            "suggestion": "Please download the dataset manually and place it in the 'data' folder",
            "download_link": "https://data.gov.in/resource/state-ut-wise-production-principal-crops-2009-10-2015-16"
        }
    
    print(f"Available columns: {list(df.columns)[:10]}...")
    
    # Find state name column
    state_col = next((c for c in df.columns if 'state' in c.lower() or 'name' in c.lower()), None)
    
    if not state_col:
        return {
            "error": "State column not found in dataset",
            "available_columns": list(df.columns)[:20]
        }
    
    # Get unique states for debugging
    unique_states = df[state_col].unique()
    print(f"Available states (first 10): {list(unique_states[:10])}")
    
    # Filter for the requested state (case-insensitive)
    df_state = df[df[state_col].str.lower().str.strip() == state.lower().strip()]
    
    if df_state.empty:
        return {
            "error": f"No data found for state: {state}",
            "available_states_sample": list(unique_states[:30])
        }
    
    # Extract crop production data
    
    crop_totals = {}
    
    # Process each column to extract crop and production
    for col in df.columns:
        if col == state_col:
            continue
            
        # Parse column name to extract crop name and year
        year_match = None
        if '-' in col:
            parts = col.split('-')
            # Look for year patterns like "2009-10", "2014-15", etc.
            for i, part in enumerate(parts):
                if len(part) == 4 and part.isdigit():
                    year_str = part
                    try:
                        year = int(year_str[:4])
                        year_match = year
                        break
                    except:
                        pass
        
        crop_name = col
        
        # Clean up crop name - remove production indicators
        if '-(Production' in crop_name:
            crop_name = crop_name.split('-(Production')[0]
        elif '-(Th. tonnes)' in crop_name:
            crop_name = crop_name.split('-(Th. tonnes)')[0]
        elif '-(000' in crop_name:
            crop_name = crop_name.split('-(000')[0]
        
        # Remove year suffix if present
        if year_match:
            crop_name = re.sub(r'-\d{4}(-\d{2})?$', '', crop_name)
        
        # Clean up prefixes
        if 'Food grains (cereals)-' in crop_name:
            crop_name = crop_name.replace('Food grains (cereals)-', '')
        elif 'Food grains(pulses)-' in crop_name:
            crop_name = crop_name.replace('Food grains(pulses)-', '')
        elif 'Oilseeds-' in crop_name:
            crop_name = crop_name.replace('Oilseeds-', '')
        
        # Remove trailing dashes and clean up
        crop_name = crop_name.strip('-').strip()
        
        if not crop_name or crop_name.lower() in ['state', 'name', 'total', 'all-india', 'all india']:
            continue
        
        # Only include recent years (if we can determine the year)
        if year_match and last_n_years:
            max_year = 2015  # Latest year in the dataset
            min_year = max_year - last_n_years + 1
            if year_match < min_year:
                continue
        
        # Get production value
        try:
            value = df_state[col].iloc[0]
            if pd.notna(value) and value != 'NA' and value != '#':
                value = float(value)
                if value > 0:
                    # Add to crop totals
                    if crop_name in crop_totals:
                        crop_totals[crop_name] += value
                    else:
                        crop_totals[crop_name] = value
        except:
            continue
    
    if not crop_totals:
        return {
            "error": f"No valid crop production data found for {state}",
            "note": "The dataset may not contain recent data or the state name may not match exactly"
        }
    
    # Sort crops by total production and get top M
    sorted_crops = sorted(crop_totals.items(), key=lambda x: x[1], reverse=True)
    top_crops = sorted_crops[:top_m]
    
    # Format results
    results = [
        {
            "crop": crop,
            "total_production": round(production, 2),
            "unit": "Thousand Tonnes"
        }
        for crop, production in top_crops
    ]
    
    return {
        "state": state,
        "period": f"Last {last_n_years} years (2009-2015)",
        "top_crops": results,
        "data_source": f"Local CSV file (originally from data.gov.in resource {CROP_PRODUCTION_RESOURCE_ID})"
    }


# Try to download datasets on module load
if __name__ == "__main__":
    print("Checking for required datasets...")
    download_datasets_if_missing()