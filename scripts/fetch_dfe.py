import os
import sys
import pandas as pd
import requests
from datetime import datetime

SOURCE_URL = os.getenv("DFE_SOURCE_URL", "https://raw.githubusercontent.com/dfe-analytical-services/attendance-data-dashboard/refs/heads/main/data/EES_ytd_data.csv")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data")
OUTPUT_NAME = os.getenv("OUTPUT_NAME", "dfe_attendance_latest.csv")

def main():
    r = requests.get(SOURCE_URL, timeout=60)
    r.raise_for_status()

    with open("temp_download.csv", "wb") as f:
        f.write(r.content)

# Read into pandas
df = pd.read_csv("temp_download.csv")

# Normalize headers (strip stray spaces)
df.columns = [c.strip() for c in df.columns]

# Keep only requested columns (if provided)
keep_cols_env = os.getenv("KEEP_COLS", "")
keep_cols = [c.strip() for c in keep_cols_env.split(",") if c.strip()]

if keep_cols:
    missing = [c for c in keep_cols if c not in df.columns]
    if missing:
        print(f"[WARN] These KEEP_COLS were not found in source columns: {missing}")
        print(f"[INFO] Available columns include: {df.columns.tolist()[:20]}{' ...' if df.shape[1]>20 else ''}")
    # Keep only those that exist, in the requested order
    keep_existing = [c for c in keep_cols if c in df.columns]
    if keep_existing:
        df = df[keep_existing]
    else:
        print("[WARN] None of the requested KEEP_COLS matched; leaving all columns unchanged.")


    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Save main (human-friendly) latest CSV
    latest_path = os.path.join(OUTPUT_DIR, OUTPUT_NAME)
    df.to_csv(latest_path, index=False)

    # Save a compressed dated snapshot to save space
    dated_path = os.path.join(OUTPUT_DIR, f"dfe_attendance_{datetime.utcnow():%Y-%m-%d}.csv.gz")
    df.to_csv(dated_path, index=False, compression="gzip")

    print(f"Saved:\n  {latest_path}\n  {dated_path}\nRows: {len(df)}  Columns: {df.shape[1]}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        sys.stderr.write(str(e) + "\n")
        sys.exit(1)
