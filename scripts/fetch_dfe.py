import os
import sys
import io
import pandas as pd
import requests
from datetime import datetime

# Inputs from workflow env
SOURCE_URL  = os.getenv("DFE_SOURCE_URL")
OUTPUT_DIR  = os.getenv("OUTPUT_DIR", "data")
OUTPUT_NAME = os.getenv("OUTPUT_NAME", "dfe_attendance_latest.csv")

def main():
    if not SOURCE_URL:
        raise ValueError("DFE_SOURCE_URL is required")

    # --- Download ---
    r = requests.get(SOURCE_URL, timeout=60)
    r.raise_for_status()

    # --- Read into pandas ---
    # (We write to a temp file to avoid partial writes on weird failures)
    with open("temp_download.csv", "wb") as f:
        f.write(r.content)

    df = pd.read_csv("temp_download.csv")

    # --- Normalise headers (strip stray spaces) ---
    df.columns = [c.strip() for c in df.columns]

    # --- STRICT: Keep only requested columns; fail if any missing ---
    keep_cols_env = os.getenv("KEEP_COLS", "").strip()
    if keep_cols_env:
        requested = [c.strip() for c in keep_cols_env.split(",") if c.strip()]
        missing = [c for c in requested if c not in df.columns]
        if missing:
            raise ValueError(
                "Strict schema check failed. Missing columns: "
                f"{missing}. First 30 available columns: {df.columns.tolist()[:30]}"
            )
        # Keep exactly and only the requested columns (in order)
        df = df[requested]

    # --- Save outputs (atomic-ish writes) ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    latest_path = os.path.join(OUTPUT_DIR, OUTPUT_NAME)
    dated_path  = os.path.join(OUTPUT_DIR, f"dfe_attendance_{datetime.utcnow():%Y-%m-%d}.csv.gz")

    tmp_latest = os.path.join(OUTPUT_DIR, f".tmp_{OUTPUT_NAME}")
    df.to_csv(tmp_latest, index=False)
    os.replace(tmp_latest, latest_path)

    tmp_dated = os.path.join(OUTPUT_DIR, f".tmp_{os.path.basename(dated_path)}")
    df.to_csv(tmp_dated, index=False, compression="gzip")
    os.replace(tmp_dated, dated_path)

    print(f"Saved:\n  {latest_path}\n  {dated_path}\nRows: {len(df)}  Columns: {df.shape[1]}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        sys.stderr.write(str(e) + "\n")
        sys.exit(1)
