import os
import io
import sys
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

# --- Inputs from workflow env ---
SOURCE_URL  = os.getenv("DFE_SOURCE_URL")
OUTPUT_DIR  = os.getenv("OUTPUT_DIR", "data")
OUTPUT_NAME = os.getenv("OUTPUT_NAME", "dfe_YTD_attendance.csv")
KEEP_COLS   = os.getenv("KEEP_COLS", "")  # comma-separated

def main():
    if not SOURCE_URL:
        raise ValueError("DFE_SOURCE_URL is required")

    # --- Download ---
    r = requests.get(SOURCE_URL, timeout=60)
    r.raise_for_status()

    # --- Read into pandas ---
    df = pd.read_csv(io.StringIO(r.text))

    # --- Parse KEEP_COLS (order not enforced) ---
    keep_cols = [c.strip() for c in KEEP_COLS.split(",") if c.strip()] if KEEP_COLS else []

    if keep_cols:
        # Columns that must exist EXCEPT 'pa_perc' (which we allow to be missing)
        required = [c for c in keep_cols if c != "pa_perc"]
        missing_required = [c for c in required if c not in df.columns]
        if missing_required:
            raise ValueError(
                "Missing required columns from source: "
                + ", ".join(missing_required)
            )

        # Keep only the requested columns that are present now
        present_subset = [c for c in keep_cols if c in df.columns]
        df = df[present_subset]

    # --- Ensure pa_perc exists; add as blank if missing ---
    if "pa_perc" not in df.columns:
        df["pa_perc"] = pd.NA

    # --- Rename education_phase → school_type (if present) ---
    if "education_phase" in df.columns:
        df = df.rename(columns={"education_phase": "school_type"})

    # --- Write outputs ---
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    out_path = Path(OUTPUT_DIR) / OUTPUT_NAME
    df.to_csv(out_path, index=False)

    # Also write a dated snapshot for history (YYYYMMDD)
    stamp = datetime.utcnow().strftime("%Y%m%d")
    snap_name = OUTPUT_NAME.replace(".csv", f"_{stamp}.csv")
    df.to_csv(Path(OUTPUT_DIR) / snap_name, index=False)

    print(f"Wrote {out_path} and snapshot {snap_name}")

if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        # Fail loudly so the workflow doesn't commit bad data
        print(f"ERROR: {ex}", file=sys.stderr)
        sys.exit(1)
