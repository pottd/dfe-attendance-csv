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

    df = pd.read_csv("temp_download.csv")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Save main (human-friendly) latest CSV
    latest_path = os.path.join(OUTPUT_DIR, OUTPUT_NAME)
    df.to_csv(latest_path, index=False)

    # Save a compressed dated snapshot to save space
    dated_path = os.path.join(OUTPUT_DIR, f"dfe_attendance_{datetime.utcnow():%Y-%m-%d}.csv.gz")
    df.to_csv(dated_path, index=False, compression="gzip")


    print(f"Saved {out_path} and {dated}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        sys.stderr.write(str(e) + "\n")
        sys.exit(1)
