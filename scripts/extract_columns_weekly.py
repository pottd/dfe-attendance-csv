import os, sys, json
import pandas as pd

SOURCE_URL = os.environ.get("SOURCE_URL")
REQUIRED_COLUMNS_CSV = os.environ.get("REQUIRED_COLUMNS_CSV", "")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "data/dfe_weekly_attendance_latest.csv")
RENAME_MAP_JSON = os.environ.get("RENAME_MAP_JSON", "")
CSV_DELIMITER = os.environ.get("CSV_DELIMITER", ",")
CSV_ENCODING = os.environ.get("CSV_ENCODING", "utf-8")

def fail(msg):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

if not SOURCE_URL:
    fail("SOURCE_URL was not provided.")

required_cols = [c.strip() for c in REQUIRED_COLUMNS_CSV.split(",") if c.strip()]
if not required_cols:
    fail("REQUIRED_COLUMNS_CSV is empty — provide a comma-separated list of columns to keep.")

# Parse rename map if provided
rename_map = {}
if RENAME_MAP_JSON.strip():
    try:
        rename_map = json.loads(RENAME_MAP_JSON)
        if not isinstance(rename_map, dict):
            fail("RENAME_MAP_JSON must be a JSON object mapping old->new names.")
    except Exception as e:
        fail(f"Invalid RENAME_MAP_JSON: {e}")

print(f"Downloading CSV from: {SOURCE_URL}")
try:
    df = pd.read_csv(SOURCE_URL, encoding=CSV_ENCODING, sep=CSV_DELIMITER)
except Exception as e:
    fail(f"Failed to read CSV from URL: {e}")

# Apply known renames (optional)
if rename_map:
    df = df.rename(columns=rename_map)

# Strict check: all required columns must exist exactly
missing = [c for c in required_cols if c not in df.columns]
if missing:
    print("Available columns:", list(df.columns))
    fail(f"Missing required columns (strict mode): {missing}")

# Keep only required columns, preserving the specified order
df_out = df[required_cols]

# Ensure output directory exists
out_dir = os.path.dirname(OUTPUT_PATH)
if out_dir:
    os.makedirs(out_dir, exist_ok=True)

# Write filtered CSV
df_out.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
print(f"Wrote filtered CSV -> {OUTPUT_PATH}")
