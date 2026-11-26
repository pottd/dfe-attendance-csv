"""
Fetch DfE pupil attendance statistics from the Explore Education Statistics (EES) API
and write CSVs in the same shape as the files previously copied from
dfe-analytical-services/attendance-data-dashboard (EES_ytd_data.csv / EES_weekly_data.csv).

Usage:
    python scripts/fetch_ees.py ytd       -> data/dfe_YTD_attendance.csv (+ dated snapshot)
    python scripts/fetch_ees.py weekly    -> data/dfe_WEEKLY_attendance.csv (+ dated .csv.gz snapshot)

Environment (all optional):
    OUTPUT_DIR        default "data"
    OUTPUT_NAME       default depends on mode
    EES_VERSION       data set version, default "*" (latest). Pin e.g. "4.*" for stability.

API docs: https://api.education.gov.uk/statistics/docs/
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

BASE = "https://api.education.gov.uk/statistics/v1"

# Data sets in the "Pupil attendance in schools" publication
REASONS_DATASET = "63629501-d3ca-c471-9780-ec4cb6fdf172"  # Reasons for absence and attendance
PA_DATASET = "55629501-e98b-0c75-adba-f95a0cfbb5e9"       # Persistent absence in schools (YTD only)

VERSION = os.getenv("EES_VERSION", "*")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data")

# attendance_reason label -> output column (matches the old EES_*_data.csv names)
REASON_COLUMNS = {
    "Overall attendance": "attendance_perc",
    "All authorised": "authorised_absence_perc",
    "All unauthorised": "unauthorised_absence_perc",
}

PAGE_SIZE = 10000
TIMEOUT = 120


# --------------------------------------------------------------------------- API helpers

def _check(resp: requests.Response) -> dict:
    if resp.status_code != 200:
        raise RuntimeError(f"EES API {resp.status_code} for {resp.url}: {resp.text[:2000]}")
    return resp.json()


def get_meta(dataset_id: str) -> "Meta":
    r = requests.get(f"{BASE}/data-sets/{dataset_id}/meta",
                     params={"dataSetVersion": VERSION}, timeout=TIMEOUT)
    return Meta(_check(r))


def query_all(dataset_id: str, criteria: dict) -> list:
    """POST query, following pages. page/pageSize go in the body; dataSetVersion in the query string."""
    results, page = [], 1
    while True:
        body = {"criteria": criteria, "page": page, "pageSize": PAGE_SIZE}
        r = requests.post(f"{BASE}/data-sets/{dataset_id}/query",
                          params={"dataSetVersion": VERSION}, json=body, timeout=TIMEOUT)
        data = _check(r)
        results.extend(data.get("results", []))
        total_pages = (data.get("paging") or {}).get("totalPages", 1) or 1
        if page >= total_pages:
            return results
        page += 1


# --------------------------------------------------------------------------- metadata lookups

class Meta:
    def __init__(self, raw: dict):
        self.raw = raw
        self.filter_col = {f["id"]: f["column"] for f in raw["filters"]}
        self.option_label = {(f["id"], o["id"]): o["label"]
                             for f in raw["filters"] for o in f["options"]}
        self.indicator_col = {i["id"]: i["column"] for i in raw["indicators"]}
        self.level_label = {g["code"]: g["label"] for g in raw.get("geographicLevels", [])}
        self.location_label = {(l["level"]["code"], o["id"]): o["label"]
                               for l in raw["locations"] for o in l["options"]}
        self.time_periods = raw["timePeriods"]

    def option_id(self, column: str, label: str) -> str:
        for f in self.raw["filters"]:
            if f["column"] == column:
                for o in f["options"]:
                    if o["label"] == label:
                        return o["id"]
                labels = ", ".join(o["label"] for o in f["options"])
                raise ValueError(f"Option '{label}' not found in {column}. Available: {labels}")
        raise ValueError(f"Filter column '{column}' not found")

    def has_filter(self, column: str) -> bool:
        return any(f["column"] == column for f in self.raw["filters"])


# --------------------------------------------------------------------------- time periods

def week_number(tp: dict) -> int:
    digits = "".join(ch for ch in str(tp["code"]) if ch.isdigit())
    return int(digits)


def academic_year_start(tp: dict) -> int:
    """Calendar weeks 32+ (August onwards) belong to the academic year starting that calendar year."""
    year, week = int(str(tp["period"])[:4]), week_number(tp)
    return year if week >= 32 else year - 1


def period_key(tp: dict) -> int:
    week = week_number(tp)
    return academic_year_start(tp) * 1000 + (week if week >= 32 else week + 53)


def academic_year_label(start: int) -> str:
    return f"{start}{str(start + 1)[-2:]}"  # e.g. 2026 -> "202627"


def latest_period(meta: Meta) -> dict:
    return max(meta.time_periods, key=period_key)


def tp_criteria(tp: dict) -> dict:
    return {"timePeriods": {"eq": {"period": tp["period"], "code": tp["code"]}}}


# --------------------------------------------------------------------------- decoding

def decode(results: list, meta: Meta) -> pd.DataFrame:
    rows = []
    for r in results:
        tp = r["timePeriod"]
        level = r["geographicLevel"]
        locs = r.get("locations", {}) or {}
        row = {
            "_period": tp["period"],
            "_code": tp["code"],
            "geographic_level": meta.level_label.get(level, level),
            "country_name": meta.location_label.get(("NAT", locs.get("NAT"))),
            "region_name": meta.location_label.get(("REG", locs.get("REG"))),
            "la_name": meta.location_label.get(("LA", locs.get("LA"))),
        }
        for fid, oid in (r.get("filters") or {}).items():
            row[meta.filter_col.get(fid, fid)] = meta.option_label.get((fid, oid), oid)
        for iid, val in (r.get("values") or {}).items():
            row[meta.indicator_col.get(iid, iid)] = val
        rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("EES API returned no rows for the query")
    return df


def fill_missing_regions(df: pd.DataFrame) -> pd.DataFrame:
    """LA rows should carry their region from the API. If any don't, fall back to the
    LA -> region mapping in the previously published YTD file in this repo."""
    la_rows = df["geographic_level"].eq("Local authority")
    missing = la_rows & df["region_name"].isna()
    if not missing.any():
        return df
    prev = Path(OUTPUT_DIR) / "dfe_YTD_attendance.csv"
    if prev.exists():
        old = pd.read_csv(prev, usecols=["la_name", "region_name"]).dropna().drop_duplicates("la_name")
        mapping = dict(zip(old["la_name"], old["region_name"]))
        df.loc[missing, "region_name"] = df.loc[missing, "la_name"].map(mapping)
    still = (la_rows & df["region_name"].isna()).sum()
    if still:
        print(f"WARNING: {still} LA rows have no region_name", file=sys.stderr)
    return df


def tidy_phase(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"education_phase": "school_type"})
    df["school_type"] = df["school_type"].replace({"All schools": "Total"})
    return df


def pivot_reasons(df: pd.DataFrame, keys: list) -> pd.DataFrame:
    df = df[df["attendance_reason"].isin(REASON_COLUMNS)].copy()
    df["session_percent"] = pd.to_numeric(df["session_percent"], errors="coerce")
    dupes = df.duplicated(keys + ["attendance_reason"], keep=False)
    if dupes.any():
        raise RuntimeError(f"Unexpected duplicate rows:\n{df[dupes].head(20)}")
    wide = (df.pivot(index=keys, columns="attendance_reason", values="session_percent")
              .rename(columns=REASON_COLUMNS)
              .reset_index())
    wide.columns.name = None
    for col in REASON_COLUMNS.values():
        if col not in wide.columns:
            wide[col] = pd.NA
    return wide


def reason_ids(meta: Meta) -> list:
    return [meta.option_id("attendance_reason", label) for label in REASON_COLUMNS]


# --------------------------------------------------------------------------- outputs

GEO_ORDER = {"National": 0, "Regional": 1, "Local authority": 2}
PHASE_ORDER = {"Primary": 0, "Secondary": 1, "Special": 2, "Total": 3}


def sort_output(df: pd.DataFrame, extra_keys=()) -> pd.DataFrame:
    df = df.assign(_g=df["geographic_level"].map(GEO_ORDER), _p=df["school_type"].map(PHASE_ORDER))
    return (df.sort_values(list(extra_keys) + ["_g", "region_name", "la_name", "_p"], na_position="first")
              .drop(columns=["_g", "_p"]))


def write_atomic(df: pd.DataFrame, path: Path, **kwargs):
    tmp = path.with_name(f".tmp_{path.name}")
    df.to_csv(tmp, index=False, **kwargs)
    os.replace(tmp, path)


def build_ytd() -> pd.DataFrame:
    meta = get_meta(REASONS_DATASET)
    latest = latest_period(meta)
    ay = academic_year_start(latest)
    print(f"Reasons data set: latest period {latest['period']} {latest['code']} (academic year {academic_year_label(ay)})")

    criteria = {"and": [
        tp_criteria(latest),
        {"filters": {"eq": meta.option_id("time_frame", "Year to date")}},
        {"filters": {"in": reason_ids(meta)}},
    ]}
    df = tidy_phase(decode(query_all(REASONS_DATASET, criteria), meta))
    df = fill_missing_regions(df)

    keys = ["geographic_level", "country_name", "region_name", "la_name", "school_type"]
    wide = pivot_reasons(df.fillna({"country_name": "", "region_name": "", "la_name": ""}), keys)
    wide.insert(0, "time_period", academic_year_label(ay))

    # Persistent absence: only joined when DfE has published it for the same academic year
    pa_meta = get_meta(PA_DATASET)
    pa_latest = latest_period(pa_meta)
    if academic_year_start(pa_latest) == ay:
        print(f"PA data set: latest period {pa_latest['period']} {pa_latest['code']}, joining pa_perc")
        pa = tidy_phase(decode(query_all(PA_DATASET, tp_criteria(pa_latest)), pa_meta))
        pa = fill_missing_regions(pa).fillna({"country_name": "", "region_name": "", "la_name": ""})
        pa["pa_perc"] = pd.to_numeric(pa["persistent_absence_percent"], errors="coerce")
        wide = wide.merge(pa[keys + ["pa_perc"]], on=keys, how="left")
    else:
        print(f"PA data set: latest period {pa_latest['period']} {pa_latest['code']} is a different "
              f"academic year, so pa_perc is left blank")
        wide["pa_perc"] = pd.NA

    wide[["country_name", "region_name", "la_name"]] = wide[["country_name", "region_name", "la_name"]].replace("", pd.NA)
    cols = ["time_period", "geographic_level", "country_name", "region_name", "la_name", "school_type",
            "attendance_perc", "authorised_absence_perc", "unauthorised_absence_perc", "pa_perc"]
    return sort_output(wide[cols])


def build_weekly() -> pd.DataFrame:
    meta = get_meta(REASONS_DATASET)
    criteria = {"and": [
        {"filters": {"eq": meta.option_id("time_frame", "Week")}},
        {"filters": {"in": reason_ids(meta)}},
    ]}
    df = tidy_phase(decode(query_all(REASONS_DATASET, criteria), meta))
    df = fill_missing_regions(df)
    df["time_period"] = df["_period"].astype(str)
    df["time_identifier"] = "Week " + df["_code"].str.replace(r"\D", "", regex=True)
    df["_key"] = [period_key({"period": p, "code": c}) for p, c in zip(df["_period"], df["_code"])]

    keys = ["_key", "time_period", "time_identifier", "geographic_level", "country_name",
            "region_name", "la_name", "school_type"]
    wide = pivot_reasons(df.fillna({"country_name": "", "region_name": "", "la_name": ""}), keys)
    wide[["country_name", "region_name", "la_name"]] = wide[["country_name", "region_name", "la_name"]].replace("", pd.NA)
    wide = sort_output(wide, extra_keys=["_key"]).drop(columns=["_key"])
    cols = ["time_period", "time_identifier", "geographic_level", "country_name", "region_name", "la_name",
            "school_type", "attendance_perc", "authorised_absence_perc", "unauthorised_absence_perc"]
    print(f"Weekly: {wide['time_identifier'].nunique()} weeks")
    return wide[cols]


def main():
    mode = (sys.argv[1] if len(sys.argv) > 1 else "").lower()
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc)

    if mode == "ytd":
        df = build_ytd()
        name = os.getenv("OUTPUT_NAME", "dfe_YTD_attendance.csv")
        write_atomic(df, out_dir / name)
        snap = out_dir / name.replace(".csv", f"_{today:%Y%m%d}.csv")
        write_atomic(df, snap)
    elif mode == "weekly":
        df = build_weekly()
        name = os.getenv("OUTPUT_NAME", "dfe_WEEKLY_attendance.csv")
        write_atomic(df, out_dir / name)
        snap = out_dir / f"{Path(name).stem}_{today:%Y-%m-%d}.csv.gz"
        write_atomic(df, snap, compression="gzip")
    else:
        raise SystemExit("Usage: python scripts/fetch_ees.py [ytd|weekly]")

    print(f"Wrote {out_dir / name} and {snap.name}: {len(df)} rows, {df.shape[1]} columns")


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        # Fail loudly so the workflow doesn't commit bad data
        print(f"ERROR: {ex}", file=sys.stderr)
        sys.exit(1)
