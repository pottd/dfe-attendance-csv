# dfe-attendance-csv

Copies of DfE pupil attendance statistics, refreshed by GitHub Actions on weekday mornings.

The data comes from the [Explore Education Statistics (EES) API](https://api.education.gov.uk/statistics/docs/),
publication *Pupil attendance in schools*:

| File | Source data set | Contents |
|---|---|---|
| `data/dfe_YTD_attendance.csv` | Reasons for absence and attendance (`63629501-d3ca-c471-9780-ec4cb6fdf172`), time frame "Year to date", latest week; plus Persistent absence in schools (`55629501-e98b-0c75-adba-f95a0cfbb5e9`) | National, regional and LA year-to-date attendance, authorised and unauthorised absence, and persistent absence |
| `data/dfe_WEEKLY_attendance.csv` | Reasons for absence and attendance, time frame "Week" | National, regional and LA weekly figures for the current academic year |

Dated snapshots (`*_YYYYMMDD.csv`, `*_YYYY-MM-DD.csv.gz`) keep the last 28 runs.

Notes:
- Columns match the files previously copied from `dfe-analytical-services/attendance-data-dashboard`
  (`EES_ytd_data.csv`, `EES_weekly_data.csv`), so downstream Power BI reports don't need changing.
- `pa_perc` is only filled when DfE has published persistent absence for the same academic year as the attendance figures; otherwise it is blank.
- Percentages come from the API rounded to 2 decimal places.
- The weekly file only holds the academic year currently in the API. Earlier years remain in git history.
- To pin a data set version, set `EES_VERSION` (e.g. `4.*`) in the workflow env. The default `*` takes the latest.

Run locally: `pip install -r requirements.txt` then `python scripts/fetch_ees.py ytd` or `python scripts/fetch_ees.py weekly`.
