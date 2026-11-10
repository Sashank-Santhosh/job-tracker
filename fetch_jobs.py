"""Week 1: pull Australian analyst job ads from Adzuna into DuckDB.

Usage:
    python fetch_jobs.py              # 2 pages per search term
    python fetch_jobs.py --pages 5    # more pages per term
    python fetch_jobs.py --report     # only print the data-quality report

Needs ADZUNA_APP_ID and ADZUNA_APP_KEY in a .env file (see .env.example).
"""

import argparse
import json
import os
import time
from datetime import date
from pathlib import Path

import duckdb
import requests
from dotenv import load_dotenv

BASE_URL = "https://api.adzuna.com/v1/api/jobs/au/search/{page}"
SEARCH_TERMS = ["data analyst", "business analyst", "reporting analyst", "insights analyst"]
RESULTS_PER_PAGE = 50
DB_PATH = Path("data/jobs.duckdb")
RAW_DIR = Path("data/raw")

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id                  VARCHAR PRIMARY KEY,
    title               VARCHAR,
    company             VARCHAR,
    location            VARCHAR,
    state               VARCHAR,
    category            VARCHAR,
    description         VARCHAR,
    salary_min          DOUBLE,
    salary_max          DOUBLE,
    salary_is_predicted BOOLEAN,
    contract_type       VARCHAR,
    contract_time       VARCHAR,
    created             TIMESTAMP,
    redirect_url        VARCHAR,
    search_term         VARCHAR,
    first_seen          DATE
);
"""


def fetch_page(term: str, page: int, app_id: str, app_key: str) -> dict:
    """Call the Adzuna search endpoint once and return the parsed JSON."""
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "what": term,
        "results_per_page": RESULTS_PER_PAGE,
        "sort_by": "date",
        "content-type": "application/json",
    }
    response = requests.get(BASE_URL.format(page=page), params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw(payload: dict, term: str, page: int) -> None:
    """Keep the untouched API response so the pipeline can be rebuilt later."""
    folder = RAW_DIR / date.today().isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    name = f"{term.replace(' ', '_')}_p{page}.json"
    (folder / name).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def to_row(job: dict, term: str) -> tuple:
    """Flatten one Adzuna result into a row for the jobs table."""
    area = (job.get("location") or {}).get("area") or []
    return (
        str(job.get("id")),
        job.get("title"),
        (job.get("company") or {}).get("display_name"),
        (job.get("location") or {}).get("display_name"),
        area[1] if len(area) > 1 else None,  # area = [country, state, region, ...]
        (job.get("category") or {}).get("label"),
        job.get("description"),
        job.get("salary_min"),
        job.get("salary_max"),
        str(job.get("salary_is_predicted", "0")) == "1",
        job.get("contract_type"),
        job.get("contract_time"),
        job.get("created"),
        job.get("redirect_url"),
        term,
        date.today(),
    )


def store(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> int:
    """Insert rows, skipping job ids already stored. Returns how many were new."""
    before = con.execute("SELECT count(*) FROM jobs").fetchone()[0]
    con.executemany(
        "INSERT OR IGNORE INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
    )
    return con.execute("SELECT count(*) FROM jobs").fetchone()[0] - before


def report(con: duckdb.DuckDBPyConnection) -> None:
    """Print the week 1 checks: volume, description length, salary coverage, duplicates."""
    total = con.execute("SELECT count(*) FROM jobs").fetchone()[0]
    if total == 0:
        print("No jobs stored yet.")
        return

    avg_len, min_len, max_len = con.execute(
        "SELECT avg(length(description)), min(length(description)), max(length(description)) FROM jobs"
    ).fetchone()
    with_salary, real_salary = con.execute(
        """SELECT count(*) FILTER (WHERE salary_min IS NOT NULL),
                  count(*) FILTER (WHERE salary_min IS NOT NULL AND NOT salary_is_predicted)
           FROM jobs"""
    ).fetchone()
    reposts = con.execute(
        """SELECT coalesce(sum(n - 1), 0) FROM (
               SELECT count(*) AS n FROM jobs
               GROUP BY lower(title), lower(company), lower(location) HAVING count(*) > 1)"""
    ).fetchone()[0]

    print(f"\nJobs stored:            {total}")
    print(f"Description length:     avg {avg_len:.0f} chars (min {min_len}, max {max_len})")
    print(f"With any salary:        {with_salary} ({with_salary / total:.0%})")
    print(f"With advertised salary: {real_salary} ({real_salary / total:.0%})  <- not Adzuna's estimate")
    print(f"Likely reposts:         {reposts} (same title + company + location)")
    print("\nBy state:")
    for state, n in con.execute(
        "SELECT coalesce(state, 'Unknown'), count(*) FROM jobs GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall():
        print(f"  {state:<32}{n}")
    print("\nSample description:\n ", con.execute(
        "SELECT description FROM jobs ORDER BY length(description) DESC LIMIT 1"
    ).fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--pages", type=int, default=2, help="pages per search term (50 ads each)")
    parser.add_argument("--report", action="store_true", help="skip fetching, just print the report")
    args = parser.parse_args()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(SCHEMA)

    if not args.report:
        load_dotenv()
        app_id, app_key = os.getenv("ADZUNA_APP_ID"), os.getenv("ADZUNA_APP_KEY")
        if not app_id or not app_key:
            raise SystemExit("Set ADZUNA_APP_ID and ADZUNA_APP_KEY in a .env file first.")

        for term in SEARCH_TERMS:
            for page in range(1, args.pages + 1):
                payload = fetch_page(term, page, app_id, app_key)
                save_raw(payload, term, page)
                results = payload.get("results", [])
                new = store(con, [to_row(job, term) for job in results])
                print(f"{term!r} page {page}: {len(results)} fetched, {new} new")
                if len(results) < RESULTS_PER_PAGE:
                    break  # no more pages for this term
                time.sleep(2.5)  # stay well under the per-minute limit

    report(con)
    con.close()


if __name__ == "__main__":
    main()
