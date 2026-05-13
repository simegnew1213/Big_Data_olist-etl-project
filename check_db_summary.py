"""
===================================================
  Olist ETL Project — Database & Raw Data Summary
===================================================
  Checks the DuckDB analytics database AND all raw
  CSV files, printing row counts and numeric sums
  for every table / dataset.
===================================================
"""

import os
import sys
import duckdb
import pandas as pd
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).parent
DB_PATH   = ROOT / "analytics.duckdb"
RAW_DIR   = ROOT / "data" / "raw"

DIVIDER   = "=" * 70
SUBDIV    = "-" * 70

# ── Helpers ────────────────────────────────────────────────────────────────────

def fmt_num(n):
    """Format a number with thousands-separator."""
    if isinstance(n, float):
        return f"{n:,.2f}"
    return f"{n:,}"


def summarise_df(df: pd.DataFrame, label: str):
    """Print row count + sum of every numeric column for a DataFrame."""
    print(f"\n  📄  {label}")
    print(f"      Rows : {fmt_num(len(df))}")

    num_cols = df.select_dtypes(include="number").columns.tolist()
    if num_cols:
        print(f"      Numeric columns & sums:")
        for col in num_cols:
            total = df[col].sum()
            print(f"        • {col:<45} {fmt_num(total)}")
    else:
        print("      (no numeric columns)")


# ── 1. DuckDB — all tables ─────────────────────────────────────────────────────

def check_duckdb():
    print(DIVIDER)
    print("  🦆  DuckDB  →  " + str(DB_PATH))
    print(DIVIDER)

    if not DB_PATH.exists():
        print("  ⚠️  analytics.duckdb not found – skipping.")
        return

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # List all schemas and tables
    tables = con.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """).fetchall()

    if not tables:
        print("  (no tables found in the database)")
        con.close()
        return

    print(f"\n  Found {len(tables)} table(s):\n")

    grand_total_rows = 0
    for schema, table in tables:
        full_name = f'"{schema}"."{table}"'
        df = con.execute(f"SELECT * FROM {full_name}").df()
        grand_total_rows += len(df)
        summarise_df(df, f"{schema}.{table}")

    print(f"\n{SUBDIV}")
    print(f"  📊  DUCKDB GRAND TOTAL ROWS : {fmt_num(grand_total_rows)}")
    print(SUBDIV)
    con.close()


# ── 2. Raw CSV files ───────────────────────────────────────────────────────────

def check_raw_csvs():
    print(f"\n{DIVIDER}")
    print("  📂  Raw CSV files  →  " + str(RAW_DIR))
    print(DIVIDER)

    if not RAW_DIR.exists():
        print("  ⚠️  data/raw directory not found – skipping.")
        return

    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        print("  (no CSV files found)")
        return

    print(f"\n  Found {len(csv_files)} CSV file(s):\n")

    grand_total_rows = 0
    all_sums: dict[str, dict] = {}   # file → {col: sum}

    for csv_path in csv_files:
        try:
            df = pd.read_csv(csv_path, low_memory=False)
        except Exception as exc:
            print(f"  ❌  {csv_path.name}  — could not read: {exc}")
            continue

        grand_total_rows += len(df)
        summarise_df(df, csv_path.name)

        # accumulate numeric sums across all files
        num_df = df.select_dtypes(include="number")
        for col in num_df.columns:
            all_sums.setdefault(col, {})
            all_sums[col][csv_path.stem] = num_df[col].sum()

    # ── Grand-total summary ────────────────────────────────────────────────────
    print(f"\n{SUBDIV}")
    print(f"  📊  RAW CSV GRAND TOTAL ROWS : {fmt_num(grand_total_rows)}")
    print(SUBDIV)

    # ── Cross-file numeric totals ──────────────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  🔢  CROSS-FILE NUMERIC COLUMN GRAND TOTALS")
    print(DIVIDER)
    for col, file_sums in sorted(all_sums.items()):
        overall = sum(file_sums.values())
        print(f"  • {col:<50} {fmt_num(overall)}")
        for fname, s in file_sums.items():
            print(f"      ↳  [{fname}]  {fmt_num(s)}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    check_duckdb()
    check_raw_csvs()

    print(f"\n{DIVIDER}")
    print("  ✅  Summary complete.")
    print(DIVIDER)
