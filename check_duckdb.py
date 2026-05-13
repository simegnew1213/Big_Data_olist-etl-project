"""
======================================================================
  Olist ETL Project — DuckDB Deep Inspection
======================================================================
  Connects to analytics.duckdb and prints:
    1. Database overview   (file size, schemas, table list)
    2. Per-table details   (columns, dtypes, nulls, row count)
    3. Numeric sums        (SUM of every numeric column per table)
    4. Grand totals        (total rows across all tables)
    5. Sample rows         (first 3 rows of every table)
======================================================================
"""

import duckdb
import pandas as pd
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────────
ROOT    = Path(__file__).parent
DB_PATH = ROOT / "analytics.duckdb"

W = 70          # line width
DIV   = "=" * W
SDIV  = "-" * W
THDIV = "~" * W

# ── Helpers ────────────────────────────────────────────────────────────────────

def h1(title: str):
    print(f"\n{DIV}")
    print(f"  {title}")
    print(DIV)

def h2(title: str):
    print(f"\n{SDIV}")
    print(f"  {title}")
    print(SDIV)

def h3(title: str):
    print(f"\n  {THDIV}")
    print(f"    {title}")
    print(f"  {THDIV}")

def fmt(n):
    """Pretty-print numbers."""
    if isinstance(n, float):
        return f"{n:>20,.4f}"
    return f"{n:>20,}"

def mb(path: Path) -> str:
    return f"{path.stat().st_size / 1_048_576:.2f} MB"


# ── Main ───────────────────────────────────────────────────────────────────────

def main():

    # ── 0. Existence check ─────────────────────────────────────────────────────
    h1("🦆  DuckDB Inspection  →  " + str(DB_PATH))

    if not DB_PATH.exists():
        print("\n  ❌  analytics.duckdb NOT FOUND at expected path.")
        print(f"      Expected: {DB_PATH}")
        return

    print(f"\n  ✅  File found  |  Size: {mb(DB_PATH)}")

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # ── 1. Database overview ───────────────────────────────────────────────────
    h2("1. DATABASE OVERVIEW")

    schemas = con.execute("""
        SELECT DISTINCT table_schema
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY 1
    """).fetchall()

    tables_info = con.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """).fetchall()

    print(f"\n  Schemas  : {[s[0] for s in schemas]}")
    print(f"  Tables   : {len(tables_info)}")
    print()
    print(f"  {'#':<4} {'Schema':<20} {'Table':<35} {'Type'}")
    print(f"  {'-'*4} {'-'*20} {'-'*35} {'-'*10}")
    for i, (schema, table, ttype) in enumerate(tables_info, 1):
        print(f"  {i:<4} {schema:<20} {table:<35} {ttype}")

    # ── 2. Per-table detail ────────────────────────────────────────────────────
    h2("2. PER-TABLE DETAILS  (columns · nulls · sums · samples)")

    grand_total_rows = 0
    grand_numeric_sums: dict[str, float] = {}   # col_name → running sum

    for schema, table, _ in tables_info:
        full = f'"{schema}"."{table}"'
        h3(f"{schema}.{table}")

        # --- fetch entire table ---
        try:
            df = con.execute(f"SELECT * FROM {full}").df()
        except Exception as exc:
            print(f"\n    ❌  Could not read table: {exc}")
            continue

        nrows, ncols = df.shape
        grand_total_rows += nrows

        # --- column info ---
        print(f"\n    Rows : {nrows:,}   |   Columns : {ncols}")
        print(f"\n    {'Column':<40} {'DType':<15} {'Nulls':>8} {'Null%':>8}")
        print(f"    {'-'*40} {'-'*15} {'-'*8} {'-'*8}")
        for col in df.columns:
            nulls = int(df[col].isna().sum())
            pct   = f"{100*nulls/nrows:.1f}%" if nrows else "N/A"
            print(f"    {col:<40} {str(df[col].dtype):<15} {nulls:>8,} {pct:>8}")

        # --- numeric sums ---
        num_cols = df.select_dtypes(include="number").columns.tolist()
        if num_cols:
            print(f"\n    {'Numeric Column':<40} {'SUM':>22}")
            print(f"    {'-'*40} {'-'*22}")
            for col in num_cols:
                total = df[col].sum()
                print(f"    {col:<40} {fmt(total)}")
                grand_numeric_sums[col] = grand_numeric_sums.get(col, 0.0) + float(total)
        else:
            print("\n    (no numeric columns in this table)")

        # --- sample rows ---
        print(f"\n    Sample rows (up to 3):")
        sample = df.head(3).to_string(index=False, max_colwidth=25)
        for line in sample.split("\n"):
            print(f"      {line}")

    # ── 3. Grand totals ────────────────────────────────────────────────────────
    h2("3. GRAND TOTALS ACROSS ALL TABLES")
    print(f"\n  Total rows (all tables)  :  {grand_total_rows:,}")

    if grand_numeric_sums:
        print(f"\n  {'Numeric Column':<45} {'Grand SUM':>22}")
        print(f"  {'-'*45} {'-'*22}")
        for col, total in sorted(grand_numeric_sums.items()):
            print(f"  {col:<45} {total:>22,.4f}")
    else:
        print("\n  (no numeric columns found across any table)")

    # ── 4. Views ──────────────────────────────────────────────────────────────
    views = [(s, t) for s, t, tp in tables_info if tp == "VIEW"]
    if views:
        h2("4. VIEWS DETECTED")
        for s, v in views:
            print(f"  • {s}.{v}")
    
    # ── 5. DuckDB version ──────────────────────────────────────────────────────
    version = con.execute("SELECT version()").fetchone()[0]
    print(f"\n{SDIV}")
    print(f"  DuckDB version : {version}")
    print(SDIV)

    con.close()

    print(f"\n{DIV}")
    print(f"  ✅  DuckDB inspection complete.")
    print(DIV)


if __name__ == "__main__":
    main()
