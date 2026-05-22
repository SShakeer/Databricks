# ─────────────────────────────────────────────────────────────────────────────
# dq_framework.py  –  Databricks Data Quality Framework
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

spark = SparkSession.builder.getOrCreate()


# ══════════════════════════════════════════════════════════════════════════════
# 1.  SETUP – Create metadata & results tables (run once)
# ══════════════════════════════════════════════════════════════════════════════

def setup_framework(catalog: str = "dq_catalog"):
    """Create the DQ catalog, metadata table, and results table."""

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}")

    # -- Metadata table --------------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.dq_metadata (
            schema      STRING  NOT NULL,
            table_name  STRING  NOT NULL,
            column      STRING  NOT NULL,
            dqrule      STRING,                -- null | duplicate | range:min:max | regex:pattern
            is_active   BOOLEAN NOT NULL DEFAULT true
        )
        USING DELTA
    """)

    # -- Results table ---------------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.dq_results (
            run_id          STRING,
            run_ts          TIMESTAMP,
            layer           STRING,
            schema          STRING,
            table_name      STRING,
            column          STRING,
            dqrule          STRING,
            status          STRING,            -- PASS | FAIL | ERROR
            fail_count      INT,
            total_count     INT,
            error_message   STRING
        )
        USING DELTA
    """)

    print(f"✅  Framework tables ready under '{catalog}'.")


# ══════════════════════════════════════════════════════════════════════════════
# 2.  POPULATE METADATA
# ══════════════════════════════════════════════════════════════════════════════

def load_metadata(rules: list[tuple], catalog: str = "dq_catalog", mode: str = "append"):
    """
    Load DQ rules into the metadata table.

    rules: list of (schema, table_name, column, dqrule) tuples
    mode : 'append' to add rules | 'overwrite' to replace all
    """
    schema = StructType([
        StructField("schema",     StringType(),  False),
        StructField("table_name", StringType(),  False),
        StructField("column",     StringType(),  False),
        StructField("dqrule",     StringType(),  True),
    ])

    df = (
        spark.createDataFrame(rules, schema=schema)
             .withColumn("is_active", lit(True))
    )

    df.write.format("delta").mode(mode).saveAsTable(f"{catalog}.dq_metadata")
    print(f"✅  Loaded {len(rules)} rule(s) into {catalog}.dq_metadata.")


# ══════════════════════════════════════════════════════════════════════════════
# 3.  CHECK IMPLEMENTATIONS
# ══════════════════════════════════════════════════════════════════════════════

def _check_null(df, column: str) -> tuple[int, int, str]:
    total      = df.count()
    fail_count = df.filter(f"`{column}` IS NULL").count()
    status     = "PASS" if fail_count == 0 else "FAIL"
    return fail_count, total, status


def _check_duplicate(df, column: str) -> tuple[int, int, str]:
    total    = df.count()
    distinct = df.select(column).distinct().count()
    fail_count = total - distinct
    status   = "PASS" if fail_count == 0 else "FAIL"
    return fail_count, total, status


def _check_range(df, column: str, rule: str) -> tuple[int, int, str]:
    _, min_val, max_val = rule.split(":")
    total      = df.count()
    fail_count = df.filter(
        f"`{column}` < {min_val} OR `{column}` > {max_val}"
    ).count()
    status = "PASS" if fail_count == 0 else "FAIL"
    return fail_count, total, status


def _check_regex(df, column: str, rule: str) -> tuple[int, int, str]:
    pattern    = rule.split(":", 1)[1]
    total      = df.count()
    fail_count = df.filter(
        f"`{column}` NOT RLIKE '{pattern}'"
    ).count()
    status = "PASS" if fail_count == 0 else "FAIL"
    return fail_count, total, status


CHECK_REGISTRY = {
    "null":      _check_null,
    "duplicate": _check_duplicate,
    "range":     _check_range,
    "regex":     _check_regex,
}


# ══════════════════════════════════════════════════════════════════════════════
# 4.  MAIN RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def run_dq(
    catalog: str = "dq_catalog",
    layer:   str = None,          # filter by layer e.g. "Bronze" | "Silver"
    fail_fast: bool = False,      # raise exception on first FAIL
) -> None:
    """
    Read active rules from dq_metadata, execute each check,
    and write results to dq_results.
    """
    import uuid
    run_id = str(uuid.uuid4())[:8]
    run_ts = datetime.utcnow()

    # -- Load active rules -----------------------------------------------------
    rules_df = spark.table(f"{catalog}.dq_metadata").filter("is_active = true")
    if layer:
        rules_df = rules_df.filter(f"schema = '{layer}'")

    rules = rules_df.collect()
    print(f"\n🚀  DQ Run [{run_id}]  |  {len(rules)} rule(s)  |  layer={layer or 'ALL'}\n")

    results = []

    for row in rules:
        full_table = f"{row.schema}.{row.table_name}"
        rule_key   = row.dqrule.split(":")[0] if row.dqrule else None
        fail_count, total, status, error_msg = 0, 0, "ERROR", None

        try:
            df = spark.table(full_table)

            if rule_key not in CHECK_REGISTRY:
                raise ValueError(f"Unknown rule type: '{rule_key}'")

            check_fn = CHECK_REGISTRY[rule_key]

            # range / regex need the full rule string; null / duplicate don't
            if rule_key in ("range", "regex"):
                fail_count, total, status = check_fn(df, row.column, row.dqrule)
            else:
                fail_count, total, status = check_fn(df, row.column)

        except Exception as e:
            status    = "ERROR"
            error_msg = str(e)

        # -- Print summary -----------------------------------------------------
        icon = {"PASS": "✅", "FAIL": "❌", "ERROR": "⚠️ "}.get(status, "?")
        print(
            f"  {icon}  [{row.schema}.{row.table_name}.{row.column}] "
            f"{row.dqrule:<20} → {status}"
            + (f"  ({fail_count}/{total} failed)" if status == "FAIL" else "")
            + (f"  ERROR: {error_msg}"            if status == "ERROR" else "")
        )

        results.append((
            run_id, run_ts,
            row.schema,          # layer
            row.schema,          # schema
            row.table_name,
            row.column,
            row.dqrule,
            status,
            fail_count,
            total,
            error_msg,
        ))

        if fail_fast and status == "FAIL":
            raise AssertionError(
                f"DQ FAIL on [{full_table}.{row.column}] rule={row.dqrule}"
            )

    # -- Write results to Delta ------------------------------------------------
    results_schema = StructType([
        StructField("run_id",        StringType(),    True),
        StructField("run_ts",        TimestampType(), True),
        StructField("layer",         StringType(),    True),
        StructField("schema",        StringType(),    True),
        StructField("table_name",    StringType(),    True),
        StructField("column",        StringType(),    True),
        StructField("dqrule",        StringType(),    True),
        StructField("status",        StringType(),    True),
        StructField("fail_count",    IntegerType(),   True),
        StructField("total_count",   IntegerType(),   True),
        StructField("error_message", StringType(),    True),
    ])

    (
        spark.createDataFrame(results, schema=results_schema)
             .write.format("delta")
             .mode("append")
             .saveAsTable(f"{catalog}.dq_results")
    )

    # -- Final summary ---------------------------------------------------------
    pass_n  = sum(1 for r in results if r[7] == "PASS")
    fail_n  = sum(1 for r in results if r[7] == "FAIL")
    error_n = sum(1 for r in results if r[7] == "ERROR")
    print(f"\n📊  Summary → PASS: {pass_n}  FAIL: {fail_n}  ERROR: {error_n}")
    print(f"📝  Results saved to {catalog}.dq_results  (run_id={run_id})\n")


# ══════════════════════════════════════════════════════════════════════════════
# 5.  REPORTING HELPER
# ══════════════════════════════════════════════════════════════════════════════

def dq_report(catalog: str = "dq_catalog", last_n_runs: int = 1):
    """Display the latest DQ results in a readable format."""
    spark.sql(f"""
        SELECT run_id, run_ts, layer, table_name, column, dqrule,
               status, fail_count, total_count
        FROM   {catalog}.dq_results
        WHERE  run_id IN (
            SELECT DISTINCT run_id
            FROM   {catalog}.dq_results
            ORDER BY run_ts DESC
            LIMIT  {last_n_runs}
        )
        ORDER BY run_ts DESC, layer, table_name, column
    """).show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════════
# 6.  ENTRYPOINT  (run this notebook cell-by-cell or as a job)
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":

    CATALOG = "dq_catalog"

    # Step 1 – one-time setup
    setup_framework(CATALOG)

    # Step 2 – load your rules
    rules = [
        ("Bronze", "job", "id", "null"),
        ("Silver", "job", "id", "duplicate"),
        # add more as needed:
        # ("Gold", "employee", "salary", "range:0:500000"),
        # ("Silver", "customer", "email", "regex:^[\\w.-]+@[\\w.-]+\\.\\w+$"),
    ]
    load_metadata(rules, catalog=CATALOG, mode="overwrite")

    # Step 3 – run all checks (or filter by layer)
    run_dq(catalog=CATALOG, layer=None, fail_fast=False)

    # Step 4 – view results
    dq_report(catalog=CATALOG, last_n_runs=1)