# ============================================================
# Databricks: Reshape KPI DataFrame to Month-wise Column Format
# ============================================================
# INPUT:  Multi-level columns (KPI, Model, Apr/May/Jun 2026 x 5 sub-cols)
# OUTPUT: Flat columns with month prefix headers
# ============================================================

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ============================================================
# STEP 1: Assume your raw DataFrame is already loaded
# Replace this block with your actual df source
# e.g., df = spark.read.csv(...).toPandas()
#        df = spark.table("your_table").toPandas()
# ============================================================

# Example skeleton matching your screenshot structure
# Your raw df likely has multi-index columns like:
#   Level 0: '', '', 'April 2026', 'April 2026', ..., 'May 2026', ..., 'June 2026', ...
#   Level 1: 'KPI', 'Model', 'Dealer walk-in', 'Digital-attributed', 'Event/Activation', 'Dealer-led BTL', 'Total', ...

# ============================================================
# STEP 2: Define column rename mapping
# Adjust the raw column names to match what's in YOUR df
# ============================================================

MONTHS = ["April 2026", "May 2026", "June 2026"]

SUB_COLS = {
    "Dealer walk-in":       "Dealer walk-in",
    "Digital-attributed":   "Digital",
    "Event / Activation":   "Event",
    "Dealer-led BTL":       "Dealer BTL",
    "Total":                "Total",
}

# ============================================================
# STEP 3: Flatten multi-level columns (if your df has them)
# Skip this if your df already has flat columns
# ============================================================

def flatten_columns(df):
    """
    Converts MultiIndex columns into flat strings like:
    'April 2026_Dealer walk-in', 'April 2026_Digital-attributed', etc.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join([str(c).strip() for c in col if str(c).strip()])
            for col in df.columns
        ]
    return df


# ============================================================
# STEP 4: Core reshape function
# ============================================================

def reshape_kpi_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshapes raw KPI dataframe into the required flat output format.

    Expected raw_df columns (after flattening multi-index):
        KPI, Model,
        April 2026_Dealer walk-in, April 2026_Digital-attributed,
        April 2026_Event / Activation, April 2026_Dealer-led BTL, April 2026_Total,
        May 2026_Dealer walk-in, ... (same pattern)
        June 2026_Dealer walk-in, ... (same pattern)

    Output columns:
        KPI, Model,
        April 2026, April 2026_Dealer walk-in, April 2026_Digital,
        April 2026_Event, April 2026_Dealer BTL, April 2026_Total,
        May 2026, May 2026_Dealer walk-in, ... (same)
        June 2026, June 2026_Dealer walk-in, ... (same)
    """

    raw_df = flatten_columns(raw_df.copy())

    output_cols = ["KPI", "Model"]
    rename_map  = {}

    for month in MONTHS:
        # Add month-level header as a marker column (empty, just for labeling)
        month_marker = month
        output_cols.append(month_marker)

        for raw_sub, clean_sub in SUB_COLS.items():
            raw_col   = f"{month}_{raw_sub}"
            clean_col = f"{month}_{clean_sub}"
            rename_map[raw_col] = clean_col
            output_cols.append(clean_col)

    # Rename matched columns
    raw_df = raw_df.rename(columns=rename_map)

    # Insert empty month-marker columns (these act as visual separators / headers)
    for month in MONTHS:
        if month not in raw_df.columns:
            raw_df[month] = ""

    # Select and return only required columns (ignore missing gracefully)
    final_cols = [c for c in output_cols if c in raw_df.columns]
    result_df  = raw_df[final_cols].copy()

    # Fill "Not Applicable" cells cleanly
    result_df = result_df.fillna("N/A")

    return result_df


# ============================================================
# STEP 5: Run reshape on your actual pandas df
# ============================================================

# If coming from Spark:
# pandas_df = your_spark_df.toPandas()

# Example usage (replace `pandas_df` with your actual variable):
# result = reshape_kpi_df(pandas_df)

# ============================================================
# STEP 6: Display in Databricks
# ============================================================

# display(spark.createDataFrame(result))   # Databricks native display
# result.to_csv("/dbfs/tmp/kpi_output.csv", index=False)  # Save to DBFS


# ============================================================
# ALTERNATIVE: If your df already has FLAT columns
# (no MultiIndex), use this direct approach instead
# ============================================================

def reshape_flat_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Use this if your DataFrame already has flat columns named like:
    'KPI', 'Model', 'Apr_Dealer_walkin', 'Apr_Digital', etc.

    Just update the column_map below to match YOUR exact column names.
    """

    column_map = {
        # Your actual col name  →  Output col name
        "KPI":                          "KPI",
        "Model":                        "Model",

        # April 2026
        "Apr_Dealer_walkin":            "April 2026_Dealer walk-in",
        "Apr_Digital":                  "April 2026_Digital",
        "Apr_Event":                    "April 2026_Event",
        "Apr_DealerBTL":                "April 2026_Dealer BTL",
        "Apr_Total":                    "April 2026_Total",

        # May 2026
        "May_Dealer_walkin":            "May 2026_Dealer walk-in",
        "May_Digital":                  "May 2026_Digital",
        "May_Event":                    "May 2026_Event",
        "May_DealerBTL":                "May 2026_Dealer BTL",
        "May_Total":                    "May 2026_Total",

        # June 2026
        "Jun_Dealer_walkin":            "June 2026_Dealer walk-in",
        "Jun_Digital":                  "June 2026_Digital",
        "Jun_Event":                    "June 2026_Event",
        "Jun_DealerBTL":                "June 2026_Dealer BTL",
        "Jun_Total":                    "June 2026_Total",
    }

    df = df.rename(columns=column_map)

    # Insert visual month-separator columns
    for month in MONTHS:
        insert_pos = [i for i, c in enumerate(df.columns) if c.startswith(month)]
        if insert_pos:
            df.insert(insert_pos[0], month, "")

    return df.fillna("N/A")


# ============================================================
# STEP 7: Build output column order explicitly (most reliable)
# Use this if STEP 4 reshape doesn't match your column names
# ============================================================

def build_output_df(df: pd.DataFrame, col_mapping: dict) -> pd.DataFrame:
    """
    Most explicit approach — define exactly which source column
    maps to which output column.

    col_mapping = {
        "output_col_name": "source_col_name_in_df",
        ...
    }
    """
    out = pd.DataFrame()
    for out_col, src_col in col_mapping.items():
        if src_col in df.columns:
            out[out_col] = df[src_col]
        else:
            out[out_col] = "N/A"
            print(f"[WARN] Source column not found: '{src_col}' → filled with N/A")
    return out


# Example col_mapping for build_output_df:
explicit_mapping = {
    "KPI":                      "KPI",
    "Model":                    "Model",
    "April 2026":               None,                        # marker — will be empty
    "Dealer walk-in (Apr)":     "April 2026_Dealer walk-in",
    "Digital (Apr)":            "April 2026_Digital-attributed",
    "Event (Apr)":              "April 2026_Event / Activation",
    "Dealer BTL (Apr)":         "April 2026_Dealer-led BTL",
    "Total (Apr)":              "April 2026_Total",
    "May 2026":                 None,
    "Dealer walk-in (May)":     "May 2026_Dealer walk-in",
    "Digital (May)":            "May 2026_Digital-attributed",
    "Event (May)":              "May 2026_Event / Activation",
    "Dealer BTL (May)":         "May 2026_Dealer-led BTL",
    "Total (May)":              "May 2026_Total",
    "June 2026":                None,
    "Dealer walk-in (Jun)":     "June 2026_Dealer walk-in",
    "Digital (Jun)":            "June 2026_Digital-attributed",
    "Event (Jun)":              "June 2026_Event / Activation",
    "Dealer BTL (Jun)":         "June 2026_Dealer-led BTL",
    "Total (Jun)":              "June 2026_Total",
}


# ============================================================
# QUICK DEBUG: Print your actual column names first
# Run this in a Databricks cell to see what you're working with
# ============================================================

def debug_columns(df):
    print("=== Column names in your DataFrame ===")
    for i, col in enumerate(df.columns):
        print(f"  [{i}] {repr(col)}")
    print(f"\nTotal columns: {len(df.columns)}")
    print(f"Shape: {df.shape}")
    if isinstance(df.columns, pd.MultiIndex):
        print("\n*** MultiIndex detected — run flatten_columns(df) first ***")

# Usage:
# debug_columns(your_df)
