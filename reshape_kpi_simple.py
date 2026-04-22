# ============================================================
# STEP 1: Check raw df - row 0 has months, row 1 has sub-headers
# ============================================================
print(your_df.head(3))
print(your_df.shape)

# ============================================================
# STEP 2: Extract month row (row 0) and header row (row 1)
# ============================================================
month_row  = your_df.iloc[0].tolist()   # ['NaN', 'NaN', 'April 2026', 'NaN', ...]
header_row = your_df.iloc[1].tolist()   # ['KPI', 'Model', 'Dealer walk-in', ...]

print("Month row  :", month_row)
print("Header row :", header_row)

# ============================================================
# STEP 3: Build combined column names by forward-filling months
# ============================================================
combined_cols = []
current_month = ''

for month, header in zip(month_row, header_row):
    # Forward fill month name when cell is NaN or empty
    if str(month) not in ['nan', 'NaN', '', 'None']:
        current_month = str(month).strip()

    header_clean = str(header).strip()

    # First two cols are KPI and Model - no month prefix needed
    if header_clean in ['KPI', 'Model']:
        combined_cols.append(header_clean)
    else:
        combined_cols.append(f"{current_month}_{header_clean}")

print("Combined cols:", combined_cols)

# ============================================================
# STEP 4: Drop rows 0 and 1 (month row and header row)
#         and assign new combined column names
# ============================================================
your_df = your_df.iloc[2:].reset_index(drop=True)
your_df.columns = combined_cols

print(your_df.head(3))

# ============================================================
# STEP 5: Forward fill KPI column (Enquiries, Waitlist, etc.)
#         because merged cells show value only in first row
# ============================================================
your_df['KPI'] = your_df['KPI'].replace('nan', None)
your_df['KPI'] = your_df['KPI'].replace('NaN', None)
your_df['KPI'] = your_df['KPI'].ffill()

# ============================================================
# STEP 6: Rename sub-columns to clean short names
# ============================================================
your_df = your_df.rename(columns={
    'April 2026_Dealer walk-in'      : 'April 2026_Dealer walk-in',
    'April 2026_Digital-attributed'  : 'April 2026_Digital',
    'April 2026_Event / Activation'  : 'April 2026_Event',
    'April 2026_Dealer-led BTL'      : 'April 2026_Dealer BTL',
    'April 2026_Total'               : 'April 2026_Total',

    'May 2026_Dealer walk-in'        : 'May 2026_Dealer walk-in',
    'May 2026_Digital-attributed'    : 'May 2026_Digital',
    'May 2026_Event / Activation'    : 'May 2026_Event',
    'May 2026_Dealer-led BTL'        : 'May 2026_Dealer BTL',
    'May 2026_Total'                 : 'May 2026_Total',

    'June 2026_Dealer walk-in'       : 'June 2026_Dealer walk-in',
    'June 2026_Digital-attributed'   : 'June 2026_Digital',
    'June 2026_Event / Activation'   : 'June 2026_Event',
    'June 2026_Dealer-led BTL'       : 'June 2026_Dealer BTL',
    'June 2026_Total'                : 'June 2026_Total',
})

# ============================================================
# STEP 7: Select final columns in required order
# ============================================================
result = your_df[[
    'KPI',
    'Model',
    'April 2026_Dealer walk-in',
    'April 2026_Digital',
    'April 2026_Event',
    'April 2026_Dealer BTL',
    'April 2026_Total',
    'May 2026_Dealer walk-in',
    'May 2026_Digital',
    'May 2026_Event',
    'May 2026_Dealer BTL',
    'May 2026_Total',
    'June 2026_Dealer walk-in',
    'June 2026_Digital',
    'June 2026_Event',
    'June 2026_Dealer BTL',
    'June 2026_Total',
]]

# ============================================================
# STEP 8: Clean up NaN values
# ============================================================
result = result.fillna('N/A')
result = result.replace('nan', 'N/A')
result = result.replace('NaN', 'N/A')

# ============================================================
# STEP 9: Display in Databricks
# ============================================================
display(result)

# Optional: save as Spark table
# spark.createDataFrame(result).write.mode("overwrite").saveAsTable("your_schema.kpi_output")
