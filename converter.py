import pandas as pd

# Load CSV
df = pd.read_csv("Files/v_invoices_ledger.csv")

# Save to Parquet (requires pyarrow or fastparquet installed)
df.to_parquet("Parquet/v_invoices_ledger.parquet", engine="pyarrow", index=False)
