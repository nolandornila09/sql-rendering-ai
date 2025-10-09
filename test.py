import duckdb
import os

# Path to your parquet file
parquet_file = os.path.join(os.getcwd(), "data/v_ar_aging_daily.parquet")

# Connect to DuckDB in-memory
con = duckdb.connect(database=":memory:")

# Create a view over the parquet file
con.execute(f"""
CREATE VIEW v_ar_aging_daily AS
SELECT *
FROM '{parquet_file}'
""")

# Your SQL query (replace tenant_id, dates, intervals as needed)
sql = """
select DATE_TRUNC('week', CAST(NOW() AS DATE));
"""
# AND (1.0 * SUM(CASE WHEN as_of_date > (CAST('2025-08-19' AS DATE) - INTERVAL '14 DAYS')
#                         THEN overdue_amount ELSE 0 END) /
#         SUM(CASE WHEN as_of_date <= (CAST('2025-08-19' AS DATE) - INTERVAL '14 DAYS')
#                 THEN overdue_amount ELSE 0 END) - 1.0) >= 0.5
# Execute query and fetch results as Pandas dataframe
df = con.execute(sql).fetchdf()
print(df)
