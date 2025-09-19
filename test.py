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
WITH ar AS (
    SELECT
        customer_id,
        CAST(as_of_date AS DATE) AS as_of_date,
        overdue_amount,
        yyyy_mm
    FROM v_ar_aging_daily
    WHERE tenant_id = 1111
      AND CAST(as_of_date AS DATE) BETWEEN CAST('2025-08-25' AS DATE) - INTERVAL '60 DAYS'
                                      AND CAST('2025-08-25' AS DATE)
      AND yyyy_mm >= (YEAR(CAST('2025-08-25' AS DATE) - INTERVAL '60 DAYS') * 100
                      + MONTH(CAST('2025-08-25' AS DATE) - INTERVAL '60 DAYS'))
)
SELECT
    customer_id,
    SUM(CASE WHEN as_of_date > (CAST('2025-08-25' AS DATE) - INTERVAL '14 DAYS')
             THEN overdue_amount ELSE 0 END) AS overdue_14d,
    SUM(CASE WHEN as_of_date <= (CAST('2025-08-25' AS DATE) - INTERVAL '14 DAYS')
             THEN overdue_amount ELSE 0 END) AS overdue_prev_60d,
    CASE
        WHEN SUM(CASE WHEN as_of_date <= (CAST('2025-08-25' AS DATE) - INTERVAL '14 DAYS')
                      THEN overdue_amount ELSE 0 END) > 0
        THEN 1.0 * SUM(CASE WHEN as_of_date > (CAST('2025-08-25' AS DATE) - INTERVAL '14 DAYS')      
                             THEN overdue_amount ELSE 0 END) /
             SUM(CASE WHEN as_of_date <= (CAST('2025-08-25' AS DATE) - INTERVAL '14 DAYS')
                      THEN overdue_amount ELSE 0 END) - 1.0
        ELSE NULL
    END AS spike_pct
FROM ar
GROUP BY customer_id
HAVING SUM(CASE WHEN as_of_date <= (CAST('2025-08-25' AS DATE) - INTERVAL '14 DAYS')
                THEN overdue_amount ELSE 0 END) > 0
ORDER BY spike_pct DESC;
"""
# AND (1.0 * SUM(CASE WHEN as_of_date > (CAST('2025-08-19' AS DATE) - INTERVAL '14 DAYS')
#                         THEN overdue_amount ELSE 0 END) /
#         SUM(CASE WHEN as_of_date <= (CAST('2025-08-19' AS DATE) - INTERVAL '14 DAYS')
#                 THEN overdue_amount ELSE 0 END) - 1.0) >= 0.5
# Execute query and fetch results as Pandas dataframe
df = con.execute(sql).fetchdf()
print(df)
