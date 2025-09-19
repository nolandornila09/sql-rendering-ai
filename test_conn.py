import duckdb, os
from dotenv import load_dotenv

load_dotenv()

con = duckdb.connect()
con.execute("INSTALL azure;")
con.execute("LOAD azure;")

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
print(con.execute("SHOW EXTENSIONS;").fetchdf())

if not account_name or not account_key:
    raise RuntimeError("Missing env AZURE_STORAGE_ACCOUNT_NAME / AZURE_STORAGE_ACCOUNT_KEY")

connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={account_name};"
    f"AccountKey={account_key};"
    f"EndpointSuffix=core.windows.net"
)

con.execute(f"SET azure_storage_connection_string='{connection_string}';")

# ✅ URL-encoded path
path = "abfss://container-cci-bc-cronus@phoenixadls.dfs.core.windows.net/Nolan%20Test%20Parquet/SalesInvoiceHeader.parquet"

df = con.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 5").fetchdf()
print(df)
