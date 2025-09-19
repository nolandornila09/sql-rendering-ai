import os
import json
import io
import duckdb
import pyarrow.parquet as pq
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Load environment variables
load_dotenv()

# FastAPI app
app = FastAPI(title="SQL Renderer API")

class QueryRequest(BaseModel):
    template_id: str
    params: dict
    template_dir: str           # where .sql.tmpl and .meta.json live
    parquet_dir: str            # base parquet directory (ADLS/local/http)

# -------------------------------
# Helpers
# -------------------------------
def download_parquet_blob(account_url, container_name, blob_name, credential):
    """Download parquet blob into in-memory stream."""
    try:
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        stream = io.BytesIO()
        blob_client.download_blob().readinto(stream)
        stream.seek(0)
        return stream
    except Exception as e:
        raise RuntimeError(f"Azure Blob download failed: {str(e)}")

def read_file_with_fallback(path: str, account_name: str, account_key: str) -> str:
    """Read text file (SQL or meta) from ADLS or Blob SDK fallback."""
    con = duckdb.connect()
    # try:
    #     con.execute("INSTALL azure;")
    #     con.execute("LOAD azure;")
    #     connection_string = (
    #         f"DefaultEndpointsProtocol=https;"
    #         f"AccountName={account_name};"
    #         f"AccountKey={account_key};"
    #         f"EndpointSuffix=core.windows.net"
    #     )
    #     con.execute(f"SET azure_storage_connection_string='{connection_string}';")
    #     rows = con.execute(f"SELECT * FROM read_text('{path}')").fetchall()
    #     return "\n".join(r[0] for r in rows)
    # except Exception as direct_err:
    #     print(f"⚠️ Direct ADLS read failed for {path}, fallback: {direct_err}")
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )
        container = path.split("://")[1].split("@")[0]
        blob_path = path.split(".net/")[1]
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob_path)
        return blob_client.download_blob().content_as_text()
    except Exception as sdk_err:
        raise RuntimeError(f"Both direct and fallback failed for {path}\nDirect: {direct_err}\nSDK: {sdk_err}")

def read_parquet_with_fallback(path: str, account_name: str, account_key: str):
    """Read parquet from ADLS using in-memory DuckDB fallback."""
    con = duckdb.connect()
    # try:
    #     con.execute("INSTALL azure;")
    #     con.execute("LOAD azure;")
    #     connection_string = (
    #         f"DefaultEndpointsProtocol=https;"
    #         f"AccountName={account_name};"
    #         f"AccountKey={account_key};"
    #         f"EndpointSuffix=core.windows.net"
    #     )
    #     con.execute(f"SET azure_storage_connection_string='{connection_string}';")
    #     return con.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
    # except Exception as direct_err:
    #     print(f"⚠️ Direct ADLS parquet read failed for {path}, fallback: {direct_err}")
    try:
        # Download blob in-memory
        account_url = f"https://{account_name}.blob.core.windows.net"
        container = path.split("://")[1].split("@")[0]
        blob_path = path.split(".net/")[1]
        stream = download_parquet_blob(account_url, container, blob_path, account_key)

        # Use PyArrow + DuckDB in-memory
        table = pq.read_table(stream)
        df = duckdb.from_arrow(table).df()
        return df
    except Exception as sdk_err:
        raise RuntimeError(f"Both direct and fallback failed for {path}\nDirect: {direct_err}\nSDK: {sdk_err}")

def render_sql(template_id: str, template_dir: str):
    """Load SQL template and metadata."""
    sql_file = f"{template_id}.sql.tmpl"
    meta_file = f"{template_id}.meta.json"

    if template_dir.startswith(("abfs://", "abfss://")):
        account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        if not (account_name and account_key):
            raise RuntimeError("Missing AZURE_STORAGE_ACCOUNT_NAME / AZURE_STORAGE_ACCOUNT_KEY")
        sql_path = f"{template_dir}/{sql_file}"
        meta_path = f"{template_dir}/{meta_file}"
        sql = read_file_with_fallback(sql_path, account_name, account_key)
        meta_text = read_file_with_fallback(meta_path, account_name, account_key)
        meta = json.loads(meta_text)
        return {"sql_template": sql, "meta": meta}
    else:
        sql_path = os.path.join(template_dir, sql_file)
        meta_path = os.path.join(template_dir, meta_file)
        if not os.path.exists(sql_path) or not os.path.exists(meta_path):
            raise FileNotFoundError(f"Template {template_id} not found in {template_dir}")
        with open(sql_path, "r") as f:
            sql = f.read()
        with open(meta_path, "r") as f:
            meta = json.load(f)
        return {"sql_template": sql, "meta": meta}

def validate_params(meta: dict, params: dict):
    required = meta.get("required_filters", [])
    for r in required:
        if r not in params:
            raise ValueError(f"Missing required param: {r}")
    return {**meta.get("defaults", {}), **params}

def substitute_params(sql_template: str, params: dict):
    sql = sql_template
    for key, value in params.items():
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            sql = sql.replace(f"@{key}", f"'{escaped}'")
        else:
            sql = sql.replace(f"@{key}", str(value))
    return sql

def execute_sql(sql_template: str, meta: dict, params: dict, parquet_dir: str):
    con = duckdb.connect(database=":memory:")
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

    # Register parquet views
    for view_name, parquet_file in meta.get("parquet_views", {}).items():
        abs_path = os.path.join(parquet_dir, parquet_file)
        print("DEBUG parquet_dir:", parquet_dir)
        print("DEBUG parquet_file:", parquet_file)
        print("DEBUG abs_path:", abs_path)
        if abs_path.startswith(("abfs://", "abfss://")):
            df = read_parquet_with_fallback(abs_path, account_name, account_key)
            con.register(view_name, df)
        else:
            con.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{abs_path}')")

    final_sql = substitute_params(sql_template, params)
    print("Final SQL:", final_sql)
    df = con.execute(final_sql).fetchdf()
    return df.to_dict(orient="records")

# -------------------------------
# FastAPI Endpoint
# -------------------------------
@app.post("/query")
def run_query(request: QueryRequest):
    try:
        rendered = render_sql(request.template_id, request.template_dir)
        final_params = validate_params(rendered["meta"], request.params)
        result = execute_sql(rendered["sql_template"], rendered["meta"], final_params, request.parquet_dir)
        return {"status": "ok", "data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
