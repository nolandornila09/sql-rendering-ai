import os
import json
import io
import re
import time
import duckdb
import pyarrow.parquet as pq
import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import logging
from opencensus.ext.azure.log_exporter import AzureLogHandler

# -------------------------------
# Setup
# -------------------------------
load_dotenv()

app = FastAPI(title="SQL Renderer API")

logger = logging.getLogger()  # root logger
logger.setLevel(logging.INFO)

connection_string = os.getenv("APPINSIGHTS_CONNECTION_STRING")
if connection_string:
    logger.addHandler(AzureLogHandler(connection_string=connection_string))

class QueryRequest(BaseModel):
    template_id: str
    params: dict
    template_dir: str           # where .sql.tmpl and .meta.json live
    parquet_dir: str            # base parquet directory (ADLS/local/http)

# -------------------------------
# Middleware: Log API requests
# -------------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration_ms = int((time.time() - start_time) * 1000)

    logger.info(
        "HTTP request processed",
        extra={
            "custom_dimensions": {
                "method": request.method,
                "url": str(request.url),
                "duration_ms": duration_ms,
                "status_code": response.status_code,
            }
        },
    )
    return response

# -------------------------------
# Helpers
# -------------------------------
def download_parquet_blob(account_url, container_name, blob_name, credential):
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
        raise RuntimeError(f"Failed to read file from {path}: {sdk_err}")


def read_parquet_with_fallback(path: str, account_name: str, account_key: str):
    try:
        account_url = f"https://{account_name}.blob.core.windows.net"
        container = path.split("://")[1].split("@")[0]
        blob_path = path.split(".net/")[1]
        stream = download_parquet_blob(account_url, container, blob_path, account_key)

        table = pq.read_table(stream)
        df = duckdb.from_arrow(table).df()
        return df
    except Exception as sdk_err:
        raise RuntimeError(f"Failed to read parquet from {path}: {sdk_err}")


def render_sql(template_id: str, template_dir: str):
    sql_file = f"{template_id}.sql.tmpl"
    meta_file = f"{template_id}.meta.json"
    policy_file = "policy.json"

    if template_dir.startswith(("abfs://", "abfss://")):
        account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        if not (account_name and account_key):
            raise RuntimeError("Missing AZURE_STORAGE_ACCOUNT_NAME / AZURE_STORAGE_ACCOUNT_KEY")

        sql_path = f"{template_dir}/{sql_file}"
        meta_path = f"{template_dir}/{meta_file}"
        policy_path = f"{template_dir}/{policy_file}"

        sql = read_file_with_fallback(sql_path, account_name, account_key)
        meta_text = read_file_with_fallback(meta_path, account_name, account_key)
        policy_text = read_file_with_fallback(policy_path, account_name, account_key)

        meta = json.loads(meta_text)
        policy = json.loads(policy_text)

        return {"sql_template": sql, "meta": meta, "policy": policy}
    else:
        sql_path = os.path.join(template_dir, sql_file)
        meta_path = os.path.join(template_dir, meta_file)
        policy_path = os.path.join(template_dir, policy_file)

        if not os.path.exists(sql_path) or not os.path.exists(meta_path) or not os.path.exists(policy_path):
            raise FileNotFoundError(f"Template {template_id} or policy.json not found in {template_dir}")

        with open(sql_path, "r") as f:
            sql = f.read()
        with open(meta_path, "r") as f:
            meta = json.load(f)
        with open(policy_path, "r") as f:
            policy = json.load(f)

        return {"sql_template": sql, "meta": meta, "policy": policy}

# -------------------------------
# Policy + Param Validation
# -------------------------------
def validate_policy(params: dict, sql: str, policy: dict):
    rules = policy.get("rules", {})

    if "window_days" in params and rules.get("max_window_days"):
        if params["window_days"] > rules["max_window_days"]:
            raise ValueError(f"window_days exceeds policy max {rules['max_window_days']}")

    if rules.get("disallow_future_as_of") and "as_of_date" in params:
        today = datetime.utcnow().date()
        as_of_date = normalize_date(params["as_of_date"])
        if as_of_date > today:
            raise ValueError("as_of_date cannot be in the future")

    allowed_ops = [op.upper() for op in rules.get("allowed_actions", ["SELECT"])]
    disallowed_ops = [a.upper() for a in rules.get("disallowed_patterns", [])]
    sql_upper = sql.upper()
    tokens = re.findall(r"[A-Z_]+", sql_upper)
    first_token = tokens[0] if tokens else None
    if first_token not in allowed_ops:
        raise ValueError(f"SQL operation '{first_token}' not allowed. Allowed: {allowed_ops}")
    for token in tokens:
        if token in disallowed_ops:
            raise ValueError(f"Disallowed SQL operation detected: {token}")

    return True


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


def normalize_date(d: str) -> str:
    return datetime.strptime(d, "%Y-%m-%d").date()

# -------------------------------
# Execute SQL with Telemetry
# -------------------------------
def execute_sql(sql_template: str, meta: dict, params: dict, parquet_dir: str, policy: dict):
    con = duckdb.connect(database=":memory:")
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

    for view_name, parquet_file in meta.get("parquet_views", {}).items():
        abs_path = os.path.join(parquet_dir, parquet_file)
        if abs_path.startswith(("abfs://", "abfss://")):
            df = read_parquet_with_fallback(abs_path, account_name, account_key)
            con.register(view_name, df)
        else:
            con.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{abs_path}')")

    final_sql = substitute_params(sql_template, params)
    validate_policy(params, final_sql, policy)

    start_time = time.time()
    df = con.execute(final_sql).fetchdf()
    duration_ms = int((time.time() - start_time) * 1000)

    row_limit = policy.get("rules", {}).get("row_limit")
    if row_limit and len(df) > row_limit:
        df = df.head(row_limit)

    row_count = len(df)
    bytes_consumed = df.memory_usage(deep=True).sum()

    logger.info(
        "SQL executed",
        extra={
            "custom_dimensions": {
                "sql": final_sql[:1000],
                "params": params,
                "execution_time_ms": duration_ms,
                "row_count": row_count,
                "bytes_consumed": int(bytes_consumed),
            }
        },
    )

    return df.to_dict(orient="records")

# -------------------------------
# API Endpoint
# -------------------------------
@app.post("/query")
def run_query(request: QueryRequest):
    try:
        rendered = render_sql(request.template_id, request.template_dir)
        final_params = validate_params(rendered["meta"], request.params)
        result = execute_sql(
            rendered["sql_template"], rendered["meta"], final_params,
            request.parquet_dir, rendered["policy"]
        )
        return {"status": "ok", "data": result}
    except Exception as e:
        logger.error("Query failed", exc_info=True, extra={"custom_dimensions": {"error": str(e)}})
        raise HTTPException(status_code=400, detail=str(e))
