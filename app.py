import os, json, duckdb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

TEMPLATE_DIR = "./templates"
app = FastAPI(title="SQL Renderer API")

class QueryRequest(BaseModel):
    template_id: str
    params: dict


def render_sql(template_id: str):
    sql_path = os.path.join(TEMPLATE_DIR, f"{template_id}.sql.tmpl")
    meta_path = os.path.join(TEMPLATE_DIR, f"{template_id}.meta.json")

    if not os.path.exists(sql_path) or not os.path.exists(meta_path):
        raise FileNotFoundError(f"Template {template_id} not found")

    with open(sql_path, "r") as f:
        sql = f.read()
    with open(meta_path, "r") as f:
        meta = json.load(f)

    return {"sql": sql, "meta": meta}


def validate_params(meta: dict, params: dict):
    required = meta.get("required_filters", [])
    for r in required:
        if r not in params:
            raise ValueError(f"Missing required param: {r}")
    return {**meta.get("defaults", {}), **params}


def execute_sql(sql: str, meta: dict, params: dict):
    con = duckdb.connect(database=":memory:")

    # Register parquet files as views
    for view_name, parquet_path in meta.get("parquet_views", {}).items():
        abs_path = os.path.join(os.getcwd(), parquet_path)
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Parquet file {abs_path} not found")
        con.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{abs_path}'")

    # Substitute parameters safely
    final_sql = sql
    for key, value in params.items():
        if isinstance(value, str):
            value = value.replace("'", "''")
            final_sql = final_sql.replace(f"@{key}", f"'{value}'")
            print('here')
        else:
            final_sql = final_sql.replace(f"@{key}", str(value))
    print(final_sql)  # Debugging line to see the final SQL
    # Execute and return results
    df = con.execute(final_sql).fetchdf()
    return df.to_dict(orient="records")


@app.post("/query")
def run_query(request: QueryRequest):
    try:
        rendered = render_sql(request.template_id)
        final_params = validate_params(rendered["meta"], request.params)
        result = execute_sql(rendered["sql"], rendered["meta"], final_params)
        return {"status": "ok", "data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
