import json
from pathlib import Path
from typing import Union, List, Dict, Any
from datetime import datetime
from qwen_agent.tools.base import BaseTool, register_tool
from sandbox_fusion import run_code, RunCodeRequest
import os

# We assume the data is mounted at /data in the sandbox
SANDBOX_DATA_DIR = "/data"
SANDBOX_ENDPOINT = os.environ.get("SANDBOX_FUSION_ENDPOINT", "http://localhost:8080")

def _execute_in_sandbox(code: str) -> str:
    """Helper to execute python code in the sandbox and return stdout/stderr"""
    req = RunCodeRequest(code=code, run_timeout=60)
    result = run_code(req, endpoint=SANDBOX_ENDPOINT)
    
    if result.run_result.stderr:
        return f"Error: {result.run_result.stderr}\nOutput: {result.run_result.stdout}"
    return result.run_result.stdout

@register_tool("list_parquet_files")
class ListParquetFiles(BaseTool):
    name = "list_parquet_files"
    description = "List all parquet files in the data directory with metadata."
    parameters = {
        "type": "object",
        "properties": {
            "directory": {
                "type": "string",
                "description": "Directory path to search for parquet files (defaults to current data dir)"
            }
        },
        "required": []
    }

    def call(self, params: Union[str, dict], **kwargs) -> str:
        # Generate Python code to run in sandbox
        code = f"""
import glob
import duckdb
import json
import os

data_dir = "{SANDBOX_DATA_DIR}"
files_info = []

try:
    # Find all parquet files
    parquet_files = glob.glob(os.path.join(data_dir, "*.parquet"))
    
    for file_path in parquet_files:
        try:
            # Use duckdb to get metadata
            conn = duckdb.connect(":memory:")
            row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{{file_path}}')").fetchone()[0]
            
            # Get column count (schema)
            # We limit 0 to just get schema
            result = conn.execute(f"SELECT * FROM read_parquet('{{file_path}}') LIMIT 0")
            column_count = len(result.description)
            conn.close()

            files_info.append({{
                "filename": os.path.basename(file_path),
                "path": file_path,
                "row_count": row_count,
                "column_count": column_count
            }})
        except Exception as e:
            files_info.append({{
                "filename": os.path.basename(file_path),
                "error": str(e)
            }})

    print(json.dumps(files_info, indent=2))
except Exception as e:
    print(f"Error listing files: {{e}}")
"""
        return _execute_in_sandbox(code)

@register_tool("get_parquet_schema")
class GetParquetSchema(BaseTool):
    name = "get_parquet_schema"
    description = "Get schema information of a parquet file."
    parameters = {
        "type": "object",
        "properties": {
            "parquet_file": {
                "type": "string",
                "description": "Name of the parquet file to inspect (e.g. 'logs.parquet')"
            }
        },
        "required": ["parquet_file"]
    }

    def call(self, params: Union[str, dict], **kwargs) -> str:
        if isinstance(params, str):
            params = json.loads(params)
        filename = params.get('parquet_file')
        
        # Handle if user provides full path or just filename
        if "/" in filename:
            filename = os.path.basename(filename)

        code = f"""
import duckdb
import json
import os

file_path = os.path.join("{SANDBOX_DATA_DIR}", "{filename}")

try:
    if not os.path.exists(file_path):
        print(f"Error: File {{file_path}} not found")
    else:
        conn = duckdb.connect(":memory:")
        
        # Get schema
        result = conn.execute(f"SELECT * FROM read_parquet('{{file_path}}') LIMIT 0")
        schema = [{{"name": desc[0], "type": str(desc[1])}} for desc in result.description]
        
        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{{file_path}}')").fetchone()[0]
        
        info = {{
            "file": "{filename}",
            "row_count": row_count,
            "columns": schema
        }}
        print(json.dumps(info, indent=2))
        conn.close()
except Exception as e:
    print(f"Error getting schema: {{e}}")
"""
        return _execute_in_sandbox(code)

@register_tool("query_parquet")
class QueryParquet(BaseTool):
    name = "query_parquet"
    description = "Query parquet files using SQL syntax."
    parameters = {
        "type": "object",
        "properties": {
            "parquet_files": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of parquet filenames to query"
            },
            "query": {
                "type": "string",
                "description": "SQL query to execute"
            },
            "limit": {
                "type": "integer",
                "description": "Max rows to return",
                "default": 10
            }
        },
        "required": ["parquet_files", "query"]
    }

    def call(self, params: Union[str, dict], **kwargs) -> str:
        if isinstance(params, str):
            params = json.loads(params)
        
        files = params.get('parquet_files', [])
        query = params.get('query')
        limit = params.get('limit', 10)

        # Sanitize filenames
        clean_files = [os.path.basename(f) for f in files]
        
        code = f"""
import duckdb
import json
import os
from datetime import datetime

data_dir = "{SANDBOX_DATA_DIR}"
files = {clean_files}
query = \"\"\"{query}\"\"\"
limit = {limit}

def serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)

try:
    conn = duckdb.connect(":memory:")
    
    # Create views for each file
    # We map the filename (without extension) to a table name
    # e.g. 'abnormal_logs.parquet' -> table 'abnormal_logs'
    
    for fname in files:
        path = os.path.join(data_dir, fname)
        if os.path.exists(path):
            table_name = os.path.splitext(fname)[0]
            # Handle potential SQL injection or invalid chars in table name if needed
            # For now assume safe filenames
            conn.execute(f"CREATE OR REPLACE VIEW {{table_name}} AS SELECT * FROM read_parquet('{{path}}')")
        else:
            print(f"Warning: File {{fname}} not found")

    # Execute Query
    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    
    rows = [dict(zip(columns, row)) for row in result]
    
    # Limit results
    if len(rows) > limit:
        rows = rows[:limit]
        
    print(json.dumps(rows, default=serialize, indent=2))
    conn.close()

except Exception as e:
    print(f"Error executing query: {{e}}")
"""
        return _execute_in_sandbox(code)

