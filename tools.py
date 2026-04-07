"""
MCP (Model Context Protocol) Server for SQL Server Integration
Exposes database tools that the AI agent can call via the MCP protocol.
"""

import os
import json
import logging
from typing import Any
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── MCP tool definitions (schema) ────────────────────────────────────────────

TOOLS = [
    {
        "name": "get_database_schema",
        "description": (
            "Retrieve the full schema of the connected SQL Server database: "
            "all tables, columns, data types, primary/foreign keys, and row counts. "
            "Always call this first before writing any SQL query."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "include_row_counts": {
                    "type": "boolean",
                    "description": "Whether to include approximate row counts (slightly slower)",
                    "default": True,
                }
            },
            "required": [],
        },
    },
    {
        "name": "execute_sql_query",
        "description": (
            "Execute a SELECT SQL query against the SQL Server database and return results. "
            "Only SELECT statements are allowed; any other statement type will be rejected. "
            "Returns rows as a list of dicts plus column metadata."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A valid T-SQL SELECT statement",
                },
                "max_rows": {
                    "type": "integer",
                    "description": "Maximum rows to return (default 500, max 5000)",
                    "default": 500,
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_table_sample",
        "description": (
            "Fetch a sample of rows from a specific table to understand its data. "
            "Useful before writing complex queries."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table (optionally schema-qualified, e.g. dbo.Orders)",
                },
                "sample_size": {
                    "type": "integer",
                    "description": "Number of sample rows to return (default 5)",
                    "default": 5,
                },
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "validate_sql_query",
        "description": (
            "Validate a SQL query for syntax errors and safety (read-only check) "
            "WITHOUT executing it. Returns validation status and any detected issues."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "SQL query to validate",
                }
            },
            "required": ["query"],
        },
    },
]


# ── Database helper ───────────────────────────────────────────────────────────

class DatabaseManager:
    """Manages SQL Server connections and query execution."""

    def __init__(self):
        self._engine = None

    def _get_connection_string(self) -> str:
        server = os.getenv("SQL_SERVER", "localhost")
        database = os.getenv("SQL_DATABASE", "master")
        username = os.getenv("SQL_USERNAME", "")
        password = os.getenv("SQL_PASSWORD", "")
        driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
        trusted = os.getenv("SQL_TRUSTED_CONNECTION", "0") == "1"

        driver_encoded = driver.replace(" ", "+")
        if trusted:
            return (
                f"mssql+pyodbc://{server}/{database}"
                f"?driver={driver_encoded}&trusted_connection=yes"
            )
        return (
            f"mssql+pyodbc://{username}:{password}@{server}/{database}"
            f"?driver={driver_encoded}"
        )

    def get_engine(self):
        if self._engine is None:
            from sqlalchemy import create_engine
            conn_str = self._get_connection_string()
            self._engine = create_engine(conn_str, pool_pre_ping=True, pool_size=5)
        return self._engine

    def execute_query(self, query: str, max_rows: int = 500) -> dict:
        import pandas as pd
        from sqlalchemy import text
        engine = self.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query))
            columns = list(result.keys())
            rows = result.fetchmany(max_rows)
            df = pd.DataFrame(rows, columns=columns)
        return {
            "columns": columns,
            "rows": df.to_dict(orient="records"),
            "row_count": len(df),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        }

    def get_schema(self, include_row_counts: bool = True) -> dict:
        from sqlalchemy import text
        engine = self.get_engine()

        schema_query = """
        SELECT
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY,
            CASE WHEN fk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_FOREIGN_KEY,
            fk.REFERENCED_TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES t
        JOIN INFORMATION_SCHEMA.COLUMNS c
            ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        LEFT JOIN (
            SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) pk ON pk.TABLE_NAME = c.TABLE_NAME AND pk.TABLE_SCHEMA = c.TABLE_SCHEMA
             AND pk.COLUMN_NAME = c.COLUMN_NAME
        LEFT JOIN (
            SELECT
                ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME,
                ccu.TABLE_NAME AS REFERENCED_TABLE_NAME
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON rc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
        ) fk ON fk.TABLE_NAME = c.TABLE_NAME AND fk.TABLE_SCHEMA = c.TABLE_SCHEMA
             AND fk.COLUMN_NAME = c.COLUMN_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
        """

        with engine.connect() as conn:
            result = conn.execute(text(schema_query))
            rows = result.fetchall()
            columns = list(result.keys())

        schema: dict = {}
        for row in rows:
            r = dict(zip(columns, row))
            key = f"{r['TABLE_SCHEMA']}.{r['TABLE_NAME']}"
            if key not in schema:
                schema[key] = {"columns": [], "row_count": None}
            schema[key]["columns"].append({
                "name": r["COLUMN_NAME"],
                "type": r["DATA_TYPE"],
                "max_length": r["CHARACTER_MAXIMUM_LENGTH"],
                "nullable": r["IS_NULLABLE"],
                "default": r["COLUMN_DEFAULT"],
                "primary_key": r["IS_PRIMARY_KEY"] == "YES",
                "foreign_key": r["IS_FOREIGN_KEY"] == "YES",
                "references": r["REFERENCED_TABLE_NAME"],
            })

        if include_row_counts:
            count_query = """
            SELECT
                s.name + '.' + t.name AS full_name,
                p.rows AS row_count
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.partitions p ON t.object_id = p.object_id
            WHERE p.index_id IN (0,1)
            """
            with engine.connect() as conn:
                result = conn.execute(text(count_query))
                for row in result:
                    if row[0] in schema:
                        schema[row[0]]["row_count"] = row[1]

        return schema


db_manager = DatabaseManager()


# ── Tool handlers ─────────────────────────────────────────────────────────────

def handle_get_database_schema(args: dict) -> dict:
    include_counts = args.get("include_row_counts", True)
    try:
        schema = db_manager.get_schema(include_row_counts=include_counts)
        return {"success": True, "schema": schema, "table_count": len(schema)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


def handle_execute_sql_query(args: dict) -> dict:
    query = args.get("query", "").strip()
    max_rows = min(int(args.get("max_rows", 500)), 5000)

    # Safety check
    first_token = query.upper().split()[0] if query else ""
    if first_token not in ("SELECT", "WITH"):
        return {
            "success": False,
            "error": f"Only SELECT queries are allowed. Got: '{first_token}'",
        }

    try:
        result = db_manager.execute_query(query, max_rows=max_rows)
        result["success"] = True
        result["query"] = query
        return result
    except Exception as exc:
        return {"success": False, "error": str(exc), "query": query}


def handle_get_table_sample(args: dict) -> dict:
    table = args.get("table_name", "")
    sample_size = int(args.get("sample_size", 5))
    query = f"SELECT TOP {sample_size} * FROM {table}"
    try:
        result = db_manager.execute_query(query, max_rows=sample_size)
        result["success"] = True
        return result
    except Exception as exc:
        return {"success": False, "error": str(exc)}


def handle_validate_sql_query(args: dict) -> dict:
    query = args.get("query", "").strip()
    issues = []

    upper = query.upper()
    # Dangerous keyword check
    dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "EXEC", "EXECUTE"]
    for kw in dangerous:
        if kw in upper.split():
            issues.append(f"Dangerous keyword detected: {kw}")

    if not query:
        issues.append("Query is empty")
    elif not upper.lstrip().startswith(("SELECT", "WITH")):
        issues.append("Query must start with SELECT or WITH")

    if issues:
        return {"valid": False, "issues": issues, "query": query}

    # Try a SET PARSEONLY (dry-run) via SSMS
    try:
        from sqlalchemy import text
        engine = db_manager.get_engine()
        with engine.connect() as conn:
            conn.execute(text("SET PARSEONLY ON"))
            conn.execute(text(query))
            conn.execute(text("SET PARSEONLY OFF"))
        return {"valid": True, "issues": [], "query": query}
    except Exception as exc:
        return {"valid": False, "issues": [str(exc)], "query": query}


TOOL_HANDLERS = {
    "get_database_schema": handle_get_database_schema,
    "execute_sql_query": handle_execute_sql_query,
    "get_table_sample": handle_get_table_sample,
    "validate_sql_query": handle_validate_sql_query,
}


def call_tool(name: str, arguments: dict) -> Any:
    """Dispatch a tool call and return the result."""
    handler = TOOL_HANDLERS.get(name)
    if handler is None:
        return {"success": False, "error": f"Unknown tool: {name}"}
    return handler(arguments)
