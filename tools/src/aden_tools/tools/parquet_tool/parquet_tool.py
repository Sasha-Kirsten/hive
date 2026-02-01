import importlib.util
import os
from typing import Any

import duckdb
from fastmcp import FastMCP

from ..file_system_toolkits.security import get_secure_path

def register_tools(mcp: FastMCP):
    """Register Parquet Tool with the MCP server."""

    # @mcp.tool()
    def secure_parquet_path(file_path: str, workspace_id, agent_id, session_id) -> str:
        """ Get the secure path for the parquet file."""
        return get_secure_path(file_path, workspace_id, agent_id, session_id,)

    # @mcp.tool()
    def _connect_to_duckdb(file_path: str) -> duckdb.DuckDBPyConnection:
        """ Connect to DuckDB with optimized settings."""
        conn = duckdb.connect(database=":memory:")
        conn.execute("SET enable_progress_bar=false;")
        conn.execute("SET memory_limit='1GB';")
        conn.execute("SET threads=2;")
        return conn

    # @mcp.tool()
    def _limit_sql_query(query: str, limit: int) -> tuple[str, int]:
        """ Add a LIMIT clause to the SQL query if not present."""
        limit = int(limit)
        if limit <= 0:
            raise ValueError("Limit must be a positive integer.")
        limit = min(limit, 100)  # Cap the limit to 100 rows
        wrapped_query = f"SELECT * FROM ({query.rstrip(';')}) AS _q LIMIT {limit}"
        return wrapped_query, limit

    @mcp.tool()
    def parquet_info(
        file_path: str,
        workspace_id: str,
        agent_id: str,
        session_id: str,
        columns_limit: int,
    ):
        """ The function to read the the parquet file and returns lightweight information about its schema.

            The arugments are:
            - file_path: The path to the parquet file.
            - workspace_id: The workspace ID.
            - agent_id: The agent ID.
            - session_id: The session ID.
            - columns_limit: The maximum number of columns to read.

            The function returns: A string representation of the parquet file content.
        """
        if importlib.util.find_spec("duckdb") is None:
            return {"error": "duckdb is not installed. Please install it to use this tool."
                    "pip install duckdb  or  pip install tools[sql]"}
        try:
            file_path = secure_parquet_path(file_path, workspace_id, agent_id, session_id)

            con = _connect_to_duckdb(file_path)
            rel = str(file_path)

            schema_query = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{rel}');").fetchall()
            columns = [{"name": row[0]} for row in schema_query][:columns_limit]
            row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{rel}');").fetchone()[0]
            output ={
                "path": rel,
                "columns": columns,
                "row_count": int(row_count)
            }
            return output
        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def parquet_preview(
        file_path: str,
        workspace_id: str,
        agent_id: str,
        session_id: str,
        limit: int = 100,
        columns: list[str] | None = None,
        where: str | None = None,
    ):
        """ The function to preview the rows in the parquet file, with optional selection and filtering.

            Arugments:
            - file_path: The path to the parquet file.
            - workspace_id: The workspace ID.
            - agent_id: The agent ID.
            - session_id: The session ID.

            Returns:
            - A dictionary containing the schema of the parquet file.
        """
        try:
            parquet_file_path = secure_parquet_path(file_path, workspace_id, agent_id, session_id)
            if not parquet_file_path.endswith(".parquet"):
                return {"error": "The file is not a parquet file."}
            elif not os.path.exists(parquet_file_path):
                return {"error": "The file does not exist."}
            conn = _connect_to_duckdb(parquet_file_path)
            rel = str(parquet_file_path)
            select_cols = "*"

            if columns:
                select_cols = ", ".join([f'"{c}"' for c in columns])

            where_clause = f"WHERE {where}" if where else ""

            query = f"""
                SELECT {select_cols}
                FROM read_parquet('{rel}')
                {where_clause}
                LIMIT {limit};
            """
            result = conn.execute(query).fetchdf()
            # rows = dict(result.to_dict(orient="list"))
            rows = [dict(rec)for rec in result.to_dict(orient="records")]
            out = {
                "path":rel,
                "limit": limit,
                "columns":list(result.columns),
                "rows": rows
            }
            return out
        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def sample_parquet(
        file_path: str,
        n: int = 5,
        workspace_id: str = "",
        agent_id: str = "",
        session_id: str = "",
    ):
        """ The function to execute a SELECT-only query against a Parquet dataset bound as view 't'.
            Arguments:
            - file_path: The path to the parquet file.
            - n: The number of sample rows to return.
            - workspace_id: The workspace ID.
            - agent_id: The agent ID.
            - session_id: The session ID.
        """
        try:
            file_path = secure_parquet_path(file_path, workspace_id, agent_id, session_id)
            conn = _connect_to_duckdb(file_path)
            rel = str(file_path)
            conn.execute(f"CREATE TEMP VIEW t AS SELECT * FROM read_parquet('{rel}');")

            sql_limited, final_limit = _limit_sql_query("SELECT * FROM t", n)
            result = conn.execute(sql_limited).fetchdf()
            rows = [dict(rec)for rec in result.to_dict(orient="records")]
            out = {
                "path": rel,
                "limit": final_limit,
                "columns": list(result.columns),
                "rows": rows
            }
            return out

        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def run_sql_on_parquet(
        file_path: str,
        query: str,
        workspace_id: str,
        agent_id: str,
        session_id: str,
    ):
        """ The function to run SQL query on the parquet file.
            The arugments are:
            - file_path: The path to the parquet file.
            - query: The SQL query to run.
            - workspace_id: The workspace ID.
            - agent_id: The agent ID.
            - session_id: The session ID.

            The function returns: A dictionary containing the query result.
        """
        try:
            file_path = secure_parquet_path(file_path, workspace_id, agent_id, session_id)
            conn = _connect_to_duckdb(file_path)
            rel = str(file_path)
            conn.execute(f"CREATE TEMP VIEW t AS SELECT * FROM read_parquet('{rel}');")
            conn.execute(f"CREATE TEMP VIEW sample_data AS SELECT * FROM read_parquet('{rel}');")

            sql_limited, _ = _limit_sql_query(query, 100)
            result = conn.execute(sql_limited).fetchdf()
            rows = [dict(rec)for rec in result.to_dict(orient="records")]
            out = {
                "path": rel,
                "columns": list(result.columns),
                "rows": rows
            }
            return out
        except Exception as e:
            return {"error": str(e)}
# ------------------------
# MCP registration hook
# ------------------------
def register_parquet_tool(mcp: Any) -> None:
    """
    Register Parquet tools with MCP server/tool registry.
    Adjust dcorator usage to match the repo's MCP server abstraction.
    """
    register_tools(mcp)

