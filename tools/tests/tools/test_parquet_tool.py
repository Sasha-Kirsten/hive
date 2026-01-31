"""Test for the Parquet tool. Goal: To test the Parquet, read and describe functions."""

from pathline import Path
from unittest import TestCase
from aden_tools.tools.parquet_tool.parquet_tool import register_tools
import pytest
from fasmcp import FastMCP
from unittest.mock import patch

# Test IDs for sandbox environment
WORKSPACE_ID = "test_workspace"
AGENT_ID = "test_agent"
SESSION_ID = "test_session"

@pytest.fixture
def parquet_tool_mcp():
    """Fixture to create an MCP instance with Parquet tool registered."""
    mcp = FastMCP()
    register_tools(mcp)
    return mcp

@pytest.fixture
def parquet_tool_functions(parquet_tool_mcp: FastMCP, tmp_path: Path):
    """Fixture to get the parquet tool functions."""
    with patch("aden_tools.tools.file_system_toolkits.security.WORKSPACES_DIR", str(tmp_path)):
        return {
            "parquet_read": parquet_tool_mcp.get_tool("parquet_read"),
            "describe_parquet": parquet_tool_mcp.get_tool("describe_parquet"),
            "sample_parquet": parquet_tool_mcp.get_tool("sample_parquet"),
            "run_sql_on_parquet": parquet_tool_mcp.get_tool("run_sql_on_parquet"),
        }

@pytest.fixture
def parquet_file_path(tmp_path: Path) -> Path:
    """Fixture to create a sample parquet file for testing."""
    sample_parquet = tmp_path / "sample.parquet"
    # Here you would normally create a sample parquet file.
    # For simplicity, we just return the path.
    return sample_parquet

@pytest.fixture
def sample_parquet_data(session_dir: Path):
    """Fixture to provide sample parquet data."""
    # Here you would normally provide sample data.
    # For simplicity, we just return an empty list.
    parquet_file = session_dir / "sample.parquet"
    parquet_file.write_text("id,name,age,salary,city,department,is_active,score,joined_at\n1,Alice,30,92000,NYC,Engineering,True,4.7,2022-03-14")
    return parquet_file


