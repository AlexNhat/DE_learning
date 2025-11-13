# src/duckdb_connection.py
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from typing import Union, Optional
from multiprocessing import cpu_count
from sandbox.duckdb.config import DuckDBConfig
from sandbox.duckdb.duckdb_functions import DuckDBFunctions


class DuckDBConnection:
    def __init__(
        self, db_path: str = ":memory:", config: Optional[DuckDBConfig] = None
    ):
        self.db_path = db_path
        self.config = config or DuckDBConfig()

        self.con = None
        duckdb.query(
            f"set python_enable_replacements={self.config.python_enable_replacements}"
        )
        try:
            # Validate config
            self.config.validate()

            # Pre-connect settings
            pre_connect_options = []
            if self.config.enable_external_access:
                pre_connect_options.append("enable_external_access=True")

            connect_args = {}
            # For in-memory, external access not needed
            if self.db_path != ":memory:":
                # DuckDB doesn't accept enable_external_access via connect args, so must set via PRAGMA before any table creation
                pass

            # Establish connection
            self.con = duckdb.connect(database=self.db_path, **connect_args)
            self.functions = DuckDBFunctions(self.con)

            # Runtime settings
            self._apply_config()

        except Exception as e:
            raise RuntimeError(f"Failed to initialize DuckDBConnection: {e}")

    # -----------------------------
    # Table Management
    # -----------------------------
    def add(self, name: str, data: Union[pd.DataFrame, pl.DataFrame]):
        """Register a pandas/polars/Arrow dataset as a DuckDB table."""
        try:
            if isinstance(data, pd.DataFrame):
                self.con.register(name, data)
            elif isinstance(data, pl.DataFrame):
                self.con.register(name, data.to_pandas())
            else:
                raise TypeError(f"Unsupported data type: {type(data)}.")
        except Exception as e:
            raise RuntimeError(f"Error registering table '{name}': {e}")

    def remove(self, name: str):
        """Unregister a table from DuckDB."""
        try:
            self.con.unregister(name)
        except Exception as e:
            raise RuntimeError(f"Error removing table '{name}': {e}")

    # -----------------------------
    # Query Execution
    # -----------------------------
    def query(self, sql: str, **params) -> pd.DataFrame:
        """Execute SQL query and return a pandas DataFrame."""
        try:
            if params:
                return self.con.execute(query=sql, params=params).fetchdf()
            return self.con.execute(query=sql).fetchdf()
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {sql}\nDetails: {e}")

    def pl(self, sql: str, **params) -> pl.DataFrame:
        """Execute SQL query and return a Polars DataFrame."""
        try:
            if params:
                return self.con.execute(query=sql, params=params).pl()
            return self.con.execute(query=sql).pl()
        except Exception as e:
            raise RuntimeError(f"Polars query execution failed: {sql}\nDetails: {e}")

    # -----------------------------
    # Configuration
    # -----------------------------
    def _apply_config(self):
        """Apply DuckDB configuration settings."""
        try:
            cfg = self.config.to_dict()
            if cfg["threads"] == 0:
                cfg["threads"] = cpu_count()

            # Apply core configuration parameters
            self.con.execute(f"SET threads = {cfg['threads']};")
            self.con.execute(f"SET memory_limit = '{cfg['memory_limit']}';")
            self.con.execute(
                f"SET preserve_insertion_order = {cfg['preserve_insertion_order']};"
            )
            self.con.execute(f"SET enable_progress_bar = {cfg['enable_progress_bar']};")

            # Recommended DuckDB performance options
            self.con.execute("SET enable_object_cache = true;")
            self.con.execute("SET parquet_metadata_cache = true;")

        except Exception as e:
            raise RuntimeError(f"Failed to apply DuckDB configuration: {e}")

    # -----------------------------
    # Connection Management
    # -----------------------------
    def close(self):
        """Close the DuckDB connection."""
        try:
            if self.con:
                self.con.close()
                self.con = None
        except Exception as e:
            raise RuntimeError(f"Failed to close DuckDB connection: {e}")

    # -----------------------------
    # Context Manager
    # -----------------------------
    def __enter__(self):
        """Support 'with' context management."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Automatically close connection when exiting context."""
        self.close()


# A = DuckDBConnection()
