# src/duckdb_connection.py
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from typing import Union, Optional
from multiprocessing import cpu_count
from sandbox.duckdb.config import DuckDBConfig


class DuckDBConnection:
    """
    DuckDBConnection – lightweight and flexible wrapper around DuckDB.
    Supports registering pandas, polars, and Arrow datasets directly.
    Automatically applies configuration and provides context manager support.
    """

    def __init__(
        self, db_path: str = ":memory:", config: Optional[DuckDBConfig] = None
    ):
        self.db_path = db_path
        self.config = config or DuckDBConfig()
        self.con = None

        try:
            # Validate and initialize configuration
            self.config.validate()
            duckdb.query(
                f"SET python_enable_replacements = {self.config.python_enable_replacements}"
            )

            # Establish connection
            self.con = duckdb.connect(database=self.db_path)
            self._apply_config()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize DuckDBConnection: {e}")

    # -----------------------------
    # Table Management
    # -----------------------------
    def add(
        self, name: str, data: Union[pd.DataFrame, pl.DataFrame, pa.Table, ds.Dataset]
    ):
        """Register a pandas/polars/Arrow dataset as a DuckDB table."""
        try:
            if isinstance(data, pd.DataFrame):
                self.con.register(name, data)
            elif isinstance(data, pl.DataFrame):
                self.con.register(name, data.to_pandas())
            elif isinstance(data, pa.Table):
                self.con.register(name, data.to_pandas())
            elif isinstance(data, ds.Dataset):
                self.con.register(name, data.to_table().to_pandas())
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
                f"SET enable_external_access = {cfg['enable_external_access']};"
            )
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
