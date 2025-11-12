# src/duckdb_connection.py
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import os
import yaml
from pathlib import Path
from typing import Union, Optional, Dict, Any
from multiprocessing import cpu_count
from config import *


class DuckDBConnection:
    """
    DuckDB Connection SIÊU MẠNH – truyền thẳng df, polars, arrow vào __init__
    Tự động register + optimize config + hỗ trợ dev/prod
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        config_path: str = "config/duckdb.yaml",
        env: Optional[str] = None,
        # TRUYỀN DATA NGAY TẠI ĐÂY
        pandas_df: Optional[pd.DataFrame] = None,
        polars_df: Optional[pl.DataFrame] = None,
        arrow_table: Optional[pa.Table] = None,
        arrow_dataset: Optional[ds.Dataset] = None,
        **named_data: Any,  # Ví dụ: sales=my_pdf, users=pl_df
    ):
        self.db_path = db_path
        self.env = (env or os.getenv("ENV", "dev")).lower()
        self.con = duckdb.connect(database=db_path, read_only=False)
        self._load_and_apply_config(config_path)
        print(f"DuckDB Connected: {db_path} | Mode: {self.env.upper()}")

        # TỰ ĐỘNG REGISTER TẤT CẢ DATA TRUYỀN VÀO
        self._auto_register(pandas_df, "pandas_df")
        self._auto_register(polars_df, "polars_df")
        self._auto_register(arrow_table, "arrow_table")
        self._auto_register(arrow_dataset, "arrow_dataset")

        # Register các named_data (sales=df, users=pl_df, ...)
        for name, data in named_data.items():
            self._auto_register(data, name)

    def _load_and_apply_config(self, config_path: str):
        cfg_file = Path(__file__).parent.parent / config_path
        if cfg_file.exists():
            with open(cfg_file) as f:
                full_cfg = yaml.safe_load(f)
                cfg = full_cfg.get(self.env, full_cfg.get("dev", {}))
        else:
            cfg = {}

        threads = cfg.get("threads", 8 if self.env == "dev" else 0)
        if threads == 0:
            threads = cpu_count()
        self.con.execute(f"SET threads = {threads};")
        self.con.execute(
            f"SET memory_limit = '{cfg.get('memory_limit', '4GB' if self.env == 'dev' else '80%')}';"
        )
        self.con.execute(
            f"SET enable_external_access = {cfg.get('enable_external_access', True if self.env == 'dev' else False)};"
        )
        self.con.execute(
            f"SET preserve_insertion_order = {cfg.get('preserve_insertion_order', True if self.env == 'dev' else False)};"
        )
        self.con.execute(
            f"SET enable_progress_bar = {cfg.get('enable_progress_bar', True if self.env == 'dev' else False)};"
        )

        temp_dir = cfg.get("temp_directory", "/tmp/duckdb_tmp")
        os.makedirs(temp_dir, exist_ok=True)
        self.con.execute(f"SET temp_directory = '{temp_dir}';")

        self.con.execute("SET enable_object_cache = true;")
        self.con.execute("SET parquet_metadata_cache = true;")

    def _auto_register(self, data: Any, default_name: str):
        """Tự động register + đặt tên thông minh"""
        if data is None:
            return

        # Tạo tên table an toàn
        import re

        safe_name = re.sub(r"\W|^(?=\d)", "_", default_name)

        # Xóa table cũ nếu có
        try:
            self.con.execute(f"DROP TABLE IF EXISTS {safe_name}")
        except:
            pass

        # Register theo kiểu dữ liệu
        if isinstance(data, (pd.DataFrame, pl.DataFrame, pa.Table, ds.Dataset)):
            if isinstance(data, pl.DataFrame):
                data = data.to_arrow()
            self.con.register(safe_name, data)
            rows = self.con.execute(f"SELECT COUNT(*) FROM {safe_name}").fetchone()[0]
            print(f"REGISTERED → {safe_name} ({rows:,} rows)")
        else:
            try:
                self.con.register(safe_name, data)
                print(f"REGISTERED → {safe_name}")
            except Exception as e:
                print(f"Không register được {safe_name}: {e}")

    # Các method tiện ích
    def query(self, sql: str, **params) -> pd.DataFrame:
        return self.con.execute(sql, params).fetchdf()

    def pl(self, sql: str, **params) -> pl.DataFrame:
        return self.con.execute(sql, params).pl()

    def arrow(self, sql: str, **params) -> pa.Table:
        return self.con.execute(sql, params).arrow()

    def close(self):
        self.con.close()
        print("DuckDB connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
