# src/de_pipeline.py
from typing import Optional, List, Callable, Any
from sandbox.duckdb.duckdb_class import DuckDBConnection


class DuckPipeline:
    """
    Flexible DEPipeline for DuckDB with custom Python transforms.

    Features:
    - extract: from file (CSV, Parquet, JSON) or SQL query
    - transform: accepts any callable; can run multiple SQL, create temp tables, UDFs, loops, joins
    - load: export DuckDB table to file
    - Prints progress; no logger
    """

    def __init__(
        self,
        db: DuckDBConnection,
        name: str = "pipeline",
        stages: Optional[List[str]] = None,
    ):
        """
        Initialize the pipeline.

        Parameters
        ----------
        db : DuckDBConnection
            Active DuckDB connection
        name : str
            Pipeline name
        stages : list of str, optional
            Pipeline stages to run: ["extract", "transform", "load"] by default
        """
        self.db = db
        self.name = name
        self.stages = stages or ["extract", "transform", "load"]
        self.metadata = {
            "rows_extracted": 0,
            "rows_transformed": 0,
            "rows_loaded": 0,
        }
        self.temp_tables = []  # Track temporary tables (optional cleanup)
        print(f"DEPipeline '{self.name}' is ready!")

    def extract(self, source: str, table_name: str = "raw_data"):
        """
        Extract data from a file or SQL query and store it as a DuckDB table.

        Parameters
        ----------
        source : str
            File path (CSV, Parquet, JSON) or SQL SELECT query
        table_name : str
            Target DuckDB table name for extracted data
        """
        print(f"Extracting from: {source}")
        try:
            if source.endswith((".csv", ".parquet", ".json", ".csv.gz")):
                query = (
                    f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM '{source}'"
                )
            elif source.startswith(("SELECT", "WITH")):
                query = f"CREATE OR REPLACE TABLE {table_name} AS {source}"
            else:
                raise ValueError("Source must be a file or a SQL SELECT query")

            self.db.query(query)
            count = self.db.query(f"SELECT COUNT(*) FROM {table_name}").iloc[0, 0]
            self.metadata["rows_extracted"] = count
            self.temp_tables.append(table_name)
            print(f"Extraction successful: {count:,} rows → '{table_name}'")
        except Exception as e:
            print(f"Extraction failed: {e}")
            raise

    def transform(self, transform_func: Callable[[DuckDBConnection], Any]):
        """
        Transform the data using a custom Python function.

        The function can:
        - Execute multiple SQL statements
        - Create temporary tables
        - Register UDFs
        - Use loops, joins, window functions
        - Return either the row count or None
        """
        print("Starting transform (custom function)...")
        try:
            result = transform_func(self.db)

            if isinstance(result, int):
                self.metadata["rows_transformed"] = result
                print(f"Transform completed: {result:,} rows")
            else:
                try:
                    count = self.db.query("SELECT COUNT(*) FROM transformed").iloc[0, 0]
                    self.metadata["rows_transformed"] = count
                    print(
                        f"Transform completed: {count:,} rows (from table 'transformed')"
                    )
                except:
                    print("Transform completed (no 'transformed' table detected)")
        except Exception as e:
            print(f"Transform failed: {e}")
            raise

    def load(
        self,
        table_name: str = "transformed",
        output_path: str = "",
        format: str = "PARQUET",
    ):
        """
        Load a DuckDB table into a file.

        Parameters
        ----------
        table_name : str
            Source DuckDB table
        output_path : str
            Output file path
        format : str
            File format: PARQUET, CSV, or JSON
        """
        if not output_path:
            print("No output path provided → skipping load")
            return

        print(f"Loading '{table_name}' → {output_path}")
        try:
            format = format.upper()
            if format not in ["PARQUET", "CSV", "JSON"]:
                raise ValueError("Supported formats: PARQUET, CSV, JSON")

            query = f"COPY {table_name} TO '{output_path}' (FORMAT {format})"
            self.db.query(query)
            count = self.metadata.get("rows_transformed", 0)
            self.metadata["rows_loaded"] = count
            print(f"Load successful: {count:,} rows → {output_path}")
        except Exception as e:
            print(f"Load failed: {e}")
            raise

    def run(
        self,
        extract_source: Optional[str] = None,
        transform_func: Optional[Callable[[DuckDBConnection], Any]] = None,
        load_path: Optional[str] = None,
        load_table: str = "transformed",
        load_format: str = "PARQUET",
    ):
        """
        Run the full pipeline: extract → transform → load.

        Parameters
        ----------
        extract_source : str, optional
            File path or SQL query for extraction
        transform_func : Callable, optional
            Custom Python function for transformation
        load_path : str, optional
            Output file path
        load_table : str
            DuckDB table to export
        load_format : str
            Output file format
        """
        print(f"\nSTARTING PIPELINE: {self.name.upper()}")
        print("-" * 60)

        try:
            if "extract" in self.stages and extract_source:
                self.extract(extract_source)

            if "transform" in self.stages and transform_func:
                self.transform(transform_func)

            if "load" in self.stages and load_path:
                self.load(load_table, load_path, load_format)

            print("-" * 60)
            print(f"PIPELINE '{self.name}' COMPLETED!")
            print("Statistics:")
            print(f"   • Extracted  : {self.metadata['rows_extracted']:,} rows")
            print(f"   • Transformed: {self.metadata['rows_transformed']:,} rows")
            print(f"   • Loaded     : {self.metadata['rows_loaded']:,} rows")
        except Exception as e:
            print(f"PIPELINE FAILED: {e}")
            raise

    def __enter__(self):
        """Enable usage as a context manager: 'with DEPipeline(...) as pipeline:'"""
        return self

    def __exit__(self, *args):
        """Optional cleanup: drop temporary tables if desired"""
        # for tbl in self.temp_tables:
        #     try: self.db.query(f"DROP TABLE IF EXISTS {tbl}")
        #     except: pass
        pass


# USEd
# with DuckDBConnection(db_path="data/analytics.db") as db:

#     # Hàm transform tự do – bạn làm gì cũng được!
#     def advanced_transform(db_conn):
#         # 1. Tạo bảng trung gian
#         db_conn.query("""
#             CREATE OR REPLACE TABLE cleaned AS
#             SELECT *,
#                    TRIM(name) AS clean_name,
#                    amount * 1.1 AS amount_tax
#             FROM raw_data
#             WHERE amount > 0
#         """)

#         # 2. Dùng UDF nếu có
#         # db_conn.functions.register_udf(...)

#         # 3. Tính toán phức tạp
#         db_conn.query("""
#             CREATE OR REPLACE TABLE monthly AS
#             SELECT
#                 customer_id,
#                 DATE_TRUNC('month', order_date) AS month,
#                 SUM(amount_tax) AS revenue,
#                 COUNT(*) AS orders
#             FROM cleaned
#             GROUP BY 1, 2
#             HAVING revenue > 1000
#         """)

#         # 4. Tạo bảng cuối
#         db_conn.query("""
#             CREATE OR REPLACE TABLE transformed AS
#             SELECT * FROM monthly ORDER BY revenue DESC
#         """)

#         # Trả về số dòng
#         return db_conn.query("SELECT COUNT(*) FROM transformed").iloc[0, 0]

#     # CHẠY PIPELINE
#     with DEPipeline(db, name="ADVANCED_SALES") as p:
#         p.run(
#             extract_source="s3://data/sales_2025.parquet",
#             transform_func=advanced_transform,
#             load_path="output/top_customers_monthly.parquet",
#             load_format="PARQUET"
#         )
