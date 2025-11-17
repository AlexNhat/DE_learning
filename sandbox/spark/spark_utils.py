from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from typing import Dict, Any, Optional, List
import pandas as pd
import logging

from sandbox.spark.spark_configs import SparkConfigs

class SparkUtils:
    
    @staticmethod    
    def create_session(app_name: str = None, master: str = "local[*]", configs: Dict[str, Any] = None) -> SparkSession:
        try: 
            builder = SparkSession.builder.appName(app_name).master(master)
            if configs:
                builder = SparkConfigs.apply_configs(builder, configs)
            return builder.getOrCreate()
            
        except Exception as e:
            logging.error(f"spark utils error in create session with error {e}")
            raise

    @staticmethod
    def stop_session(spark: SparkSession):
        spark.stop()
        return None

    @staticmethod
    def set_runtime_config(spark: SparkSession, key: str, value: Any):
        """
        Set runtime config sau khi session đã tạo (chỉ cho các config modifiable, như sql.arrow).
        - Kiểm tra docs để biết config nào là runtime (e.g., spark.sql.* thường là).
        - Nếu là static config, method này sẽ raise warning nhưng vẫn set (nhưng không hiệu lực).
        """
        try:
            spark.conf.set(key, value)
            print(f"Runtime config '{key}' set to '{value}' successfully.")
        except Exception as e:
            logging.error(f"Error setting runtime config '{key}': {str(e)} (có thể là static config, không thể thay đổi sau).")
            raise

    @staticmethod
    def read_csv_with_pandas(path: str, **kwargs) -> pd.DataFrame:
        """
        Đọc file CSV sử dụng Pandas.
        - path: Đường dẫn file (local hoặc hỗ trợ Pandas như s3 với fsspec).
        - **kwargs: Các options của pd.read_csv() như delimiter, header, dtype, etc.
        Returns: Pandas DataFrame.
        """
        try:
            return pd.read_csv(path, **kwargs)
        except Exception as e:
            logging.error(f"Error reading CSV with Pandas: {str(e)}")
            raise

    @staticmethod
    def read_excel_with_pandas(path: str, sheet_name: str = 0, **kwargs) -> pd.DataFrame:
        """
        Đọc file Excel sử dụng Pandas.
        - path: Đường dẫn file.
        - sheet_name: Tên sheet hoặc index (default: 0).
        - **kwargs: Options của pd.read_excel() như header, usecols, etc.
        Returns: Pandas DataFrame.
        """
        try:
            return pd.read_excel(path, sheet_name=sheet_name, **kwargs)
        except Exception as e:
            logging.error(f"Error reading Excel with Pandas: {str(e)}")
            raise

    @staticmethod
    def read_json_with_pandas(path: str, **kwargs) -> pd.DataFrame:
        """
        Đọc file JSON sử dụng Pandas.
        - path: Đường dẫn file.
        - **kwargs: Options của pd.read_json() như orient, lines, etc.
        Returns: Pandas DataFrame.
        """
        try:
            return pd.read_json(path, **kwargs)
        except Exception as e:
            logging.error(f"Error reading JSON with Pandas: {str(e)}")
            raise

    @staticmethod
    def read_parquet_with_pandas(path: str, **kwargs) -> pd.DataFrame:
        """
        Đọc file Parquet sử dụng Pandas (yêu cầu pyarrow hoặc fastparquet installed).
        - path: Đường dẫn file.
        - **kwargs: Options của pd.read_parquet() như columns, use_threads, etc.
        Returns: Pandas DataFrame.
        """
        try:
            return pd.read_parquet(path, **kwargs)
        except Exception as e:
            logging.error(f"Error reading Parquet with Pandas: {str(e)}")
            raise

    @staticmethod
    def to_spark_df(spark: SparkSession, pd_df: pd.DataFrame, schema: Optional[StructType] = None) -> DataFrame:
        """
        Convert Pandas DataFrame sang Spark DataFrame.
        - spark: SparkSession.
        - pd_df: Pandas DataFrame.
        - schema: Optional schema để enforce types (StructType).
        Returns: Spark DataFrame.
        Note: Tối ưu nếu Arrow enabled.
        """
        try:
            return spark.createDataFrame(pd_df, schema=schema)
        except Exception as e:
            logging.error(f"Error converting Pandas to Spark DF: {str(e)}")
            raise

    @staticmethod
    def to_pandas_df(spark_df: DataFrame) -> pd.DataFrame:
        """
        Convert Spark DataFrame sang Pandas DataFrame.
        - spark_df: Spark DataFrame.
        Returns: Pandas DataFrame (collect toàn bộ data về driver, chỉ dùng cho small data).
        Warning: Có thể gây OOM nếu data lớn.
        """
        try:
            return spark_df.toPandas()
        except Exception as e:
            logging.error(f"Error converting Spark to Pandas DF: {str(e)}")
            raise

    @staticmethod
    def write_pandas_to_csv(pd_df: pd.DataFrame, path: str, **kwargs):
        """
        Ghi Pandas DataFrame ra file CSV.
        - pd_df: Pandas DataFrame.
        - path: Đường dẫn output.
        - **kwargs: Options của pd.to_csv() như index, header, etc.
        """
        try:
            pd_df.to_csv(path, **kwargs)
        except Exception as e:
            logging.error(f"Error writing Pandas to CSV: {str(e)}")
            raise

    @staticmethod
    def write_pandas_to_excel(pd_df: pd.DataFrame, path: str, sheet_name: str = 'Sheet1', **kwargs):
        """
        Ghi Pandas DataFrame ra file Excel.
        - pd_df: Pandas DataFrame.
        - path: Đường dẫn output.
        - sheet_name: Tên sheet.
        - **kwargs: Options của pd.to_excel() như index, header, etc.
        """
        try:
            pd_df.to_excel(path, sheet_name=sheet_name, **kwargs)
        except Exception as e:
            logging.error(f"Error writing Pandas to Excel: {str(e)}")
            raise

    @staticmethod
    def write_pandas_to_json(pd_df: pd.DataFrame, path: str, **kwargs):
        """
        Ghi Pandas DataFrame ra file JSON.
        - pd_df: Pandas DataFrame.
        - path: Đường dẫn output.
        - **kwargs: Options của pd.to_json() như orient, lines, etc.
        """
        try:
            pd_df.to_json(path, **kwargs)
        except Exception as e:
            logging.error(f"Error writing Pandas to JSON: {str(e)}")
            raise

    @staticmethod
    def write_pandas_to_parquet(pd_df: pd.DataFrame, path: str, **kwargs):
        """
        Ghi Pandas DataFrame ra file Parquet.
        - pd_df: Pandas DataFrame.
        - path: Đường dẫn output.
        - **kwargs: Options của pd.to_parquet() như compression, partition_cols, etc.
        """
        try:
            pd_df.to_parquet(path, **kwargs)
        except Exception as e:
            logging.error(f"Error writing Pandas to Parquet: {str(e)}")
            raise

    @staticmethod
    def read_file_to_spark_df(spark: SparkSession, path: str, format: str = 'csv', schema: Optional[StructType] = None, pandas_options: Dict[str, Any] = None) -> DataFrame:
        """
        Đọc file sử dụng Pandas rồi convert sang Spark DF (mở rộng cho nhiều format).
        - spark: SparkSession.
        - path: Đường dẫn file.
        - format: 'csv', 'excel', 'json', 'parquet'.
        - schema: Optional schema cho Spark DF.
        - pandas_options: Dict options cho Pandas read functions.
        Returns: Spark DataFrame.
        """
        pandas_options = pandas_options or {}
        if format == 'csv':
            pd_df = SparkUtils.read_csv_with_pandas(path, **pandas_options)
        elif format == 'excel':
            pd_df = SparkUtils.read_excel_with_pandas(path, **pandas_options)
        elif format == 'json':
            pd_df = SparkUtils.read_json_with_pandas(path, **pandas_options)
        elif format == 'parquet':
            pd_df = SparkUtils.read_parquet_with_pandas(path, **pandas_options)
        else:
            raise ValueError(f"Unsupported format: {format}")
        return SparkUtils.to_spark_df(spark, pd_df, schema)

    @staticmethod
    def write_spark_df_to_file(spark_df: DataFrame, path: str, format: str = 'csv', pandas_options: Dict[str, Any] = None):
        """
        Convert Spark DF sang Pandas rồi ghi ra file (cho small data).
        - spark_df: Spark DataFrame.
        - path: Đường dẫn output.
        - format: 'csv', 'excel', 'json', 'parquet'.
        - pandas_options: Dict options cho Pandas write functions.
        """
        pandas_options = pandas_options or {}
        pd_df = SparkUtils.to_pandas_df(spark_df)
        if format == 'csv':
            SparkUtils.write_pandas_to_csv(pd_df, path, **pandas_options)
        elif format == 'excel':
            SparkUtils.write_pandas_to_excel(pd_df, path, **pandas_options)
        elif format == 'json':
            SparkUtils.write_pandas_to_json(pd_df, path, **pandas_options)
        elif format == 'parquet':
            SparkUtils.write_pandas_to_parquet(pd_df, path, **pandas_options)
        else:
            raise ValueError(f"Unsupported format: {format}")

    @staticmethod
    def validate_df(df: DataFrame, expected_schema: Optional[StructType] = None) -> bool:
        """
        Validate DataFrame: Check nulls, duplicates, schema match.
        - df: DataFrame cần validate.
        - expected_schema: Schema mong đợi (optional).
        Returns: True nếu valid, else raise exception.
        """
        if expected_schema:
            if df.schema != expected_schema:
                raise ValueError("Schema mismatch!")
        # Check duplicates (ví dụ)
        if df.count() != df.dropDuplicates().count():
            raise ValueError("DataFrame has duplicates!")
        return True

# if __name__ == "__main__":
#     spark = SparkUtils.create_session("PandasIntegrationApp")

#     # Ví dụ đọc CSV với Pandas rồi convert sang Spark DF
#     csv_path = "data/sample.csv"
#     pd_options = {"header": True, "delimiter": ","}
#     spark_df = SparkUtils.read_file_to_spark_df(spark, csv_path, format='csv', pandas_options=pd_options)
#     spark_df.show()

#     # Ví dụ convert Spark DF sang Pandas và ghi ra Excel
#     excel_path = "output/sample.xlsx"
#     SparkUtils.write_spark_df_to_file(spark_df, excel_path, format='excel', pandas_options={"index": False})

#     spark.stop()