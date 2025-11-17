from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import (
    StructType, StructField,
    ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, DecimalType,
    StringType, BinaryType, BooleanType,
    DateType, TimestampType,
    ArrayType, MapType, DataType
)
# from pyspark.sql.functions import col, from_json
from typing import Optional, Dict, Any, List
# from decimal import Decimal
# from datetime import date, datetime


class SparkDataType:
    """Builder tĩnh để tạo StructField nhanh, đẹp, đúng chuẩn PySpark"""

    @staticmethod
    def byte(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, ByteType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def short(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, ShortType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def int(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, IntegerType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def long(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, LongType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def float(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, FloatType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def double(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, DoubleType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def decimal(precision: int = 10, scale: int = 2):
        def _decimal(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
            return StructField(name, DecimalType(precision, scale), nullable=nullable, metadata=metadata or {})
        return _decimal

    @staticmethod
    def string(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, StringType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def binary(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, BinaryType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def boolean(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, BooleanType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def date(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, DateType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def timestamp(name: str, nullable: bool = False, metadata: Optional[Dict[str, Any]] = None) -> StructField:
        return StructField(name, TimestampType(), nullable=nullable, metadata=metadata or {})

    @staticmethod
    def array(element_type: DataType):

        def _array(name: str, nullable: bool = True, metadata: Optional[Dict[str, Any]] = None) -> StructField:
            return StructField(name, ArrayType(element_type, containsNull=True), nullable=nullable, metadata=metadata or {})
        return _array
    
    
    @staticmethod
    def map(key_type: DataType, value_type: DataType):
        def _map(name: str, nullable: bool = True, metadata: Optional[Dict[str, Any]] = None) -> StructField:
            return StructField(name, MapType(key_type, value_type), nullable=nullable, metadata=metadata or {})
        return _map
    
    
    @staticmethod
    def struct(fields: List[StructField]):
        def _struct(name: str, nullable: bool = True, metadata: Optional[Dict[str, Any]] = None) -> StructField:
            return StructField(name, StructType(fields), nullable=nullable, metadata=metadata or {})
        return _struct

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        SparkDataType.long("id", nullable=False),
        SparkDataType.array(StringType())("tags"),
        SparkDataType.map(StringType(), IntegerType())("scores"),
        SparkDataType.struct([
            SparkDataType.string("city"),
            SparkDataType.int("population")
        ])("info")
    ])

    data = [
        (1, ["gold", "vip"], {"math": 95, "english": 88}, Row(city="Hà Nội", population=8000000)),
        (2, None, {}, None)
    ]

    spark.createDataFrame(data, schema).show()
    spark.stop()