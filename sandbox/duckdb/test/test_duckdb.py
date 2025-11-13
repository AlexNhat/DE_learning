# main_full_pipeline.py
from sandbox.duckdb.duckdb_class import DuckDBConnection
from sandbox.duckdb.duckdb_utils import DuckDBUtils
from sandbox.duckdb.duckdb_pipeline import DuckPipeline
from duckdb.typing import DOUBLE, VARCHAR, INTEGER, BOOLEAN, BIGINT
import os

# Tạo thư mục output nếu chưa có
os.makedirs("sandbox/duckdb/test/output", exist_ok=True)

# ========================================
# 1. TẠO DỮ LIỆU BRONZE (100 dòng)
# ========================================
print("Bước 1: Tạo dữ liệu bronze...")
df = DuckDBUtils.generate_test_data(100)
print(f"Đã tạo {len(df)} dòng bronze")
print(df.head(3))

# ========================================
# 2. KẾT HỢP TẤT CẢ: CONNECTION + FUNCTIONS + PIPELINE
# ========================================
with DuckDBConnection(db_path="sandbox/duckdb/test/output/sales.db") as db:

    # Đưa bronze vào DuckDB
    db.add("bronze", df)
    print("Đã add bronze table vào DuckDB")

    # Tạo pipeline
    with DuckPipeline(db, name="SALES_TO_GOLD") as pipeline:

        # ========================================
        # 3. TRANSFORM: DÙNG ĐẦY ĐỦ DuckDBFunctions + UDF
        # ========================================
        def transform_bronze_to_gold(db_conn: DuckDBConnection):
            with db_conn.functions as f:  # TỰ ĐỘNG CLEANUP UDF!

                # UDF 1: Phân loại giá
                f.register_udf(
                    name="price_tier",
                    function=lambda x: (
                        "Premium" if x > 900 else "Standard" if x > 700 else "Basic"
                    ),
                    parameters=[DOUBLE],
                    return_type=VARCHAR,
                    vectorized=False,
                )

                # UDF 2: Tính VAT 10%
                f.register_udf(
                    name="vat_10",
                    function=lambda x: round(x * 1.1, 2),
                    parameters=[DOUBLE],
                    return_type=DOUBLE,
                )

                # UDF 3: Đơn hàng lớn?
                f.register_udf(
                    name="is_big",
                    function=lambda q, p: q * p > 5000,
                    parameters=[BIGINT, DOUBLE],
                    return_type=BOOLEAN,
                )

                # UDF 4: Phân khúc doanh thu
                f.register_udf(
                    name="segment",
                    function=lambda x: (
                        "DIAMOND" if x > 30000 else "PLATINUM" if x > 20000 else "GOLD"
                    ),
                    parameters=[DOUBLE],
                    return_type=VARCHAR,
                )

                print("Đã đăng ký 4 UDF: price_tier, vat_10, is_big, segment")

                # Silver: làm sạch + UDF
                db_conn.query(
                    """
                    CREATE OR REPLACE TABLE silver AS
                    SELECT
                        id,
                        sale_date,
                        product,
                        quantity,
                        price,
                        vat_10(price) AS price_vat,
                        quantity * vat_10(price) AS revenue,
                        price_tier(price) AS tier,
                        is_big(quantity, price) AS big_order_flag
                    FROM bronze
                    WHERE price IS NOT NULL
                """
                )

                # Gold: aggregate theo sản phẩm + tier
                db_conn.query(
                    """
                    CREATE OR REPLACE TABLE gold AS
                    SELECT
                        product,
                        tier,
                        COUNT(*) AS total_orders,
                        SUM(quantity) AS total_qty,
                        ROUND(SUM(revenue), 2) AS total_revenue,
                        COUNTIF(big_order_flag) AS big_orders_count,
                        segment(SUM(revenue)) AS customer_segment
                    FROM silver
                    GROUP BY product, tier
                    ORDER BY total_revenue DESC
                """
                )

                # Final gold table
                db_conn.query(
                    "CREATE OR REPLACE TABLE gold_final AS SELECT * FROM gold"
                )

                count = db_conn.query("SELECT COUNT(*) FROM gold_final").iloc[0, 0]
                print(f"GOLD table created: {count} nhóm")
                return count

        # ========================================
        # 4. CHẠY TOÀN BỘ PIPELINE
        # ========================================
        pipeline.run(
            extract_source=None,
            transform_func=transform_bronze_to_gold,
            load_path="sandbox/duckdb/test/output/sales_warehouse_gold.parquet",
            load_table="gold_final",
        )

    # ========================================
    # 5. XEM KẾT QUẢ GOLD
    # ========================================
    print("\nWAREHOUSE GOLD RESULT (TOP 10):")
    result = db.query("SELECT * FROM gold_final LIMIT 10")
    print(result)

    # Bonus: lưu CSV nếu muốn
    db.query(
        "COPY gold_final TO 'sandbox/duckdb/test/output/sales_warehouse_gold.csv' (HEADER, DELIMITER ',')"
    )
    print("Đã xuất cả Parquet + CSV!")

# ========================================
# HOÀN TẤT!
# ========================================
print("\nTẤT CẢ HOÀN THÀNH!")
print("File output:")
print("   → output/sales_warehouse_gold.parquet")
print("   → output/sales_warehouse_gold.csv")
print("   → warehouse.db (file DuckDB)")
