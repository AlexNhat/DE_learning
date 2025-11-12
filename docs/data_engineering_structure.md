# 📂 Cấu trúc thư mục dự án Data Engineering

```
D:\DE_learning\
├── .env                    # Chứa các biến môi trường, secret keys (thêm vào .gitignore)
├── .gitignore
├── README.md
├── docker-compose.yml      # (Tùy chọn) Dùng để dựng các service như Postgres, Spark, Airflow local
│
├───data_lake/              # Mô phỏng Data Lake, nơi lưu trữ dữ liệu tập trung
│   ├── bronze/             # Tầng dữ liệu thô, không qua chỉnh sửa, lấy từ nguồn
│   │   └── sales/
│   │       └── 2025-11-12_sales_data.json
│   ├── silver/             # Tầng dữ liệu đã được làm sạch, chuẩn hóa, hợp nhất
│   │   └── sales_cleaned/
│   │       └── sales_2025.parquet
│   └── gold/               # Tầng dữ liệu đã được tổng hợp, sẵn sàng cho phân tích
│       └── monthly_revenue/
│           └── revenue_by_month.parquet
│
├───docs/                   # Nơi chứa tài liệu chi tiết hơn
│   ├── architecture.md     # Sơ đồ, giải thích kiến trúc
│   └── data_models.md      # Mô tả các mô hình dữ liệu
│
├───infrastructure/         # (Tùy chọn) Code để tạo hạ tầng (Infrastructure as Code)
│   └── terraform/
│
├───scripts/                # Các script tiện ích dùng chung
│   └── ingest_data.py      # Script để lấy dữ liệu từ API/DB và đưa vào tầng bronze
│
├───sandbox/
│   └── 03_apache_spark/
│       ├── _prepare_data.py # Script để lấy MỘT PHẦN dữ liệu từ data_lake vào đây
│       ├── rdd_basics.py
│       ├── dataframe_api.py
│       └── data/           # Dữ liệu mẫu, được copy từ data_lake để thử nghiệm
│           └── sample_sales.json
│
└───projects/
    └── project_1_batch_etl_pipeline/
        ├── README.md
        ├── requirements.txt
        ├── src/
        │   ├── extract.py   # Đọc dữ liệu từ data_lake/bronze
        │   ├── transform.py # Áp dụng logic, làm sạch -> tạo ra dữ liệu silver/gold
        │   └── load.py      # Ghi dữ liệu đã xử lý vào data_lake/gold
        └── ... (các thư mục khác như tests, config, dags)
```
