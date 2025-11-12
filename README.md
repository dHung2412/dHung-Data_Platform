DATA WAREHOUSE FOR BANKING
----------------------------------------------------------------------------
Huy hiệu
Trạng thái Build: (Travis CI, GitHub Actions)
Code Coverage: (Codecov, Coveralls)
--------------------------------------

A complete ETL pipeline implementation for a Banking Data Warehouse using dimensional modeling (Kimball methodology)
----------------------------------------------------------------------------
ARCHITECTURE

┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   MinIO     │──►───│  ETL Pipeline│──►───│ PostgreSQL  │
│ (Raw Data)  │      │   (Python)   │      │    (DWH)    │
└─────────────┘      └──────┬───────┘      └─────────────┘
                            │
                            ▼
                     ┌─────────────┐
                     │    Redis    │
                     │  (Caching)  │
                     └─────────────┘
----------------------------------------------------------------------------
It's Scalable
Start with 1000 rows → Scale to 1 billion rows by swapping:
PostgreSQL → Snowflake/Redshift
Local Python → Apache Airflow/Spark
Single machine → Kubernetes cluster


* [Tính Năng](#tính-năng)
* [Công Nghệ Sử Dụng](#công-nghệ-sử-dụng)
* [Cài Đặt](#cài-đặt)
* [Cách Sử Dụng](#cách-sử-dụng)
* [Cách Đóng Góp](#cách-đóng-góp)

## ✨ Tính Năng
* 🚀 Tốc độ xử lý nhanh
* 📦 Không phụ thuộc (zero-dependency)
* 📝 Hỗ trợ 3 định dạng output: JSON, CSV, XML

## 🛠️ Công Nghệ Sử Dụng
* [Node.js](https://nodejs.org/)
* [React](https://reactjs.org/)
* [PostgreSQL](https://www.postgresql.org/)
* [Docker](https://www.docker.com/)