import psycopg2
from psycopg2.errors import Error as PsycopgError
import os
import sys
from dotenv import load_dotenv

load_dotenv()

PG_CONN_INFO = {
    "dbname"    : os.getenv("POSTGRES_DB"),
    "user"      : os.getenv("POSTGRES_USER"),
    "password"  : os.getenv("POSTGRES_PASSWORD"),
    "host"      : os.getenv("POSTGRES_HOST"),
    "port"      : os.getenv("POSTGRES_PORT")
}

SQL_FILE_PATH = r"D:\Project\Data_Engineering\DE_Pipeline\sql\schema.sql"

# Tên schema và các bảng quan trọng cần kiểm tra
PG_SCHEMA_NAME = "risk_dwh"
EXPECTED_PG_TABLES = [
    'dim_date', 
    'dim_risk_category', 
    'dim_business_unit',
    'dim_control',
    'dim_customer',
    'dim_product',
    'dim_risk_rating',
    'fact_risk_event',
    'fact_risk_assessment'
]

class SchemaManager:
    """
    Quản lý việc tạo và xác thực schema cho PostgreSQL.
    """
    def __init__(self, pg_conn_info):
        self.pg_conn_info = pg_conn_info
        print(f"----> SchemaManager khởi tạo. Sẵn sàng kết nối")

    def create_postgresql_schema(self, sql_file_path):
        print(f"----> Đang thực thi {sql_file_path} trên PostgreSQL")
        if not os.path.exists(sql_file_path):
            print(f"----> Lỗi. Không tìm thấy file {sql_file_path}")
            return False
        try:
            with psycopg2.connect(**self.pg_conn_info) as conn:
                with conn.cursor() as cur:
                    with open(sql_file_path, 'r', encoding='utf-8') as f:
                        sql_scripts = f.read()

                    cur.execute(sql_scripts)
                conn.commit()

            print(f"----> Đã thực thi {sql_file_path} thành công")
            return True
        except (PsycopgError, IOError) as e:
            print(f"----> Lỗi khi tạo PostgreSQL schema: {e}")
            return False
        
    def validate_postgresql_schema(self, schema_name, table_list):
        """
        Xác thực rằng schema và các bảng mong đợi đã tồn tại
        """
        print(f"----> Đang xác thực schema '{schema_name}' của PostgreSQL")
        try:
            with psycopg2.connect(**self.pg_conn_info) as conn:
                with conn.cursor() as cur:
                    # 1. Kiểm tra sự tồn tại của schema
                    cur.execute(
                        "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                        (schema_name,)
                    )
                    if not cur.fetchone():
                        print(f"----> Schema '{schema_name}' không tồn tại ")
                        return False
                    print(f"----> Schema '{schema_name}' tồn tại")
                    
                    # 2. Kiểm tra sự tồn tại của các bảng
                    missing_tables = []
                    for table in table_list:
                        cur.execute(
                            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                            (schema_name, table)
                        )
                        if not cur.fetchone():
                            missing_tables.append(table)
                    
                    if missing_tables:
                        print(f"----> Lỗi. Thiếu các bảng sau: '{missing_tables}'")    
                        return False
                    print(f"----> Tất cả {len(table_list)} bảng đều đã tồn tại")
            print(f"----> Xác thực PostgreSQL thành công")
            return True
        except PsycopgError as e:
            print(f"----> Lỗi khi xác thực PosthreSQL: {e}")
            return False
        

# if __name__ == "__main__":
    
#     manager = SchemaManager(PG_CONN_INFO)

#     if manager.create_postgresql_schema(SQL_FILE_PATH):
#         manager.validate_postgresql_schema(PG_SCHEMA_NAME, EXPECTED_PG_TABLES)
#     else:
#         print("Dừng lại do lỗi khi tạo PostgreSQL schema.")
#         sys.exit(1)