import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.base_config import get_database_config
from connector_storage.minio_connector import MinIOConnector

config = get_database_config()
with MinIOConnector(config["minio"].endpoint,
                    config["minio"].access_key,
                    config["minio"].secret_key,
                    config["minio"].secure) as minio_client:
    if minio_client:
        print(f"----> Minio Client đã được khởi tạo")
    minio_client.upload_file("raw-banking-data","comprehensive.csv","data/storage/Comprehensive_Banking_Database.csv")
    