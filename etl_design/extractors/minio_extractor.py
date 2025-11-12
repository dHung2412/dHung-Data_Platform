import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from etl_design.base_etl import BaseETL
from connector_storage.minio_connector import MinIOConnector
import tempfile


class Minio_Extracter(BaseETL):
    """Extract files from MinIO storage"""

    def __init__(self, minio_config):
        super().__init__(Minio_Extracter)
        self.config = minio_config
        self.connector = None
    
    def execute(self, bucket_name: str, object_name: str) -> str:
        try: 
            self.log_info(f"----> Extracting {bucket_name}/{object_name} from MinIO")

            self.connector = MinIOConnector(
                endpoint=self.config.endpoint,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                secure=self.config.secure
            )

            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
            local_path = temp_file.name
            temp_file.close()

            success = self.connector.download_file(bucket_name, object_name, local_path)
            if success:
                self.log_info(f"----> Successfully extracted to {local_path}")
                return local_path
            else:
                self.log_error(f"----> Failed to download file from MinIO")
                return None
        except Exception as e:
            self.log_error(f"----> Error extracting from MinIO: {e}")
            return None