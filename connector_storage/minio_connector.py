import os
from minio import Minio
from minio.error import S3Error

class MinIOConnector:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False):
        """
        MinIO Client
        Args:
            endpoint (str): URL và port của MinIO. Ví dụ: 'localhost:9000'.
            access_key (str): Access key để đăng nhập.
            secret_key (str): Secret key để đăng nhập.
            secure (bool): True nếu sử dụng HTTPS, False nếu dùng HTTP. Mặc định là False.
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure

        try: 
            self.client = Minio(
                endpoint,
                access_key=access_key, 
                secret_key=secret_key, 
                secure=secure
            )
            print(f"----> Kết nối với MinIO thành công")
        except (S3Error, TypeError) as e:
            print(f"----> Lỗi khởi tạo với MinIO client: {e}")
            self.client = None

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        print(f"----> Đóng kết nối MinIO")
        return False
        
    def check_bucket_exists(self, bucket_name: str):
        """
        Xem 1 bucket có tồn tại không, nếu không tạo mới trước khi upload
        """
        if not self.client:
            print(f"----> Client chưa được tạo")
            return False
        try:
            found = self.client.bucket_exists(bucket_name)
            if not found:
                self.client.make_bucket(bucket_name)
                print(f"----> Bucket {bucket_name} đã được tạo")
            else:
                print(f"----> Bucket {bucket_name} đã tồn tại")
        except S3Error as e:
            print(f"---->  Lỗi khi kiểm tra/ tạo bucket {e}")
            return False

    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        """
        Upload file từ local lên MinIO
        Args:
            bucket_name (str): Tên bucket trên MinIO.
            object_name (str): Tên file/đường dẫn object trên MinIO.
            file_path (str): Đường dẫn tới file local cần upload.
        
        Returns:
            bool: True nếu thành công, False nếu thất bại.
        """
        if not self.client:
            print(f"----> Client chưa được tạo")
            return False

        if not os.path.exists(file_path):
            print(f"----> File không tồn tại {file_path}")

        if not self.check_bucket_exists(bucket_name):
            return False

        try:
            print(f"----> Upload file '{file_path}' to '{bucket_name}/{object_name}' ...")
            self.client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=file_path
            )
            print(f"----> Đã tải file lên thành công")
            return True
        except S3Error as e:
            print(f"----> Lỗi khi upload file: {e}")
            return False
        
    def download_file(self, bucket_name: str, object_name: str, file_path: str):
        """
        Tải file từ MinIO về local
        Args:
            bucket_name (str): Tên bucket trên MinIO.
            object_name (str): Tên file/đường dẫn object trên MinIO.
            file_path (str): Đường dẫn lưu file local.

        Returns:
            bool: True nếu thành công, False nếu thất bại.
        """

        if not self.client:
            print(f"----> Client chưa được tạo")
            return False
        
        try:
            print(f"----> Đang download file '{bucket_name}/{object_name}' về '{file_path}'")
            self.client.fget_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=file_path
            )
            print(f"----> Đã tải file về thành công")
            return True
        except S3Error as e:
            print(f"----> Lỗi khi tải file về: {e}")
            return False
    
        
