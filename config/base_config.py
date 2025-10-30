import os
from dataclasses import dataclass
from typing import Dict
from dotenv import load_dotenv


# ========== BASE CLASS ==========
@dataclass
class DatabaseConfig:
    """Lớp cơ sở để xác thực rằng không có giá trị cấu hình nào bị thiếu."""
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None or value == "":
                raise ValueError(f"----> Missing environment variable for config: {key.upper()}")


# ========== REDIS ==========
@dataclass
class RedisConfig(DatabaseConfig):
    """Cấu hình cho Redis."""
    host: str
    port: int
    password: str
    database: str
    key_column: str = "id"


# ========== POSTGRES ==========
@dataclass
class PostgresConfig(DatabaseConfig):
    """Cấu hình cho PostgreSQL."""
    host: str
    port: int
    user: str
    password: str
    database: str


# ========== MINIO ==========
@dataclass
class MinioConfig(DatabaseConfig):
    """Cấu hình cho MinIO."""
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool


# ========== MAIN CONFIG LOADER ==========
def get_database_config() -> Dict[str, DatabaseConfig]:
    """Load toàn bộ cấu hình từ file .env"""
    load_dotenv()

    config = {
        "redis": RedisConfig(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            password=os.getenv("REDIS_PASSWORD"),
            database=os.getenv("REDIS_DB"),
        ),
        "postgres": PostgresConfig(
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DB"),
        ),
        "minio": MinioConfig(
            endpoint=os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_USER"),
            secret_key=os.getenv("MINIO_PASSWORD"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        ),
    }

    # Validate toàn bộ config
    for name, setting in config.items():
        setting.validate()

    return config
