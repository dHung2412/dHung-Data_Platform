import os
from dataclasses import dataclass
from typing import Dict, Optional
from dotenv import load_dotenv


@dataclass
class Database_config:
    """Lớp cơ sở để xác thực rằng không có giá trị cấu hình nào bị thiếu."""
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"----> Missing environment variable for config: {key.upper()}")
@dataclass
class RedisConfig(Database_config):
    """Cấu hình cho Redis."""
    host : str
    port : int
    password : str
    database : str
    key_column : str = "id"

@dataclass
class PostgresConfig(Database_config):
    """Cấu hình cho PostgreSQL."""
    host : str
    port : str
    user : str
    password : str
    database : str

def get_database_config()-> Dict[str,Database_config]:
    load_dotenv()
    config = {
        
        "redis": RedisConfig(
            host= os.getenv("REDIS_HOST"),
            port= int(os.getenv("REDIS_PORT")),
            password= os.getenv("REDIS_PASSWORD"),
            database= os.getenv("REDIS_DB"),
        ),

        "postgres": PostgresConfig(
            host= os.getenv("POSTGRES_HOST"),
            port= int(os.getenv("POSTGRES_PORT")),
            user= os.getenv("POSTGRES_USER"),
            password =os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DB"),
        )

    }

    for db , setting in config.items():
        setting.validate()
    return config
