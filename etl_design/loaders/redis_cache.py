import json 
from etl_design.base_etl import BaseETL
from connector_storage.redis_connector import RedisConnect
from typing import Dict

class Redis_Cache(BaseETL):
    def __init__(self, redis_config):
        super().__init__("Redis_Cache")
        self.config = redis_config
        self.connector = None

    def connect(self):
        try:
            self.connector  = RedisConnect(
                host = self.config.host, 
                port = self.config.port,
                user = self.config.user,
                password = self.config.password,
                db = int(self.config.database) if self.config.database.isdigit() else 0
            )
            self.connector.connect()
            self.log_info(f"----> Kết nối Redis thành công.")
        except Exception as e:
            self.log_error(f"----> Lỗi kết nối Redis: {e}")
            self.connector = None

    def execute(self, operation: str, **kwargs):
        try:
            if not self.connector:
                self.connect()
                if not self.connector:
                    return None
            
            if operation == 'cache_dim_keys':
                return self._cache_dim_keys_hash(kwargs.get('table_name'),
                                                  kwargs.get('key_mapping'))
            elif operation == 'get_dim_keys':
                return self._get_dim_keys_hash(kwargs.get('table_name'))
            
            elif operation == 'cache_etl_metadata':
                return self._cache_etl_metadata(kwargs.get('metadata'))
            
            elif operation == 'get_etl_metadata':
                return self._get_etl_metadata()
            else:
                self.log_error(f"----> Unknown operation: {operation}")
                return None
        except Exception as e:
            self.log_error(f"----> Lỗi khi thực thi operation '{operation}': {e}")
            return None
        
    def _cache_dim_keys_hash(self, table_name: str, key_mapping: Dict):
        """
        TỐI ƯU: Cache dimension keys sử dụng Redis HASH.
        
        Format in Redis:
        Key: dim:{table_name} (Kiểu HASH)
        Field: {business_key}
        Value: {surrogate_key}
        """
        if not table_name or not key_mapping:
            self.log_warning(f"----> Bỏ qua cache dim keys vì table_name hoặc key_mapping rỗng.")
            return False
        
        self.log_info(f"----> Catching {len(key_mapping)} keys cho Hash 'dim:{table_name}'")

        redis_key = f"dim:{table_name}"

        pipeline = self.connector.client.pipeline()
        pipeline.hset(redis_key, mapping=key_mapping)
        pipeline.expire(redis_key, 86400)
        pipeline.execute()

        self.log_info(f"----> Successfully cached keys for {table_name}")
        return True
    
    def _get_dim_keys_hash(self, table_name: str) -> Dict:
        """
        Lấy toàn bộ keys từ 1 HASH trong 1 lệnh.
        """
        redis_key = f"dim:{table_name}"

        raw_mapping = self.connector.client.hgetall(redis_key)

        key_mapping = {
            bk.decode('utf-8'): int(sk.decode('utf-8'))
            for bk, sk in raw_mapping.items()
        }

        self.log_info(f"----> Retrieved {len(key_mapping)} keys từ Hash '{redis_key}'")
        return key_mapping
        
    def _cache_etl_metadata(self, metadata: Dict):
        if not metadata or 'run_id' not in metadata:
            self.log_error("----> Không thể cache metadata vì thiếu 'run_id' hoặc metadata rỗng")
            return False
        
        self.log_info(f"----> Caching ETL metadata cho run_id: {metadata.get('run_id')}")

        redis_key = f"etl:metadata:{metadata.get('run_id')}"

        pipeline = self.connector.client.pipeline()

        # Lưu chi tiết run
        pipeline.setex(
            redis_key, 
            86400,
            json.dumps(metadata)
        )
        # Thêm vào danh sách 
        pipeline.lpush("etl:recent_runs", redis_key)
        # Cắt danh sách 
        pipeline.ltrim("etl:recent_runs", 0, 99)

        pipeline.execute()
        return True
    
    def _get_etl_metadata(self):
        self.log_info(f"----> Lấy 10 metadata chạy ETL gần nhất")
        
        recent_run_keys = self.connector.client.lrange("etl:recent_runs", 0, 9)
        
        if not recent_run_keys:
            return []
        
        metadata_json_list = self.connector.client.mget(recent_run_keys)

        metadata_list = []
        for data in metadata_json_list:
            if data:
                metadata_list.append(json.loads(data.decode('utf-0')))

        return metadata_list
    
    def close(self):
        if self.connector:
            self.connector.close()
            self.log_info("----> Đã đóng kết nối Redis")