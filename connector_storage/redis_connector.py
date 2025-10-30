import redis
from redis.exceptions import ConnectionError

class RedisConnect:
    def __init__(self, host, port, user, password, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db

        self.config = {"host":host, 
                       "port":port, 
                       "username":user, 
                       "password":password, 
                       "db":db}

        self.client = None

    def connect(self):
        try:
            self.client = redis.Redis(**self.config,decode_responses=True)
            self.client.ping()
            print(f"----> Connected to Redis")
            return self.client
        except ConnectionError as e:
            raise Exception(f"----> Failed to connected Redis: {e}") from e

    def close(self):
        if self.client:
            self.client.close()
            print("----> Redis close")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()