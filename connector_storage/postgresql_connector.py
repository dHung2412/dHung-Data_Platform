import psycopg2
from psycopg2 import OperationalError

class PostgresConnect:
    
    def __init__(self, host, port, user, password, dbname):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        
        self.config = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "dbname": dbname
        }
        
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.config)
            self.cursor = self.conn.cursor()
            print(f"----> Connected to PostgreSQL database '{self.dbname}'")
            return self.conn
        except OperationalError as e:
            raise Exception(f"----> Failed to connect to PostgreSQL: {e}") from e

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("----> PostgreSQL connection and cursor closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()