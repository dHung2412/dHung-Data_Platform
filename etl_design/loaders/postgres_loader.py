import pandas as pd
from etl_design.base_etl import BaseETL
from connector_storage.postgresql_connector import PostgresConnect
from typing import Dict

class Postgres_Loader(BaseETL):
    """Load transformed data into PostgreSQL"""
    def __init__(self, postgres_config):
        super().__init__("PostgresLoader")
        self.config = postgres_config
        self.connector = None

    def connect(self):
        """Establish PostgreSQL connection"""
        self.connector = PostgresConnect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            dbname=self.config.database
        )
        self.connector.connect()

    def execute(self, dimensions: Dict[str, pd.DataFrame], 
                      facts: Dict[str, pd.DataFrame]) -> Dict:
        """
        Load dimensions and facts to PostgreSQL
        
        Returns:
            Dict with dimension key mappings for fact table loading
        """
        try:
            if not self.connector:
                self.connect()
            
            # Load dimensions first
            dim_keys = self._load_dimensions(dimensions)
            
            # Load facts with dimension keys
            self._load_facts(facts, dim_keys)
            
            self.log_info("----> Data loading completed successfully")
            return dim_keys
            
        except Exception as e:
            self.log_error(f"Error loading data: {e}")
            if self.connector and self.connector.conn:
                self.connector.conn.rollback()
            raise

    def _load_dimensions(self, dimensions: Dict[str, pd.DataFrame]) -> Dict:
        """Load dimension tables and return key mappings"""
        self.log_info(f"----> Loading dimension table")

        dim_keys = {}

        load_order = ['dim_customer', 'dim_customer_pii', 'dim_branch','dim_account', 'dim_card', 'dim_loan', 'dim_date']
        
        for dim_name in load_order:
            if dim_name in dimensions:
                self.log_info(f"----> Loading {dim_name}")
                keys = _load_dimension_table(dim_name, dimensions[dim_name])
                if keys:
                    dim_keys[dim_name] = keys
        return dim_keys
    
    def _load_dimension_table(self, dim_table: str, df: pd.DataFrame) -> Dict:
        """
        Load a single dimension table with SCD Type 2 support
        Returns mapping of business_key -> surrogate_key
        """
        cursor = self.connector.cursor
        key_mapping = {}

        if dim_table == 'dim_date':
            for _, row in df.iterrows():
                cursor.execute(f"""
                
""")

