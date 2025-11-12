import pandas as pd
from etl_design.base_etl import BaseETL
from connector_storage.postgresql_connector import PostgresConnect
from typing import Dict
from sqlalchemy import create_engine

class PostgresLoader(BaseETL):

    def __init__(self, postgres_config):
        super().__init__("PostgresLoader")
        self.config = postgres_config
        self.connector = None
        self.engine = None

        self.table_configs = {
            'dim_customer': {
                'type': 'scd2',
                'business_key': 'customer_id_source',
                'surrogate_key': 'customer_key'
            },
            'dim_customer_pii': {
                'type': 'scd1',
                'business_key': 'customer_key',
                'surrogate_key': 'customer_key'
            },
            'dim_branch': {
                'type': 'scd1',
                'business_key': 'branch_id_source',
                'surrogate_key': 'branch_key'
            },
            'dim_account': {
                'type': 'scd2',
                'business_key': 'account_id_source',
                'surrogate_key': 'account_key'
            },
            'dim_card': {
                'type': 'scd2',
                'business_key': 'card_id_source',
                'surrogate_key': 'card_key'
            },
            'dim_loan': {
                'type': 'scd2',
                'business_key': 'loan_id_source',
                'surrogate_key': 'loan_key'
            },
            'dim_date': {
                'type': 'scd1',
                'business_key': 'date_key',
                'surrogate_key': 'date_key'
            },
        }


    def connect(self):
        self.log_info("----> Connecting to Postgres database")
        self.connector = PostgresConnect(**self.config)
        self.connector.connect()

        db_url = f"postgresql://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}"
        self.engine = create_engine(db_url)
        self.log_info("----> Engine SQLAlchemy created - sẵn sàng cho bulk load.")

    def execute(self, dimensions: Dict[str, pd.DataFrame], facts: Dict[str, pd.DataFrame]) -> Dict:
        """Quy trình chính: Tải Dimensions -> Transform Facts -> Tải Facts."""
        try:
            if not self.connector:
                self.connect()
            # 1. Tải Dimensions
            dim_keys = self._load_dimensions(dimensions)
            
            # 2. Dùng key mapping để tranform các bảng Facts
            self.log_info("----> Transforming fact tables using dimension keys")
            transformed_facts = self._transform_facts(facts, dim_keys)
            
            # 3. Tải Facts
            self._load_facts(transformed_facts)
            
            self.connector.conn.commit()
            self.log_info("----> Data loading completed successfully (Đã commit)")
            return dim_keys
            
        except Exception as e:
            self.log_error(f"----> Error loading data: {e}")
            if self.connector and self.connector.conn:
                self.log_error("----> Rolling back transaction due to error")
                self.connector.conn.rollback()
            raise

    def _load_dimensions(self, dimensions: Dict[str, pd.DataFrame]) -> Dict:
        self.log_info(f"----> BẮT ĐẦU TẢI DIMENSIONS <----")
        all_dim_keys = {}
        
        load_order = ['dim_customer', 'dim_customer_pii', 'dim_branch','dim_account', 'dim_card', 'dim_loan', 'dim_date']
        
        for dim_name in load_order:
            if dim_name in dimensions and dim_name in self.table_configs:
                df = dimensions[dim_name]
                if df.empty:
                    self.log_info(f"----> Dimension table {dim_name} is empty, skipping load.")
                    continue

                self.log_info(f"----> Loading {dim_name}")
                config = self.table_configs[dim_name]

                if config['type'] == 'scd2':
                    keys = self._load_scd2_dimension(dim_name, df, config)
                else:
                    keys = self._load_scd1_dimension(dim_name, df, config)

                if keys:
                    all_dim_keys[dim_name] = keys

        self.log_info(f"----> TẢI DIMENSIONS HOÀN TẤT <----")
        return all_dim_keys

    def _load_scd1_dimension(self, dim_name: str, df: pd.DataFrame, config: Dict) -> Dict:
        staging_table = f"stg_{dim_name}"
        b_key = config['business_key']
        s_key = config['surrogate_key']

        with self.connector.conn.cursor() as cursor:
            df.to_sql(staging_table, self.engine, if_exists='replace', index=False, method='multi')
            self.log_info(f"----> Staging table {staging_table} created with {len(df)} records")
            
            cols = ", ".join([f'"{col}"' for col in df.columns])
            update_cols = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col != b_key])

            sql_merge = f"""
                INSERT INTO {dim_name} ({cols})
                SELECT {cols} FROM {staging_table}
                ON CONFLICT ({b_key}) DO UPDATE
                SET {update_cols};
            """
            cursor.execute(sql_merge)
            business_keys = tuple[b_key].unique()
            cursor.execute(
                f"SELECT {s_key}, {b_key} FROM {dim_name} WHERE {b_key} IN %s",
                ((business_keys,))
            )
            key_mapping = dict(cursor.fetchall())

            cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
            self.log_info(f"----> Đã merge và lấy {len(key_mapping)} keys từ {dim_name}")
            self.log_info(f"----> Staging table {staging_table} dropped")
            return key_mapping
        
    def _load_scd2_dimension(self, dim_name: str, df: pd.DataFrame, config: Dict) -> Dict:
        staging_table = f"stg_{dim_name}"
        b_key = config['business_key']
        s_key = config['surrogate_key']

        scd_cols = {'valid_from_date', 'valid_to_date', 'is_current', s_key}
        compare_cols = [col for col in df.columns if col not in scd_cols and col != b_key]

        with self.connector.conn.cursor() as cursor:
            # 1. Tải vào staging
            df.to_sql(staging_table, self.engine, if_exists='replace', index=False)
            self.log_info(f"----> Staging table {staging_table} created with {len(df)} records")

            # 2. Expire old records
            diff_checks = " OR ".join([f'd"{col} <> s."{col}"' for col in compare_cols])

            sql_expire = f"""
                UPDATE {dim_name} d SET
                    valid_to_date = CURRENT_DATE - 1,
                    is_current = FALSE
                FROM {staging_table} s
                WHERE d.{b_key} = s.{b_key}
                  AND d.is_current = TRUE
                  AND ({diff_checks});
            """
            cursor.execute(sql_expire) 
            self.log_info(f"----> Expired old records in {dim_name} where changes detected")

            # 3. Insert new records or updated records
            cols = ", ".join([f'"{col}"' for col in df.columns])
            sql_insert_new = f"""
                INSERT INTO {dim_name} ({cols}, valid_from_date, valid_to_date, is_current)
                SELECT {cols}, CURRENT_DATE, '9999-12-31', TRUE
                FROM {staging_table} s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {dim_name} d
                    WHERE d.{b_key} = s.{b_key}
                      AND d.is_current = TRUE
                );
                """
            cursor.execute(sql_insert_new)
            self.log_info(f"----> Inserted {cursor.rowcount} new/update records into {dim_name}")

            # 4. Lấy surrogate keys cho các bản ghi hiện tại
            business_keys = tuple[df[b_key].unique()]
            sql_get_keys = f"""
                SELECT {s_key}, {b_key} FROM {dim_name}
                WHERE {b_key} IN %s AND is_current = TRUE;
            """
            cursor.execute(sql_get_keys, (business_keys,))
            key_mapping = dict(cursor.fetchall())

            # 5. Drop staging table
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
            self.log_info(f"----> Đã merge và lấy {len(key_mapping)} keys từ {dim_name}")
            self.log_info(f"----> Staging table {staging_table} dropped")
            return key_mapping
    
    def _transform_facts(self, facts: Dict[str, pd.DataFrame], all_dim_keys: Dict) -> Dict[str, pd.DataFrame]:
        """Thay thế business keys trong bảng Facts bằng surrogate keys từ Dimensions."""
        self.log_info("----> Starting Transform FACT Table <----")

        # ánh xạ : (cột nguồn, cột đích, bảng Dim)
        fact_key_map = {
            'fact_tramsaction': {
                ('transaction_date', 'transaction_date_key', 'dim_date'),
                ('customer_id_source', 'customer_key', 'dim_customer'),
                ('account_id_source', 'account_key', 'dim_account'),
                ('branch_id_source', 'branch_key', 'dim_branch'),
                ('card_id_source', 'card_key', 'dim_card')
            },
            'fact_loan_application': {
                ('application_date', 'application_date_key', 'dim_date'),  
                ('customer_id_source', 'customer_key', 'dim_customer'),
                ('loan_id_source', 'loan_key', 'dim_loan')
            },
            'fact_feedback': {
                ('feedback_date', 'feedback_date_key', 'dim_date'),
                ('resolution_date', 'resolution_date_key', 'dim_date'),
                ('customer_id_source', 'customer_key', 'dim_customer')
            }
        }

        transformed_facts = {}
        for fact_name, df in facts.items():
            if fact_name in fact_key_map:
                self.log_info(f"----> Transforming fact table {fact_name}")
                df_copy = df.copy()

                source_cols_to_drop = []
                for source_col, target_col, dim_table in fact_key_map.get(fact_name):
                    if dim_table not in all_dim_keys:
                        self.log_error(f"----> Dimension keys for {dim_table} not found, skipping key mapping for {fact_name}")
                        continue
                    if source_col not in df_copy.columns:
                        self.log_error(f"----> Source column {source_col} not found in {fact_name}, skipping this mapping")
                        continue

                    key_mapping = all_dim_keys[dim_table]

                    # Tạo cột mới với surrogate key
                    df_copy[target_col] = df_copy[source_col].map(key_mapping)
                    source_cols_to_drop.append(source_col)

                    null_keys = df_copy[target_col].isnull()
                    if null_keys.any():
                        missing_count = null_keys.sum()
                        self.log_error(f"----> Warning: {missing_count} records in {fact_name} have no matching key in {dim_table} for source column {source_col}")
                
                # Loại bỏ các cột nguồn đã được thay thế
                df_copy.drop(columns=list(set(source_cols_to_drop)), inplace=True)
                transformed_facts[fact_name] = df_copy
            else:
                self.log_info(f"----> No key mapping defined for fact table {fact_name}, skipping transformation")
                transformed_facts[fact_name] = df
        self.log_info("----> Transform FACT Table Completed <----")
        return transformed_facts
    
    def _load_facts(self, facts: Dict[str, pd.DataFrame]):
        self.log_info("----> BẮT ĐẦU TẢI FACTS <----")
        
        for fact_name, df in facts.items():
            if df.empty:
                self.log_info(f"----> Fact table {fact_name} is empty, skipping load.")
                continue

            self.log_info(f"----> Loading fact table {fact_name} with {len(df)} records")
            with self.connector.conn.cursor() as cursor:
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{fact_name}';")
                db_columns = {row[0] for row in cursor.fetchall()}

            df_cols_to_load = [col for col in df.columns if col in db_columns]

            df[df_cols_to_load].to_sql(fact_name, self.engine, if_exists='append', index=False, method='multi')
            self.log_info(f"----> Loaded {len(df)} records into {fact_name}")
        self.log_info("----> TẢI FACTS HOÀN TẤT <----")

    def close(self):
        if self.connector:
            self.connector.close()
            self.log_info("----> Postgres connection closed")