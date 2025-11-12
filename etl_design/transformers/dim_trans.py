from typing import Dict
import pandas as pd
from etl_design.base_etl import BaseETL
from datetime import datetime

class DimensionTransformers(BaseETL):
    """Transform data for dimension tables"""
    
    def __init__(self):
        super().__init__(DimensionTransformers)

    def execute(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Transform raw data into dimension tables
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Dict of dimension DataFrames
        """
        try:
            self.log_info(f"----> Tranforming dimensions")

            dimensions = {}

            dimensions['dim_customer'] = self._transform_customer(df)
            dimensions['dim_customer_pii'] = self._transform_customer_pii(df)
            dimensions['dim_branch'] = self._transform_branch(df)
            dimensions['dim_account'] = self._transform_account(df)
            dimensions['dim_card'] = self._transform_card(df)
            dimensions['dim_loan'] = self._transform_loan(df)
            dimensions['dim_date'] = self._transform_date(df)

            self.log_info(f"----> Dimension tranformation completed")
            return dimensions
        except Exception as e:
            self.log_error(f"----> Error tranforming dimensions: {e}")
            return None
    
    def _transform_customer(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform customer dimension"""
        customer_df = df[['Customer ID', 'Age', 'Gender', 'City']].copy()
        customer_df = customer_df.drop_duplicates(subset='Customer ID')

        # Caculate birth year
        current_year = datetime.now().year
        customer_df['birth_year'] = current_year - customer_df['Age']

        customer_df = customer_df.rename(columns={
            'Customer ID' : 'customer_id_source',
            "Gender"      : 'gender',
            "City"        : 'city'
        })

        customer_df['valid_from_date'] = datetime.now().date()
        customer_df['valid_to_date'] =  pd.to_datetime('9999-12-31').date()
        customer_df['is_current'] = True

        return customer_df[['customer_id_source', 'birth_year', 'gender', 'city', 'valid_from_date', 'valid_to_date', 'is_current']]
    
    def _transform_customer_pii(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform customer PII (will be linked via customer_key after insert)"""
        pii_df = df[['Customer ID', 'First Name', 'Last Name', 'Address', 'Contact Number', 'Email']].copy()
        pii_df = pii_df.drop_duplicates(subset='Customer ID')

        pii_df = pii_df.rename(columns={
            'Customer ID': 'customer_id_source',
            'First Name': 'first_name',
            'Last Name': 'last_name',
            'Address': 'address',
            'Contact Number': 'contact_number',
            'Email': 'email'
        })
        
        return pii_df
    
    def _transform_branch(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform branch dimension"""
        branch_df = df[['Branch ID']].copy()
        
        branch_df = branch_df.rename(columns={'Branch ID': 'branch_id_source'})
        branch_df['branch_name'] = 'Branch ' + branch_df['branch_id_source'].astype(str)
        branch_df['branch_location'] = 'Location ' + branch_df['branch_id_source'].astype(str)
        
        return branch_df

    def _transform_account(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform account dimension"""
        # Rebuild account_id from Customer ID
        account_df = df[['Customer ID', 'Account Type', 'Date Of Account Opening', 
                        'Last Transaction Date']].copy()
        account_df = account_df.drop_duplicates(subset=['Customer ID'])
        
        account_df['account_id_source'] = 'ACC_' + account_df['Customer ID'].astype(str)
        account_df['date_of_account_opening'] = pd.to_datetime(account_df['Date Of Account Opening'])
        account_df['last_transaction_date'] = pd.to_datetime(account_df['Last Transaction Date'])
        
        account_df = account_df.rename(columns={'Account Type': 'account_type'})
        
        account_df['valid_from_date'] = datetime.now().date()
        account_df['valid_to_date'] = pd.to_datetime('9999-12-31').date()
        account_df['is_current'] = True
        
        return account_df[['account_id_source', 'account_type', 'date_of_account_opening',
                          'last_transaction_date', 'valid_from_date', 'valid_to_date', 'is_current']]

    def _transform_card(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform card dimension"""
        card_df = df[['CardID', 'Card Type', 'Credit Limit', 'Rewards Points']].copy()
        card_df = card_df.drop_duplicates(subset=['CardID'])
        
        card_df = card_df.rename(columns={
            'CardID': 'card_id_source',
            'Card Type': 'card_type',
            'Credit Limit': 'credit_limit',
            'Rewards Points': 'rewards_points'
        })
        
        card_df['valid_from_date'] = datetime.now().date()
        card_df['valid_to_date'] = pd.to_datetime('9999-12-31').date()
        card_df['is_current'] = True
        
        return card_df
    
    def _transform_loan(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform loan dimension"""
        loan_df = df[['Loan ID', 'Loan Type', 'Loan Amount', 'Interest Rate', 
                     'Loan Term', 'Loan Status']].copy()
        loan_df = loan_df.drop_duplicates(subset=['Loan ID'])
        
        loan_df = loan_df.rename(columns={
            'Loan ID': 'loan_id_source',
            'Loan Type': 'loan_type',
            'Loan Amount': 'loan_amount',
            'Interest Rate': 'interest_rate',
            'Loan Term': 'loan_term',
            'Loan Status': 'current_loan_status'
        })
        
        loan_df['valid_from_date'] = datetime.now().date()
        loan_df['valid_to_date'] = pd.to_datetime('9999-12-31').date()
        loan_df['is_current'] = True
        
        return loan_df
    
    def _transform_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate date dimension from all date columns"""
        date_columns = ['Transaction Date', 'Date Of Account Opening', 
                       'Last Transaction Date', 'Approval/Rejection Date', 
                       'Feedback Date', 'Resolution Date']
        
        all_dates = []
        for col in date_columns:
            if col in df.columns:
                dates = pd.to_datetime(df[col], errors='coerce').dropna()
                all_dates.extend(dates.unique())
        
        all_dates = pd.Series(all_dates).unique()
        date_df = pd.DataFrame({'date_key': pd.to_datetime(all_dates)})
        date_df = date_df.sort_values('date_key').reset_index(drop=True)
        
        # Generate date attributes
        date_df['full_date_desc'] = date_df['date_key'].dt.strftime('%d-%m-%Y')
        date_df['day_of_week_num'] = date_df['date_key'].dt.dayofweek + 1
        date_df['day_of_week_name'] = date_df['date_key'].dt.day_name()
        date_df['day_of_month'] = date_df['date_key'].dt.day
        date_df['month_num'] = date_df['date_key'].dt.month
        date_df['month_name'] = 'Tháng ' + date_df['month_num'].astype(str)
        date_df['quarter_num'] = date_df['date_key'].dt.quarter
        date_df['year_num'] = date_df['date_key'].dt.year
        date_df['is_weekend'] = date_df['day_of_week_num'].isin([6, 7])
        date_df['is_holiday'] = False  # Can be enhanced with holiday calendar
        
        return date_df