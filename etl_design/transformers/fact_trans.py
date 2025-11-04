from typing import Dict
import pandas as pd
from etl_design.base_etl import BaseETL

class Fact_Transformer(BaseETL):
    """Transform data for fact tables"""

    def __init__(self):
        super().__init__(Fact_Transformer)
    
    def execute(self,df: pd.DataFrame, dimension_keys: Dict) -> Dict[str, pd.DataFrame]:
        """
        Transform raw data into fact tables
        
        Args:
            df: Raw DataFrame
            dimension_keys: Dictionary of dimension key mappings
            
        Returns:
            Dict of fact DataFrames
        """
        try:
            self.log_info(f"----> Transforming facts")
            
            facts = {}

            facts['fact_transaction'] = self._transform_transaction(df, dimension_keys)
            facts['fact_loan_application'] = self._transform_transaction(df, dimension_keys)
            facts['fact_feedback'] = self._transform_transaction(df, dimension_keys)
            facts['fact_account_snapshot'] = self._transform_transaction(df, dimension_keys)
            facts['fact_card_snapshot'] = self._transform_transaction(df, dimension_keys)

            self.log_info(f"----> Fact transformation completed")
            return facts
        except Exception as e:
            self.log_error(f"----> Error transformation facts: {e}")
            return None

    def _transform_transaction(self, df: pd.DataFrame, dim_keys: Dict) -> pd.DataFrame:
        """Transform transaction fact"""
        trans_df = df[['TransactionID', 'Transaction Date', 'Transaction Type', 
                       'Transaction Amount', 'Account Balance After Transaction',
                       'Customer ID', 'Branch ID', 'CardID', 'Anomaly']].copy()
        
        trans_df = trans_df.rename(columns={
            'TransactionID': 'transaction_id_source',
            'Transaction Date': 'date_key',
            'Transaction Type': 'transaction_type',
            'Transaction Amount': 'transaction_amount',
            'Account Balance After Transaction': 'acc_balancer_after_transaction',
            'Anomaly': 'anomaly_flag'
        })
        
        trans_df['date_key'] = pd.to_datetime(trans_df['date_key'])
        
        # Map to dimension keys (placeholder - will be done via JOIN in loader)
        trans_df['customer_id_source'] = df['Customer ID']
        trans_df['account_id_source'] = 'ACC_' + df['Customer ID'].astype(str)
        trans_df['branch_id_source'] = df['Branch ID']
        trans_df['card_id_source'] = df['CardID']
        
        return trans_df
    
    def _transform_loan_application(self, df: pd.DataFrame, dim_keys: Dict) -> pd.DataFrame:
        """Transform loan application fact"""
        loan_df = df[['Approval/Rejection Date', 'Customer ID', 'Loan ID', 
                      'Loan Status']].copy()
        
        loan_df = loan_df.rename(columns={
            'Approval/Rejection Date': 'application_date_key',
            'Loan Status': 'application_status'
        })
        
        loan_df['application_date_key'] = pd.to_datetime(loan_df['application_date_key'])
        loan_df['customer_id_source'] = df['Customer ID']
        loan_df['loan_id_source'] = df['Loan ID']
        
        return loan_df
    
    def _transform_feedback(self, df: pd.DataFrame, dim_keys: Dict) -> pd.DataFrame:
        """Transform feedback fact"""
        feedback_df = df[['Feedback ID', 'Feedback Date', 'Resolution Date',
                          'Customer ID', 'Feedback Type', 'Resolution Status']].copy()
        
        feedback_df = feedback_df.rename(columns={
            'Feedback ID': 'feedback_id',
            'Feedback Date': 'feedback_date_key',
            'Resolution Date': 'resolution_date_key',
            'Feedback Type': 'feedback_type',
            'Resolution Status': 'resolution_status'
        })
        
        feedback_df['feedback_date_key'] = pd.to_datetime(feedback_df['feedback_date_key'])
        feedback_df['resolution_date_key'] = pd.to_datetime(feedback_df['resolution_date_key'], errors='coerce')
        feedback_df['customer_id_source'] = df['Customer ID']
        
        return feedback_df
    
    def _transform_account_snapshot(self, df: pd.DataFrame, dim_keys: Dict) -> pd.DataFrame:
        """Transform account snapshot (daily balance)"""
        snapshot_df = df[['Last Transaction Date', 'Customer ID', 
                          'Account Balance']].copy()
        
        snapshot_df = snapshot_df.rename(columns={
            'Last Transaction Date': 'snapshot_date_key',
            'Account Balance': 'account_balance'
        })
        
        snapshot_df['snapshot_date_key'] = pd.to_datetime(snapshot_df['snapshot_date_key'])
        snapshot_df['customer_id_source'] = df['Customer ID']
        snapshot_df['account_id_source'] = 'ACC_' + df['Customer ID'].astype(str)
        
        return snapshot_df
    
    def _transform_card_snapshot(self, df: pd.DataFrame, dim_keys: Dict) -> pd.DataFrame:
        """Transform card snapshot"""
        card_snap_df = df[['Last Credit Card Payment Date', 'CardID', 'Customer ID',
                           'Credit Card Balance', 'Minimum Payment Due', 
                           'Payment Due Date']].copy()
        
        card_snap_df = card_snap_df.rename(columns={
            'Last Credit Card Payment Date': 'snapshot_date_key',
            'Credit Card Balance': 'credit_card_balance',
            'Minimum Payment Due': 'minimum_payment_due',
            'Payment Due Date': 'payment_due_date'
        })
        
        card_snap_df['snapshot_date_key'] = pd.to_datetime(card_snap_df['snapshot_date_key'])
        card_snap_df['payment_due_date'] = pd.to_datetime(card_snap_df['payment_due_date'])
        card_snap_df['customer_id_source'] = df['Customer ID']
        card_snap_df['card_id_source'] = df['CardID']
        
        return card_snap_df