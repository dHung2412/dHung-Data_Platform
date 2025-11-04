import pandas as pd
from  etl_design.base_etl import BaseETL

class CSV_Extractor(BaseETL):
    """Parse CSV file and return DataFrame"""

    def __init__(self):
        super().__init__(CSV_Extractor)
    
    def execute(self, file_path: str) -> pd.DataFrame:
        """
        Read CSV file and return DataFrame
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            pd.DataFrame: Parsed data
        """
        try:
            self.log_info(f"----> Reading CSV from {file_path}")

            df = pd.read_csv(file_path)
            
            self.log_info(f"----> Successfully read '{len(df)}' rows and '{len(df.columns)}' columns")
            self.log_info(f"----> Columns: {list(df.columns)}")

            return df
        except Exception as e:
            self.log_error(f"----> Error reading CSV: {e}")
            return None 