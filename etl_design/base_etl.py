from abc import ABC, abstractmethod
from typing import Any, Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseETL(ABC):
    """Base class for all ETL components"""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """Execute the ETL component"""
        pass
    
    def log_info(self, message: str):
        self.logger.info(f"[{self.name}] {message}")
    
    def log_error(self, message: str):
        self.logger.error(f"[{self.name}] {message}")

    def log_warning(self, message: str):
        self.logger.warning(f"[{self.name}] {message}")