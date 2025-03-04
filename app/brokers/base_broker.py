from abc import ABC, abstractmethod
from app.models import trade

class BaseBroker(ABC):
    
    @abstractmethod
    def get_data(self,email) -> list[trade.Trade]:
        pass