from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class RecoverCriteria:
    """
    Criteria for specifying recovery parameters in log recovery operations.

    Attributes:
        transaction_id(int): the id of the specific entry in log
        timestamp(datetime): the datetime of the specific entry in log
    """
    transaction_id: Optional[int] = None
    timestamp: Optional[datetime] = None
