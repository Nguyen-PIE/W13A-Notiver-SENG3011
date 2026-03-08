from pydantic import BaseModel
from typing import List, Optional

# This represents one row in your Excel (20 columns)
class Event(BaseModel):
    date_start: str
    date_end: str
    lga: str
    offence_type: str
    rate_per_100k: float
    lga_rank: Optional[int] = None
    ten_year_trend: Optional[str] = None
    ten_year_percent_change: Optional[float] = None

# This represents the final JSON file structure
class CrimeDataExport(BaseModel):
    data_source: str
    data_type: str
    version: str
    collection_time: str
    events: List[Event]