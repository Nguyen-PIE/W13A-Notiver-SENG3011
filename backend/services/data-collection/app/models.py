from pydantic import BaseModel, ConfigDict, Field
from typing import List, Optional

import config as config

class DataTimeObject(BaseModel):
    date_start: str
    date_end: str
    timezone: str = config.DEFAULT_TIMEZONE

class Event(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    
    lga: str = Field(alias=config.RAW_LGA_COL)
    offence_type: str = Field(alias=config.RAW_OFFENCE_COL)
    offence_count: int = Field(alias='count')
    rate_per_100k: Optional[float] = Field(default=None, alias=config.RAW_RATE_COL)
    ten_year_trend: Optional[str] = None
    ten_year_percent_change: Optional[float] = None
    time_object: DataTimeObject

class CrimeDataExport(BaseModel):
    data_source: str
    data_type: str
    version: str
    dataset_id: Optional[int] = None
    time_object: dict
    events: List[Event]