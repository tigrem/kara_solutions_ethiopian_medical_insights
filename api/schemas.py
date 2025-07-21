# api/schemas.py
from datetime import date, datetime
from typing import Optional, List
from pydantic import BaseModel, Field

# --- Base Schemas (for shared attributes) ---
class ChannelBase(BaseModel):
    telegram_channel_id: int
    channel_name: str

class DateBase(BaseModel):
    full_date: date
    day_of_week: Optional[int] = None
    day_name: Optional[str] = None
    day_of_month: Optional[int] = None
    day_of_year: Optional[int] = None
    week_of_year: Optional[int] = None
    month: Optional[int] = None
    month_name: Optional[str] = None
    quarter: Optional[int] = None
    year: Optional[int] = None

class MessageBase(BaseModel):
    message_id: str
    message_length: Optional[int] = None
    has_image: Optional[bool] = None
    views_count: Optional[int] = None
    message_timestamp: Optional[datetime] = None
    message_text: Optional[str] = None

class ImageDetectionBase(BaseModel):
    message_id: str
    detected_object_class: str
    confidence_score: float # Pydantic will convert Numeric to float
    detection_timestamp: datetime

# --- Response Schemas (for returning data) ---
class Channel(ChannelBase):
    channel_sk: str # Surrogate key for internal use
    class Config:
        orm_mode = True # Enables ORM mode for Pydantic

class Date(DateBase):
    date_key: int
    class Config:
        orm_mode = True

class Message(MessageBase):
    channel_fk: Optional[str] = None
    message_date_fk: Optional[int] = None
    message_scraped_date_fk: Optional[int] = None
    # You can embed related models if desired, but for simplicity, we'll keep FKs
    class Config:
        orm_mode = True

class ImageDetection(ImageDetectionBase):
    detection_id: str
    class Config:
        orm_mode = True

# --- Analytical Endpoint Response Schemas ---

# GET /api/reports/top-products?limit=10
class TopProduct(BaseModel):
    product_keyword: str
    occurrence_count: int

# GET /api/channels/{channel_name}/activity
class ChannelActivity(BaseModel):
    activity_date: date
    message_count: int
    total_views: Optional[int] = None

# GET /api/search/messages?query=paracetamol
# The Message schema can be directly used here for individual messages.
# For a list of messages:
class MessageSearchResults(BaseModel):
    query: str
    count: int
    results: List[Message]