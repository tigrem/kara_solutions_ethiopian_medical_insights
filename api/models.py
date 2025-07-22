# api/models.py
from sqlalchemy import Column, Integer, BigInteger, String, Text, Boolean, DateTime, Numeric, Date
from .database import Base

# Convention: dbt models are often in the 'public' schema by default
# Adjust schema name if your dbt project creates models in a different schema
DBT_SCHEMA = "public"

class DimChannel(Base):
    __tablename__ = "dim_channels"
    __table_args__ = {"schema": DBT_SCHEMA}

    channel_sk = Column(String, primary_key=True, index=True) # dbt generated string surrogate key
    telegram_channel_id = Column(BigInteger, unique=True, index=True)
    channel_name = Column(String, unique=True, index=True)

class DimDate(Base):
    __tablename__ = "dim_dates"
    __table_args__ = {"schema": DBT_SCHEMA}

    date_key = Column(Integer, primary_key=True, index=True)
    full_date = Column(Date, unique=True)
    day_of_week = Column(Integer)
    day_name = Column(String)
    day_of_month = Column(Integer)
    day_of_year = Column(Integer)
    week_of_year = Column(Integer)
    month = Column(Integer)
    month_name = Column(String)
    quarter = Column(Integer)
    year = Column(Integer)

class FctMessage(Base):
    __tablename__ = "fct_messages"
    __table_args__ = {"schema": DBT_SCHEMA}

    message_id = Column(String, primary_key=True, index=True) # Assuming message_id is unique enough as a PK
    channel_fk = Column(String) # Foreign key to dim_channels.channel_sk
    message_date_fk = Column(Integer) # Foreign key to dim_dates.date_key
    message_length = Column(Integer)
    has_image = Column(Boolean)
    views_count = Column(Integer)
    message_scraped_date_fk = Column(Integer) # Foreign key to dim_dates.date_key
    message_timestamp = Column(DateTime(timezone=True)) # Use DateTime(timezone=True) for TIMESTAMP WITH TIME ZONE
    message_text = Column(Text)

class FctImageDetection(Base):
    __tablename__ = "fct_image_detections"
    __table_args__ = {"schema": DBT_SCHEMA}

    # For fact tables without a natural single-column PK, consider a composite PK
    # or a synthetic one if not already created in dbt.
    # Assuming message_id + detected_object_class + detection_timestamp is unique for simplicity
    # If your dbt model has a primary key defined, use that.
    detection_id = Column(String, primary_key=True, index=True) # Assuming dbt generated a unique ID
    message_id = Column(String) # Foreign key to fct_messages.message_id
    detected_object_class = Column(String)
    confidence_score = Column(Numeric)
    detection_timestamp = Column(DateTime(timezone=True)) # Assuming this from dbt model