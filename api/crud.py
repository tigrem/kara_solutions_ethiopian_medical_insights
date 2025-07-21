# api/crud.py
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, String, Date as SqlDate, or_, desc
from . import models, schemas
from datetime import date, datetime
from typing import List, Optional

# --- Helper to get channel_sk from channel_name ---
def get_channel_sk_by_name(db: Session, channel_name: str) -> Optional[str]:
    channel = db.query(models.DimChannel).filter(models.DimChannel.channel_name == channel_name).first()
    return channel.channel_sk if channel else None

# --- Analytical Endpoints Query Functions ---

def get_top_products(db: Session, limit: int = 10) -> List[schemas.TopProduct]:
    """
    Returns the most frequently mentioned product keywords.
    NOTE: This requires defining what constitutes a 'product' keyword.
    For simplicity, we'll search for a predefined list of common medical terms/products.
    In a real scenario, this might involve more sophisticated text analysis or a dedicated
    product dimension table populated by dbt.
    """
    # Define a list of common product/medical keywords you want to track
    # You would typically get this from a configuration or a separate source
    product_keywords = [
        "paracetamol", "amoxicillin", "ibuprofen", "antibiotics", "malaria",
        "fever", "cough", "cold", "pain", "vaccine", "covid", "cholera",
        "diabetes", "hypertension", "hiv", "tuberculosis", "mask", "sanitizer"
    ]

    # Use a subquery or CTE to count occurrences of each keyword
    # This is a simplified approach; full-text search would be more robust for large scale
    results = []
    for keyword in product_keywords:
        count = db.query(models.FctMessage) \
            .filter(models.FctMessage.message_text.ilike(f"%{keyword}%")) \
            .count()
        if count > 0:
            results.append({"product_keyword": keyword, "occurrence_count": count})

    # Sort by count and take the top N
    sorted_results = sorted(results, key=lambda x: x["occurrence_count"], reverse=True)
    return [schemas.TopProduct(**item) for item in sorted_results[:limit]]


def get_channel_activity(db: Session, channel_name: str) -> List[schemas.ChannelActivity]:
    """
    Returns the posting activity for a specific channel by date.
    """
    channel_sk = get_channel_sk_by_name(db, channel_name)
    if not channel_sk:
        return [] # Channel not found

    activity = db.query(
        models.DimDate.full_date.label("activity_date"),
        func.count(models.FctMessage.message_id).label("message_count"),
        func.sum(models.FctMessage.views_count).label("total_views")
    ).join(
        models.FctMessage,
        models.DimDate.date_key == models.FctMessage.message_date_fk
    ).filter(
        models.FctMessage.channel_fk == channel_sk
    ).group_by(
        models.DimDate.full_date
    ).order_by(
        models.DimDate.full_date
    ).all()

    return [schemas.ChannelActivity(activity_date=row.activity_date,
                                    message_count=row.message_count,
                                    total_views=row.total_views) for row in activity]


def search_messages(db: Session, query: str, limit: int = 100) -> List[schemas.Message]:
    """
    Searches for messages containing a specific keyword.
    """
    # Using ILIKE for case-insensitive substring search
    messages = db.query(models.FctMessage).filter(
        models.FctMessage.message_text.ilike(f"%{query}%")
    ).order_by(
        models.FctMessage.message_timestamp.desc()
    ).limit(limit).all()

    return [schemas.Message.from_orm(msg) for msg in messages]