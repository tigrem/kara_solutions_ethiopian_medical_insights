# api/main.py
from typing import List, Optional
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
import uvicorn

from . import models, schemas, crud
from .database import engine, Base, get_db

# Create all tables (if they don't exist yet, based on SQLAlchemy models)
# This is useful for initial setup, but in production, dbt handles table creation.
# Base.metadata.create_all(bind=engine) # Comment out if you only want dbt to create tables

app = FastAPI(
    title="Ethiopian Medical Insights API",
    description="API for analytical insights from Telegram medical messages and YOLO detections.",
    version="1.0.0",
)

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Welcome to the Ethiopian Medical Insights API! Access docs at /docs"}

@app.get(
    "/api/reports/top-products",
    response_model=List[schemas.TopProduct],
    summary="Get the most frequently mentioned products/keywords",
    description="Returns a list of top medical products or keywords based on message content."
)
def get_top_products_endpoint(
        limit: int = Query(10, ge=1, le=100),
        db: Session = Depends(get_db)
):
    return crud.get_top_products(db, limit=limit)

@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=List[schemas.ChannelActivity],
    summary="Get posting activity for a specific channel",
    description="Returns daily message counts and total views for a given Telegram channel."
)
def get_channel_activity_endpoint(
        channel_name: str,
        db: Session = Depends(get_db)
):
    # Check if channel exists (optional, but good practice)
    channel = db.query(models.DimChannel).filter(models.DimChannel.channel_name == channel_name).first()
    if not channel:
        raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found.")
    return crud.get_channel_activity(db, channel_name=channel_name)

@app.get(
    "/api/search/messages",
    response_model=schemas.MessageSearchResults, # Use the wrapper schema for search results
    summary="Search for messages by keyword",
    description="Searches for messages containing a specified keyword and returns a list of matching messages."
)
def search_messages_endpoint(
        query: str = Query(..., min_length=2, description="Keyword to search in message content."),
        limit: int = Query(100, ge=1, le=500, description="Maximum number of messages to return."),
        db: Session = Depends(get_db)
):
    messages = crud.search_messages(db, query=query, limit=limit)
    return schemas.MessageSearchResults(query=query, count=len(messages), results=messages)

# You can add more endpoints here, e.g., for fct_image_detections
@app.get(
    "/api/detections/{message_id}",
    response_model=List[schemas.ImageDetection],
    summary="Get YOLO detections for a specific message",
    description="Returns object detections made by YOLO for a given message_id."
)
def get_detections_for_message(
        message_id: str,
        db: Session = Depends(get_db)
):
    detections = db.query(models.FctImageDetection).filter(models.FctImageDetection.message_id == message_id).all()
    if not detections:
        raise HTTPException(status_code=404, detail=f"No image detections found for message ID '{message_id}'.")
    return detections


# Main block to run the FastAPI app with Uvicorn
if __name__ == "__main__":
    # Ensure your environment variables are set before running this.
    # Example (for testing, not for production):
    # import os
    # os.environ["POSTGRES_HOST"] = "your_neon_host"
    # os.environ["POSTGRES_DB"] = "your_db_name"
    # os.environ["POSTGRES_USER"] = "your_user"
    # os.environ["POSTGRES_PASSWORD"] = "your_password"
    # os.environ["POSTGRES_PORT"] = "your_port"

    uvicorn.run(app, host="0.0.0.0", port=8000)