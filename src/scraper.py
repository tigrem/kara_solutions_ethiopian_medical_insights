
import os
import json
import asyncio
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from datetime import datetime
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")

TELEGRAM_CHANNELS = [
    'https://t.me/chemed_chem', # Example, replace with actual
    'https://t.me/lobelia4cosmetics',
    'https://t.me/tikvahpharma',
    # Add more channels from https://et.tgstat.com/medicine as needed
]

RAW_DATA_PATH = os.path.join(os.getcwd(), 'data', 'raw', 'telegram_messages')
IMAGE_DATA_PATH = os.path.join(os.getcwd(), 'data', 'raw', 'telegram_images')

os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(IMAGE_DATA_PATH, exist_ok=True)

async def initialize_telegram_client():
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        logging.error("Telegram API ID or Hash not found in .env. Please set them.")
        return None
    try:
        client = TelegramClient('telegram_scraper_session', int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            logging.info("Authorizing Telegram client...")
            # This will prompt for phone number and code on first run if not authorized
            await client.start()
        logging.info("Telegram client initialized and authorized.")
        return client
    except Exception as e:
        logging.error(f"Error initializing Telegram client: {e}")
        return None

async def scrape_channel(client, channel_url):
    logging.info(f"Starting scraping for channel: {channel_url}")
    try:
        entity = await client.get_entity(channel_url)
        channel_name = entity.username or entity.title.replace(" ", "_").replace("/", "_") # Sanitize channel name

        channel_message_path = os.path.join(RAW_DATA_PATH, datetime.now().strftime('%Y-%m-%d'), channel_name)
        os.makedirs(channel_message_path, exist_ok=True)

        channel_image_path = os.path.join(IMAGE_DATA_PATH, datetime.now().strftime('%Y-%m-%d'), channel_name)
        os.makedirs(channel_image_path, exist_ok=True)

        message_count = 0
        image_count = 0

        async for message in client.iter_messages(entity, limit=None):
            message_dict = {
                'id': message.id,
                'date': message.date.isoformat(),
                'message': message.message,
                'views': message.views,
                'channel_id': entity.id,
                'channel_name': channel_name,
                'has_media': message.media is not None,
                'media_type': None,
                'file_name': None,
                'file_path': None
            }

            if message.media:
                if isinstance(message.media, MessageMediaPhoto):
                    message_dict['media_type'] = 'photo'
                    try:
                        file_name = f"message_{message.id}_photo.jpg"
                        file_path = os.path.join(channel_image_path, file_name)
                        await client.download_media(message, file=file_path)
                        message_dict['file_name'] = file_name
                        message_dict['file_path'] = file_path # Store path relative to project root
                        image_count += 1
                    except Exception as e:
                        logging.warning(f"Could not download photo for message {message.id} in {channel_name}: {e}")
                elif isinstance(message.media, MessageMediaDocument) and message.media.document.mime_type.startswith('image/'):
                    message_dict['media_type'] = 'document_image'
                    try:
                        file_ext = message.media.document.mime_type.split('/')[-1]
                        file_name = f"message_{message.id}_doc_image.{file_ext}"
                        file_path = os.path.join(channel_image_path, file_name)
                        await client.download_media(message, file=file_path)
                        message_dict['file_name'] = file_name
                        message_dict['file_path'] = file_path
                        image_count += 1
                    except Exception as e:
                        logging.warning(f"Could not download document image for message {message.id} in {channel_name}: {e}")
                else:
                    message_dict['media_type'] = 'other_media'

            message_file_path = os.path.join(channel_message_path, f"{message.id}.json")
            with open(message_file_path, 'w', encoding='utf-8') as f:
                json.dump(message_dict, f, ensure_ascii=False, indent=4)
            message_count += 1

        logging.info(f"Finished scraping {message_count} messages and {image_count} images from {channel_name}")

    except Exception as e:
        logging.error(f"Error scraping channel {channel_url}: {e}")

async def main():
    client = await initialize_telegram_client()
    if not client:
        return

    for channel_url in TELEGRAM_CHANNELS:
        await scrape_channel(client, channel_url)

    await client.disconnect()
    logging.info("Scraping process completed.")

if __name__ == '__main__':
    asyncio.run(main())

