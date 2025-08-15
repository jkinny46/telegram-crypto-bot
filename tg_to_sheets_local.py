import asyncio
import json
import os
import sys
import logging
from datetime import timezone

from telethon import TelegramClient
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound
import google.generativeai as genai

# =========================
# LOGGING (writes tg_bot.log)
# =========================
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), "tg_bot.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


# === CONFIGURATION ===
SERVICE_ACCOUNT_FILE = '/Users/willkannegieser/telegram-crypto-bot/service_account.json' # <-- Path to your JSON key
SHEET_ID = '1blyRttLeDMh05rGHLAIbVh6rXMQ6LYLhXOVnQzVdv14'
WORKSHEET_NAME = 'Telegram_fundraising_update'

API_ID = 29116869
API_HASH = 'd1a88feacf4bb8abc8d4d27f864da2c6'
SESSION_NAME = 'my_session'
CHANNEL_USERNAME = '@crypto_fundraising'
GOOGLE_API_KEY = 'AIzaSyDRhaxdcPcLP4hR9XFErY7DLc42pNOYfxQ'
GEMINI_MODEL_NAME = 'gemini-1.5-flash'
EXPECTED_HEADERS = ['message_id', 'timestamp', 'text', 'Cleaned Text', 'Company', 'Company Description', 'Amount Raised', 'Round Type', 'Lead Investors', 'Other Investors']
MESSAGE_LIMIT = 50  # Adjust as needed

# ------------
# INIT CLIENTS
# ------------
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel(GEMINI_MODEL_NAME)

creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)

# -------------
# HELPERS
# -------------
def parse_message(input_text: str):
    prompt = f"""
    Parse the following text and extract the specified information into a valid JSON object.

    Input Text:
    "{input_text}"

    Extract these fields:
    - company_name
    - company_description
    - funding_amount_usd
    - funding_round_type
    - lead_investor
    - other_investors

    Output ONLY the JSON object.
    """
    response = model.generate_content(prompt)
    try:
        clean = response.text.strip().replace('```json', '').replace('```', '').strip()
        return json.loads(clean)
    except Exception as e:
        logging.info(f"Gemini parse error: {e}. Raw: {getattr(response, 'text', '')[:500]}")
        return None

async def status_check():
    """Print whether the sheet is caught up with the channel. No writes."""
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    await client.connect()

    latest = await client.get_messages(CHANNEL_USERNAME, limit=1)
    latest_id = latest[0].id if latest and latest[0] else 0

    ws = gc.open_by_key(SHEET_ID).worksheet(WORKSHEET_NAME)
    ids = [int(x) for x in ws.col_values(1)[1:] if x.strip().isdigit()]
    last_in_sheet = max(ids) if ids else 0

    print(f"Channel latest message_id: {latest_id}")
    print(f"Sheet last message_id:    {last_in_sheet}")
    if last_in_sheet >= latest_id:
        print("✅ Sheet is up-to-date.")
    else:
        print(f"⏳ Behind by {latest_id - last_in_sheet} messages.")

    await client.disconnect()

# -------------------------
# MAIN: scrape + append
# -------------------------
async def scrape_and_append():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    await client.connect()

    # Open or create the worksheet
    sh = gc.open_by_key(SHEET_ID)
    try:
        worksheet = sh.worksheet(WORKSHEET_NAME)
    except WorksheetNotFound:
        worksheet = sh.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=20)
        worksheet.update('A1', [EXPECTED_HEADERS])

    # Ensure headers are present/updated once
    header = worksheet.row_values(1)
    if header != EXPECTED_HEADERS:
        worksheet.update('A1', [EXPECTED_HEADERS])

    # ======= LOG START =======
    logging.info("START run")
    logging.info(f"Opened sheet {WORKSHEET_NAME}")

    # --- DEDUPE SETUP: read existing message_ids and compute last_id ---
    existing_ids = set()
    try:
        col = worksheet.col_values(1)[1:]  # Column A, skip header
        existing_ids = {int(x) for x in col if x.strip().isdigit()}
    except Exception as e:
        logging.info(f"Couldn't read existing IDs: {e}")

    last_id = max(existing_ids) if existing_ids else 0
    logging.info(f"Last stored message_id: {last_id or 'none'}")

    count = 0

    # Fetch only messages newer than what's already in the sheet,
    # and iterate oldest->newest so rows append chronologically.
    async for message in client.iter_messages(
            CHANNEL_USERNAME,
            min_id=last_id,      # <-- the magic: only ids > last_id
            reverse=True):       # oldest -> newest

        if not message or not message.text:
            continue
        if message.id in existing_ids:  # extra guard
            logging.info(f"Skipped message {message.id} — duplicate")
            continue

        logging.info(f"Processed message {message.id}")

        timestamp = message.date.replace(tzinfo=timezone.utc).isoformat()
        original_text = message.text
        cleaned_text = original_text.strip()

        parsed = parse_message(cleaned_text)
        if not parsed:
            # parsing failed; continue (already logged)
            continue

        row_data = [
            message.id,
            timestamp,
            original_text,
            cleaned_text,
            parsed.get('company_name'),
            parsed.get('company_description'),
            parsed.get('funding_amount_usd'),
            parsed.get('funding_round_type'),
            parsed.get('lead_investor'),
            ', '.join(parsed.get('other_investors', []))
                if isinstance(parsed.get('other_investors'), list)
                else parsed.get('other_investors'),
        ]

        try:
            worksheet.append_row(row_data, value_input_option='USER_ENTERED')
            count += 1
            logging.info("Appended 1 row")
        except Exception as e:
            logging.info(f"Append error for message {message.id}: {e}")

    await client.disconnect()
    logging.info(f"DONE. Appended {count} new rows")
    print(f"✅ Done. Appended {count} new rows.")

# -----------
# ENTRYPOINT
# -----------
if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--status':
        asyncio.run(status_check())
    else:
        asyncio.run(scrape_and_append())