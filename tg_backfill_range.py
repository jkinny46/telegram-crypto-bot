import asyncio
import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Dict, Tuple

from telethon import TelegramClient
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound, APIError
import google.generativeai as genai

# =========================
# LOGGING
# =========================
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), "tg_backfill.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# ==========
# CONFIG (copy the same values you use in tg_to_sheets_local.py)
# ==========

SERVICE_ACCOUNT_FILE = '/Users/willkannegieser/telegram-crypto-bot/service_account.json' # <-- Path to your JSON key
SHEET_ID = '1qvVYEdafNNI-BN6GHK6c40gKg7xqvDeyv3kszoPrR-s'
WORKSHEET_NAME = 'Telegram_Fundraising'

API_ID = 29116869
API_HASH = 'd1a88feacf4bb8abc8d4d27f864da2c6'
SESSION_NAME = 'my_session'
CHANNEL_USERNAME = '@crypto_fundraising'
GOOGLE_API_KEY = 'AIzaSyDRhaxdcPcLP4hR9XFErY7DLc42pNOYfxQ'
GEMINI_MODEL_NAME = 'gemini-1.5-flash'

EXPECTED_HEADERS = [
    'message_id', 'timestamp', 'text', 'Cleaned Text',
    'Company', 'Company Description', 'Amount Raised',
    'Round Type', 'Lead Investors', 'Other Investors'
]
# ---- Tuning knobs ----
PARSE_BATCH_SIZE = 10            # how many Telegram messages per Gemini call
GEMINI_RPM_LIMIT = 12            # keep below free-tier 15 RPM
GEMINI_MAX_ATTEMPTS = 6          # retry attempts per LLM call
SHEETS_BATCH_APPEND_SIZE = 20    # rows per Sheets append_rows
SHEETS_BACKOFF_BASE = 1.5        # seconds
MESSAGE_HARD_CAP = None          # None for unlimited; or int during testing

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

# ----------------
# Rate limiter
# ----------------
class RateLimiter:
    """Simple async rate limiter for 'calls per minute'."""
    def __init__(self, rpm: int):
        self.rpm = max(1, rpm)
        self._timestamps: List[float] = []
        self._lock = asyncio.Lock()

    async def wait(self):
        async with self._lock:
            import time
            now = time.monotonic()
            # drop entries older than 60s
            self._timestamps = [t for t in self._timestamps if now - t < 60.0]
            if len(self._timestamps) >= self.rpm:
                # sleep until oldest drops out of the window
                to_sleep = 60.0 - (now - self._timestamps[0])
                if to_sleep > 0:
                    await asyncio.sleep(to_sleep)
                # after sleeping, clean again
                now = time.monotonic()
                self._timestamps = [t for t in self._timestamps if now - t < 60.0]
            self._timestamps.append(time.monotonic())

gemini_limiter = RateLimiter(GEMINI_RPM_LIMIT)

# ----------------
# Helpers
# ----------------
async def generate_with_backoff(prompt: str) -> str:
    """Call Gemini with RPM throttling + exponential backoff honoring retry_delay."""
    await gemini_limiter.wait()  # enforce per-minute cap

    attempt = 0
    while True:
        try:
            resp = model.generate_content(prompt)
            return resp.text or ""
        except Exception as e:
            # Try to honor server-suggested retry delay if present
            attempt += 1
            retry_seconds = 15.0
            msg = str(e)
            # crude parse for 'retry_delay { seconds: N }'
            if "retry_delay" in msg and "seconds" in msg:
                try:
                    tokens = msg.split()
                    if "seconds" in tokens:
                        idx = tokens.index("seconds")
                        if idx + 1 < len(tokens):
                            s = tokens[idx + 1].strip("{}:,")
                            if s.replace('.', '', 1).isdigit():
                                retry_seconds = float(s)
                except Exception:
                    pass
            delay = min(retry_seconds * (2 ** (attempt - 1)), 120.0)
            if attempt >= GEMINI_MAX_ATTEMPTS:
                logging.error(f"Gemini retries exhausted: {e}")
                raise
            logging.warning(f"Gemini error (attempt {attempt}/{GEMINI_MAX_ATTEMPTS}): {e} | sleeping {delay:.1f}s")
            await asyncio.sleep(delay)

async def parse_batch(messages: List[Tuple[int, str]]) -> Dict[int, dict]:
    """
    messages: list of (message_id, text)
    returns: {message_id: parsed_dict}
    """
    items = [{"id": mid, "text": txt[:6000]} for (mid, txt) in messages]  # keep prompt size sane
    prompt = (
        "You are a JSON parser. For each item, extract:\n"
        "company_name, company_description, funding_amount_usd, funding_round_type, "
        "lead_investor, other_investors.\n"
        "Return ONLY a JSON array where each element is "
        "{\"id\": <id>, \"parsed\": {\"company_name\": ..., \"company_description\": ..., "
        "\"funding_amount_usd\": ..., \"funding_round_type\": ..., "
        "\"lead_investor\": ..., \"other_investors\": ...}}.\n"
        f"Items:\n{json.dumps(items)}"
    )

    text = await generate_with_backoff(prompt)
    try:
        clean = text.strip().replace('```json', '').replace('```', '').strip()
        arr = json.loads(clean)
        out = {}
        for elt in arr if isinstance(arr, list) else []:
            mid = elt.get("id")
            parsed = elt.get("parsed")
            if isinstance(mid, int) and isinstance(parsed, dict):
                out[mid] = parsed
        return out
    except Exception as e:
        logging.warning(f"Batch parse JSON error: {e} | raw: {text[:400]}")
        return {}

async def append_with_backoff(ws, rows: List[List], value_input_option: str = 'USER_ENTERED',
                              max_attempts: int = 5, base_delay: float = SHEETS_BACKOFF_BASE):
    for attempt in range(max_attempts):
        try:
            ws.append_rows(rows, value_input_option=value_input_option)
            return
        except APIError as e:
            delay = min(base_delay * (2 ** attempt), 30.0)
            logging.warning(f"append_rows failed (attempt {attempt+1}/{max_attempts}): {e} | sleeping {delay:.1f}s")
            await asyncio.sleep(delay)
        except Exception as e:
            delay = min(base_delay * (2 ** attempt), 30.0)
            logging.warning(f"append_rows unexpected error (attempt {attempt+1}/{max_attempts}): {e} | sleeping {delay:.1f}s")
            await asyncio.sleep(delay)
    raise RuntimeError("append_rows failed after retries")

# ----------------
# Backfill
# ----------------
async def backfill_range(from_date: datetime, to_date: datetime):
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()

    # Open or create worksheet
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=20)
        ws.append_row(EXPECTED_HEADERS, value_input_option='RAW')

    # Ensure headers correct
    header = ws.row_values(1)
    if header != EXPECTED_HEADERS:
        ws.update('A1', [EXPECTED_HEADERS])

    # Existing IDs to avoid dupes
    existing_ids = set()
    try:
        col = ws.col_values(1)[1:]  # skip header
        existing_ids = {int(x) for x in col if x.strip().isdigit()}
    except Exception as e:
        logging.info(f"Couldn't read existing IDs: {e}")

    logging.info(f"Starting backfill {from_date.isoformat()} → {to_date.isoformat()}")

    # Collect candidate messages first (date-filtered, deduped) BEFORE touching Gemini
    candidates: List[Tuple[int, str, str]] = []  # (id, iso_ts, text)
    async for message in client.iter_messages(
        CHANNEL_USERNAME,
        reverse=True,
        offset_date=from_date,
        limit=None
    ):
        if not message or not getattr(message, "text", None):
            continue
        msg_dt = message.date.replace(tzinfo=timezone.utc)
        if msg_dt < from_date:
            continue
        if msg_dt > to_date:
            break  # reverse=True => we're past the window
        if message.id in existing_ids:
            continue

        candidates.append((message.id, msg_dt.isoformat(), message.text))
        if MESSAGE_HARD_CAP and len(candidates) >= MESSAGE_HARD_CAP:
            break

    logging.info(f"Found {len(candidates)} messages to process after filtering")

    # Process in batches through Gemini and then append to Sheets in batches
    total_appended = 0
    append_batch: List[List] = []

    # walk candidates in chunks for LLM
    for i in range(0, len(candidates), PARSE_BATCH_SIZE):
        chunk = candidates[i:i+PARSE_BATCH_SIZE]
        ids_and_texts = [(mid, txt.strip()) for (mid, _ts, txt) in chunk if txt and txt.strip()]

        if not ids_and_texts:
            continue

        parsed_map = await parse_batch(ids_and_texts)

        # Build rows for those that parsed
        for mid, ts, txt in chunk:
            parsed = parsed_map.get(mid)
            if not parsed:
                continue
            row = [
                mid,
                ts,
                txt,
                txt.strip(),
                parsed.get('company_name'),
                parsed.get('company_description'),
                parsed.get('funding_amount_usd'),
                parsed.get('funding_round_type'),
                parsed.get('lead_investor'),
                ', '.join(parsed.get('other_investors', []))
                    if isinstance(parsed.get('other_investors'), list)
                    else parsed.get('other_investors'),
            ]
            append_batch.append(row)

            if len(append_batch) >= SHEETS_BATCH_APPEND_SIZE:
                await append_with_backoff(ws, append_batch, value_input_option='USER_ENTERED')
                total_appended += len(append_batch)
                append_batch.clear()
                logging.info(f"Appended {total_appended} rows so far; throttling…")
                await asyncio.sleep(2.0)  # be kind to Sheets

    # flush remaining rows
    if append_batch:
        await append_with_backoff(ws, append_batch, value_input_option='USER_ENTERED')
        total_appended += len(append_batch)

    await client.disconnect()
    logging.info(f"✅ Backfill complete. Added {total_appended} new rows.")
    print(f"✅ Done. Appended {total_appended} new rows.")

# -----------
# CLI
# -----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="from_date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    from_dt = datetime.strptime(args.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    to_dt = datetime.strptime(args.to_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)

    asyncio.run(backfill_range(from_dt, to_dt))