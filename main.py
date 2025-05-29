import os
import asyncio
import requests
import psycopg2
from tqdm import tqdm
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from dateutil import parser

load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_KEY")
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

SP500_SYMBOLS = ["GOOGL"]  # При необхідності додати інші компанії

TIMEFRAMES = {
    "daily": ("TIME_SERIES_DAILY", "sp500_daily", None, timedelta(days=730), 86400),
    "hourly": ("TIME_SERIES_INTRADAY", "sp500_hourly", "60min", timedelta(days=20), 3600),
    "5min": ("TIME_SERIES_INTRADAY", "sp500_minute", "5min", timedelta(days=2), 300)
}

def execute_ddl_from_file(cursor, file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        ddl_sql = f.read()
        cursor.execute(ddl_sql)

def fetch_data(symbol: str, tf: str):
    func, _, interval, _, _ = TIMEFRAMES[tf]
    params = {
        "function": func,
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY,
        "outputsize": "full",
        "datatype": "json"
    }

    if interval and func != "TIME_SERIES_DAILY":
        params["interval"] = interval

    url = "https://www.alphavantage.co/query"
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def parse_and_store(symbol: str, tf: str, data: dict, cursor):
    table = TIMEFRAMES[tf][1]
    max_age = TIMEFRAMES[tf][3]
    now = datetime.now(timezone.utc)

    if "Note" in data:
        print(f"    Перевищено ліміт: {data['Note']}")
        return "limit"
    if "Information" in data:
        print(f"    Перевищено ліміт / інформація: {data['Information']}")
        return "limit"
    if "Error Message" in data:
        print(f"    Неправильний запит: {data['Error Message']}")
        return "error"

    key = next((k for k in data if "Time Series" in k), None)
    if not key:
        print(f"    Немає даних для {symbol} ({tf})")
        return "empty"

    for timestamp_str, values in data[key].items():
        try:
            ts = parser.isoparse(timestamp_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = ts.astimezone(timezone.utc)
        except Exception as e:
            print(f"    Помилка парсингу дати {timestamp_str}: {e}")
            continue

        if now - ts > max_age:
            continue

        open_ = values["1. open"]
        high = values["2. high"]
        low = values["3. low"]
        close = values["4. close"]
        volume = values.get("6. volume") or values.get("5. volume") or 0

        try:
            cursor.execute(
                f"""
                INSERT INTO sp_500.{table} (symbol, timestamp, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (symbol, ts, open_, high, low, close, volume)
            )
        except Exception as e:
            print(f"    INSERT помилка: {e} для {symbol} @ {ts}")

    return "ok"

async def process_tf(tf: str):
    while True:
        start_time = datetime.now(timezone.utc).isoformat(timespec='seconds')
        print(f"\n  Оновлення таймфрейму: {tf} — Початок: {start_time} UTC")
        start = datetime.now(timezone.utc)
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            cursor = conn.cursor()
            for symbol in tqdm(SP500_SYMBOLS):
                try:
                    data = fetch_data(symbol, tf)
                    result = parse_and_store(symbol, tf, data, cursor)
                    if result == "limit":
                        print(" Ліміт API перевищено, пропускаємо цей цикл.")
                    conn.commit()
                except Exception as e:
                    print(f"    Помилка обробки {symbol}: {e}")
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"    DB помилка: {e}")
        end = datetime.now(timezone.utc)
        duration = (end - start).total_seconds()
        print(f"    {tf} завершено за {duration:.2f} сек")
        await asyncio.sleep(TIMEFRAMES[tf][4])

async def main():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    execute_ddl_from_file(cursor, os.path.join(os.path.dirname(__file__), "ddl.sql"))
    conn.commit()
    cursor.close()
    conn.close()

    await asyncio.gather(*(process_tf(tf) for tf in TIMEFRAMES))

if __name__ == "__main__":
    asyncio.run(main())
