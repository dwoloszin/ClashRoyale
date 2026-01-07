import os
import re
import requests
import pandas as pd
import time
from collections import defaultdict
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API token
API_TOKEN = os.getenv("API_TOKEN")
CLAN_TAG = os.getenv("CLAN_TAG")
USER_TAG = os.getenv("USER_TAG")


def save_tables_to_csv(
    tables: dict,
    output_dir: str = "download",
    index: bool = False
):
    """
    Save a dictionary of DataFrames to CSV files.
    One file per table.
    """

    os.makedirs(output_dir, exist_ok=True)

    for table_name, df in tables.items():
        if df.empty:
            print(f"Skipping empty table: {table_name}")
            continue

        # Make filename filesystem-safe
        safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", table_name)
        file_path = os.path.join(output_dir, f"{safe_name}.csv")

        df.to_csv(file_path, index=index)
        print(f"Saved: {file_path}")


def _flatten_json(
    obj: Any,
    parent_key: str = "root",
    out: Dict[str, list] = None
):
    """
    Recursively flatten JSON into table-like structures.
    Each list of dicts becomes its own table.
    """
    if out is None:
        out = defaultdict(list)

    if isinstance(obj, dict):
        row = {}
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                _flatten_json(v, f"{parent_key}.{k}", out)
            else:
                row[k] = v

        if row:
            out[parent_key].append(row)

    elif isinstance(obj, list):
        for item in obj:
            _flatten_json(item, parent_key, out)

    return out


def fetch_clan_data_auto_tables(
    clan_tag: str,
    api_token: str,
    max_retries: int = 5,
    backoff_factor: float = 1.5,
    timeout: int = 30
) -> Dict[str, pd.DataFrame]:
    """
    Fetch Clash Royale clan data and auto-split all nested
    JSON objects into separate DataFrames.

    Returns:
        Dict[str, pd.DataFrame]
    """

    clan_tag_clean = clan_tag.replace('#', '').upper()
    user_tag_clean = USER_TAG.replace('#', '').upper()
    url = f"https://api.clashroyale.com/v1/clans/%23{clan_tag_clean}"
    #url = f"https://api.clashroyale.com/v1/clans/%23{clan_tag_clean}/riverracelog"
    #url = f"https://api.clashroyale.com/v1/clans/%23{clan_tag_clean}/currentriverrace"
    #url = f"https://api.clashroyale.com/v1/players/%23{user_tag_clean}"

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json"
    }

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[Attempt {attempt}] Fetching clan #{clan_tag_clean}")
            response = requests.get(url, headers=headers, timeout=timeout)

            # ---------- Rate limit handling ----------
            if response.status_code == 429:
                wait = backoff_factor ** attempt
                print(f"Rate limited (429). Sleeping {wait:.1f}s")
                time.sleep(wait)
                continue

            # ---------- Retry on server errors ----------
            if response.status_code >= 500:
                wait = backoff_factor ** attempt
                print(f"Server error {response.status_code}. Retrying in {wait:.1f}s")
                time.sleep(wait)
                continue

            # ---------- Hard fail ----------
            if response.status_code != 200:
                raise RuntimeError(
                    f"HTTP {response.status_code}: {response.text[:300]}"
                )

            data = response.json()

            # ---------- Auto split ----------
            tables = _flatten_json(data)

            dfs = {}
            for table_name, rows in tables.items():
                dfs[table_name] = pd.DataFrame(rows)

            return dfs

        except requests.exceptions.RequestException as e:
            wait = backoff_factor ** attempt
            print(f"Network error: {e}. Retrying in {wait:.1f}s")
            time.sleep(wait)

    raise RuntimeError("Max retries exceeded while fetching clan data")


def getnewData():
    tables = fetch_clan_data_auto_tables(CLAN_TAG, API_TOKEN)
    print(tables.keys())
    for k in tables.keys():
        df = tables[k]
        print(df)

    # ---- Save all tables ----
    save_tables_to_csv(
        tables,
        output_dir="download",
        index=False
    )


if __name__ == "__main__":
    getnewData()
