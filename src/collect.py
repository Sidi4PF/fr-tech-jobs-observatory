from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_FILE = RAW_DIR / "offers_raw.parquet"

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
API_BASE = "https://api.francetravail.io/partenaire/offresdemploi/v2"


class FranceTravailCollector:
    SEARCH_QUERY = "Python SQL"

    def __init__(self, token: str, request_timeout: int = 30) -> None:
        self.token = token
        self.request_timeout = request_timeout
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "fr-tech-jobs-observatory/0.1",
        }

    def fetch_offers(
        self,
        query: str,
        max_pages: int = 20,
        page_size: int = 150,
    ) -> list[dict[str, Any]]:
        offers: list[dict[str, Any]] = []

        for start in range(0, max_pages * page_size, page_size):
            end = start + page_size - 1
            print(f"[collect] query='{query}' range={start}-{end}")

            try:
                response = requests.get(
                    f"{API_BASE}/offres/search",
                    headers=self.headers,
                    params={
                        "motsCles": query,
                        "range": f"{start}-{end}",
                    },
                    timeout=self.request_timeout,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                print(f"[error] Request failed for '{query}' ({start}-{end}): {exc}")
                break

            try:
                payload = response.json()
            except ValueError as exc:
                print(f"[error] Failed to parse JSON response: {exc}")
                print(f"[error] response body: {response.text}")
                break

            results = payload.get("resultats", [])

            if not results:
                print(f"[info] No more results after range {start}-{end}")
                break

            offers.extend(results)
            print(f"[info] fetched {len(results)} offers | cumulative={len(offers)}")

            time.sleep(0.2)

        return offers

    def collect_all(self, max_pages: int = 20) -> pd.DataFrame:
        print(f"\n=== Collecting: {self.SEARCH_QUERY} ===")
        all_offers = self.fetch_offers(query=self.SEARCH_QUERY, max_pages=max_pages)

        if not all_offers:
            print("[warn] No offers collected. Returning an empty DataFrame.")
            return pd.DataFrame()

        df = pd.DataFrame(all_offers)

        before = len(df)
        if "id" in df.columns:
            df = df.drop_duplicates(subset="id").reset_index(drop=True)
        after = len(df)

        print(f"\n[summary] total rows before dedup: {before}")
        print(f"[summary] total rows after dedup:  {after}")

        return df


def load_env() -> None:
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(dotenv_path=env_path)


def get_access_token() -> str:
    client_id = os.getenv("FT_CLIENT_ID")
    client_secret = os.getenv("FT_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError("Missing FT_CLIENT_ID or FT_CLIENT_SECRET in .env file.")

    response = requests.post(
        TOKEN_URL,
        params={"realm": "/partenaire"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "api_offresdemploiv2 o2dsoffre",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(f"Token request failed ({response.status_code}): {response.text}")

    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token found in token response.")

    return token


def save_raw_data(df: pd.DataFrame) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(RAW_FILE, index=False)
    print(f"[saved] Raw offers saved to: {RAW_FILE}")


def main() -> None:
    load_env()
    token = get_access_token()
    print("[ok] Access token retrieved")

    collector = FranceTravailCollector(token=token)
    df = collector.collect_all(max_pages=20)

    if df.empty:
        print("[warn] Empty dataset, file not saved.")
        return

    save_raw_data(df)

    print("\nDone.")
    print(f"Collected {len(df)} unique offers.")


if __name__ == "__main__":
    main()