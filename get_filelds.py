import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_API_BASE = os.getenv("ZOHO_API_BASE")  # e.g. https://www.zohoapis.com

def get_zoho_access_token():
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
    }
    r = requests.post(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def fetch_deal(deal_id: str) -> dict:
    token = get_zoho_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    url = f"{ZOHO_API_BASE}/crm/v2/Deals/{deal_id}"
    r = requests.get(url, headers=headers, timeout=60)
    print("Status:", r.status_code)
    r.raise_for_status()

    data = r.json().get("data", [])
    return data[0] if data else {}

def preview_value(v):
    """Small helper to show types nicely."""
    if isinstance(v, dict):
        return {"__type__": "dict", "keys": list(v.keys())[:30]}
    if isinstance(v, list):
        return {"__type__": "list", "len": len(v)}
    return {"__type__": type(v).__name__, "sample": str(v)[:120]}

if __name__ == "__main__":
    deal_id = input("Paste Deal ID: ").strip()
    deal = fetch_deal(deal_id)

    if not deal:
        print("No deal returned.")
        raise SystemExit()

    keys = list(deal.keys())
    print("\n=== TOP LEVEL KEYS (first 200) ===")
    print(keys[:200])

    # 1) List fields (subforms)
    list_fields = [k for k, v in deal.items() if isinstance(v, list)]
    print("\n=== LIST FIELDS (usually subforms) ===")
    print(list_fields)

    # 2) Preview each list field structure
    print("\n=== PREVIEW LIST FIELDS STRUCTURE ===")
    for k in list_fields:
        rows = deal.get(k) or []
        print(f"\n--- {k} ---")
        print("Row count:", len(rows))
        if not rows:
            print("Empty subform (no rows).")
            continue

        first = rows[0]
        if not isinstance(first, dict):
            print("First row is not dict:", type(first).__name__)
            continue

        print("First row keys:", list(first.keys()))
        print("First row field types preview:")
        for kk, vv in first.items():
            print(f"  - {kk}: {preview_value(vv)}")

    # 3) Search likely campaign/subform keys by name
    look_for = ("campaign", "advert", "subform")
    likely = [k for k in keys if any(x in k.lower() for x in look_for)]
    print("\n=== KEYS MATCHING campaign/advert/subform ===")
    print(likely)

    # 4) (Optional) dump a small snippet to file for inspection
    with open("deal_debug.json", "w", encoding="utf-8") as f:
        json.dump(deal, f, indent=2, ensure_ascii=False)

    print("\nSaved full deal JSON to: deal_debug.json")
