import os
import time
import json
import hashlib
import requests
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone as dt_timezone, timedelta
from pytz import timezone as pytz_timezone
from dotenv import load_dotenv
from clickhouse_connect import get_client

load_dotenv()

# ======================================================
# LOGGING
# ======================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("zoho_leads_sync")

# ======================================================
# CONFIG
# ======================================================
CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_API_BASE = os.getenv("ZOHO_API_BASE")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_LEADS_TABLE = os.getenv("CLICKHOUSE_LEADS_TABLE", "leads_table")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
DETAIL_WORKERS = int(os.getenv("LEADS_DETAIL_WORKERS", os.getenv("DETAIL_WORKERS", "20")))
DETAIL_CHUNK_LOG = int(os.getenv("LEADS_DETAIL_CHUNK_LOG", "200"))
RECREATE_TABLE = os.getenv("LEADS_RECREATE_TABLE", "false").lower() in ("1", "true", "yes", "y")
DEBUG_SUBFORMS = os.getenv("LEADS_DEBUG_SUBFORMS", "false").lower() in ("1", "true", "yes", "y")
ONLY_INSERT_CHANGED = os.getenv("LEADS_ONLY_INSERT_CHANGED", "true").lower() in ("1", "true", "yes", "y")
USE_MODIFIED_TIME_VERSION = os.getenv("LEADS_USE_MODIFIED_TIME_VERSION", "true").lower() in ("1", "true", "yes", "y")

INCREMENTAL_HOURS = int(os.getenv("LEADS_INCREMENTAL_HOURS", "0"))
AUTO_INCREMENTAL = os.getenv("LEADS_AUTO_INCREMENTAL", "true").lower() in ("1", "true", "yes", "y")
AUTO_INCREMENTAL_FALLBACK_HOURS = int(os.getenv("LEADS_AUTO_INCREMENTAL_FALLBACK_HOURS", "6"))

# Force full sync (ignore incremental, re-fetch everything, no change-detect skip)
FORCE_FULL_SYNC = os.getenv("LEADS_FORCE_FULL_SYNC", "false").lower() in ("1", "true", "yes", "y")

# Force ClickHouse OPTIMIZE after insert so ReplacingMergeTree deduplicates immediately
FORCE_OPTIMIZE = os.getenv("LEADS_FORCE_OPTIMIZE", "true").lower() in ("1", "true", "yes", "y")

# ======================================================
# SUBFORMS
# ======================================================
CAMPAIGNS_SUBFORM_KEY = os.getenv("LEADS_CAMPAIGNS_SUBFORM_KEY", "Campaigns_Subform")
EVENTS_SUBFORM_KEY = os.getenv("LEADS_EVENTS_SUBFORM_KEY", "Events_Subform")
SUBFORM_KEYS = {CAMPAIGNS_SUBFORM_KEY, EVENTS_SUBFORM_KEY}

CAMPAIGNS_SUBFORM_FIELD_MAP = {
    "Advert_ID": "campaigns_advert_id",
    "Advert_Name": "campaigns_advert_name",
    "Advert_Placement": "campaigns_advert_placement",
    "Advert_Set_ID": "campaigns_advert_set_id",
    "Advert_Set_Name": "campaigns_advert_set_name",
    "Advert_Site_Source_Name": "campaigns_advert_site_source_name",
    "Campaign_Content": "campaigns_campaign_content",
    "Campaign_ID": "campaigns_campaign_id",
    "Campaign_Name": "campaigns_campaign_name",
    "Campaign_Sources": "campaigns_campaign_sources",
    "Lead_Medium": "campaigns_lead_medium",
    "Page_URL": "campaigns_page_url",
    "Referrer_URL": "campaigns_referrer_url",
    "Source_Campaign": "campaigns_source_campaign",
}

EVENTS_SUBFORM_FIELD_MAP = {
    "Comments": "events_comments",
    "Event_Date": "events_event_date",
    "Event_Location": "events_event_location",
    "Event_Name": "events_event_name",
    "Status": "events_status",
    "Pick_List_3": "events_comment",
}

FIELD_MAPPING = {
    "id": "id",
    "Full_Name": "full_name",
    "Phone": "phone",
    "Email": "email",
    "Owner": "owner",
}

# ======================================================
# DATETIME PARSING (bulletproof)
# ======================================================
EPOCH_ZERO = datetime(1970, 1, 1, tzinfo=dt_timezone.utc)

def parse_datetime_safe(value) -> datetime:
    """
    Convert any value to a datetime object for ClickHouse.
    Never returns a string — always returns a datetime.
    """
    if isinstance(value, datetime):
        return value
    if not value or not isinstance(value, str):
        return EPOCH_ZERO
    value = value.strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return EPOCH_ZERO


# ======================================================
# HELPERS
# ======================================================
def sanitize_column_name(name: str) -> str:
    return (name or "").strip().lower().replace(" ", "_").replace("-", "_")


def normalize_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        if "name" in value and value.get("name") is not None:
            return str(value["name"])
        if "id" in value and value.get("id") is not None:
            return str(value["id"])
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def record_hash_for_row(row: dict) -> str:
    volatile = {"inserted_at"}
    parts = []
    for k in sorted(row.keys()):
        if k in volatile:
            continue
        v = row.get(k)
        parts.append(f"{k}={'' if v is None else v}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def parse_subform_value(value):
    if value is None:
        return []
    if isinstance(value, list):
        out = []
        for item in value:
            if isinstance(item, dict):
                out.append(item)
            elif isinstance(item, str):
                s = item.strip()
                if not s:
                    continue
                try:
                    j = json.loads(s)
                    if isinstance(j, dict):
                        out.append(j)
                except Exception:
                    pass
        return out
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [r for r in parsed if isinstance(r, dict)]
        except Exception:
            return []
    return []


def get_subform_created_time(row: dict) -> str:
    cb = row.get("Created_By")
    if isinstance(cb, dict):
        return cb.get("created_time") or cb.get("Created_Time") or ""
    if isinstance(cb, list) and cb and isinstance(cb[0], dict):
        return cb[0].get("created_time") or cb[0].get("Created_Time") or ""
    return row.get("Created_Time", "") or row.get("created_time", "") or row.get("Event_Date", "") or ""


def pick_latest_subform_row(rows: list) -> dict:
    if not rows:
        return {}
    try:
        return max(rows, key=lambda r: get_subform_created_time(r) or "")
    except Exception:
        return rows[-1]


def flatten_subform_into_flat(flat: dict, subform_key: str, subform_value):
    rows = parse_subform_value(subform_value)
    if DEBUG_SUBFORMS:
        logger.info("[DEBUG] %s raw_type=%s parsed_rows=%s", subform_key, type(subform_value).__name__, len(rows))
        if rows:
            logger.info("[DEBUG] %s first_row_keys=%s", subform_key, list(rows[0].keys())[:60])
    if not rows:
        return
    chosen = pick_latest_subform_row(rows)
    if not isinstance(chosen, dict):
        return
    mapping = CAMPAIGNS_SUBFORM_FIELD_MAP if subform_key == CAMPAIGNS_SUBFORM_KEY else EVENTS_SUBFORM_FIELD_MAP
    for in_key, out_col in mapping.items():
        if in_key in chosen:
            flat[out_col] = normalize_value(chosen.get(in_key))


def dedupe_preserve_order(items):
    seen = set()
    out = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


# ======================================================
# ZOHO API
# ======================================================
def get_zoho_access_token():
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
    }
    r = requests.post(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]


def zoho_get(session: requests.Session, url: str, headers: dict, params=None, timeout=90):
    last_exc = None
    for attempt in range(1, 6):
        try:
            r = session.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else min(60, 2 ** attempt)
                logger.warning("429 rate limit. Wait %ss: %s", wait, url)
                time.sleep(wait)
                continue
            if r.status_code in (500, 502, 503, 504):
                wait = min(30, 2 ** attempt)
                logger.warning("%s server error. Wait %ss: %s", r.status_code, wait, url)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            wait = min(30, 2 ** attempt)
            logger.warning("Request error: %s. Wait %ss: %s", e, wait, url)
            time.sleep(wait)
    if last_exc:
        raise last_exc
    raise RuntimeError("zoho_get failed unexpectedly")


# ──────────────────────────────────────────────────────
# BULK PAGINATED FETCH
# ──────────────────────────────────────────────────────
def fetch_all_leads_bulk(headers: dict, modified_since: str = None) -> list:
    """
    Fetch leads via paginated list API (200/page).
    50k leads = ~250 requests instead of 50,000.
    """
    logger.info("📥 Fetching leads in bulk pages%s...",
                f" (modified since {modified_since})" if modified_since else " (full sync)")
    session = requests.Session()
    all_leads = []
    page = 1

    req_headers = dict(headers)
    if modified_since:
        req_headers["If-Modified-Since"] = modified_since

    while True:
        url = f"{ZOHO_API_BASE}/crm/v2/Leads"
        params = {"per_page": 200, "page": page}
        try:
            res = zoho_get(session, url, req_headers, params=params, timeout=90)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 304:
                logger.info("✅ No leads modified since %s", modified_since)
                return []
            raise

        payload = res.json()
        records = payload.get("data", []) or []
        all_leads.extend(records)

        more = payload.get("info", {}).get("more_records", False)
        if page % 50 == 0 or not more:
            logger.info("  ...page %s | total so far: %s", page, len(all_leads))
        if not more:
            break
        page += 1

    logger.info("✅ Bulk fetch complete: %s leads in %s pages", len(all_leads), page)
    return all_leads


# ──────────────────────────────────────────────────────
# PER-ID DETAIL FETCH (only for subforms)
# ──────────────────────────────────────────────────────
_thread_local = threading.local()

def get_thread_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


def fetch_lead_by_id(headers: dict, lead_id: str) -> dict:
    session = get_thread_session()
    url = f"{ZOHO_API_BASE}/crm/v2/Leads/{lead_id}"
    res = zoho_get(session, url, headers, params=None, timeout=90)
    payload = res.json()
    data = payload.get("data", [])
    return data[0] if data else {}


def fetch_full_details_parallel(headers: dict, lead_map: dict):
    """Fetch full individual details and REPLACE bulk records entirely."""
    ids = list(lead_map.keys())
    logger.info("📥 Fetching %s full lead details (workers=%s)...", len(ids), DETAIL_WORKERS)

    errors = 0
    start = time.time()

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
        futures = {ex.submit(fetch_lead_by_id, headers, lid): lid for lid in ids}
        done = 0
        for fut in as_completed(futures):
            lid = futures[fut]
            try:
                detail = fut.result()
                if detail:
                    # Replace the entire record with full detail
                    lead_map[lid] = detail
            except Exception as e:
                errors += 1
                if errors <= 20:
                    logger.error("❌ Failed detail %s: %s", lid, e)
            done += 1
            if done % DETAIL_CHUNK_LOG == 0:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                logger.info("  ...detail %s/%s | %.2f/sec | errors=%s", done, len(ids), rate, errors)

    logger.info("✅ Detail fetch done (errors=%s)", errors)


# ──────────────────────────────────────────────────────
# MAIN FETCH ORCHESTRATOR
# ──────────────────────────────────────────────────────
def fetch_zoho_leads_with_subforms_fast(modified_since: str = None):
    access_token = get_zoho_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

    # Step 1: Bulk fetch
    leads = fetch_all_leads_bulk(headers, modified_since=modified_since)
    if not leads:
        return leads

    # Step 2: Check if subforms came with bulk response
    sample = leads[0]
    has_campaigns = CAMPAIGNS_SUBFORM_KEY in sample
    has_events = EVENTS_SUBFORM_KEY in sample
    logger.info("🔎 Bulk includes Campaigns: %s, Events: %s", has_campaigns, has_events)

    if has_campaigns and has_events:
        logger.info("✅ Subforms in bulk — no per-ID fetches needed!")
        return leads

    # Step 3: Bulk didn't include full data — fetch ALL details per ID
    lead_map = {l.get("id"): l for l in leads if l.get("id")}
    fetch_full_details_parallel(headers, lead_map)
    return list(lead_map.values())


# ======================================================
# NORMALIZATION
# ======================================================
def normalize_record(record: dict) -> dict:
    flat = {
        "id": record.get("id", "") or "",
        "full_name": "", "phone": "", "email": "", "owner": "",
        "created_at": "", "created_by_id": "", "created_by_name": "",
    }
    if USE_MODIFIED_TIME_VERSION:
        flat["modified_time"] = ""

    for col in [
        "campaigns_advert_id", "campaigns_advert_name", "campaigns_advert_placement",
        "campaigns_advert_set_id", "campaigns_advert_set_name", "campaigns_advert_site_source_name",
        "campaigns_campaign_content", "campaigns_campaign_id", "campaigns_campaign_name",
        "campaigns_campaign_sources", "campaigns_lead_medium", "campaigns_page_url",
        "campaigns_referrer_url", "campaigns_source_campaign",
        "events_comment", "events_comments", "events_event_date",
        "events_event_location", "events_event_name", "events_status",
    ]:
        flat[col] = ""

    for key, value in record.items():
        if key in SUBFORM_KEYS:
            flatten_subform_into_flat(flat, key, value)
            continue
        if USE_MODIFIED_TIME_VERSION and key in ("Modified_Time", "Last_Modified_Time"):
            flat["modified_time"] = normalize_value(value)
            continue
        if key == "Created_By":
            if isinstance(value, list) and value and isinstance(value[0], dict):
                cb = value[0]
                flat["created_at"] = cb.get("created_time", "") or ""
                flat["created_by_id"] = cb.get("id", "") or ""
                flat["created_by_name"] = cb.get("name", "") or ""
            elif isinstance(value, dict):
                flat["created_at"] = value.get("created_time", "") or ""
                flat["created_by_id"] = value.get("id", "") or ""
                flat["created_by_name"] = value.get("name", "") or ""
            continue
        if key == "Full_Name":
            flat["full_name"] = normalize_value(value); continue
        if key == "Phone":
            flat["phone"] = normalize_value(value); continue
        if key == "Email":
            flat["email"] = normalize_value(value); continue
        if key == "Owner":
            flat["owner"] = (value.get("name") or value.get("id") or "") if isinstance(value, dict) else normalize_value(value)
            continue

        out_key = FIELD_MAPPING.get(key, sanitize_column_name(key))
        if isinstance(value, dict) and "name" in value:
            flat[out_key] = value.get("name") or ""
            continue
        flat[out_key] = "" if value is None else str(value)

    return flat


def prepare_records(raw_records):
    normalized = [normalize_record(r) for r in raw_records]
    inserted_at = datetime.now(pytz_timezone("Africa/Nairobi")).replace(microsecond=0)
    for r in normalized:
        r["inserted_at"] = inserted_at
        r["record_hash"] = record_hash_for_row(r)
    return normalized


# ======================================================
# CLICKHOUSE
# ======================================================
def get_existing_columns(client):
    rows = client.query(f"DESCRIBE TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_LEADS_TABLE}").result_rows
    return {r[0]: r[1] for r in rows}


def get_existing_hashes(client):
    q = f"""
    SELECT id, argMax(record_hash, inserted_at) AS record_hash
    FROM {CLICKHOUSE_DB}.{CLICKHOUSE_LEADS_TABLE}
    GROUP BY id
    """
    try:
        rows = client.query(q).result_rows
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        logger.warning("Could not read existing hashes: %s", e)
        return {}


def get_last_sync_time(client) -> str:
    for col_name in ("modified_time", "Modified_Time"):
        try:
            q = f"SELECT max(`{col_name}`) FROM {CLICKHOUSE_DB}.{CLICKHOUSE_LEADS_TABLE}"
            rows = client.query(q).result_rows
            if rows and rows[0][0]:
                val = rows[0][0]
                if isinstance(val, datetime):
                    return val.strftime("%Y-%m-%dT%H:%M:%S+00:00")
                return str(val)
        except Exception:
            continue
    logger.info("Could not get last sync time (column may not exist)")
    return ""


def table_has_data(client) -> bool:
    try:
        q = f"SELECT count() FROM {CLICKHOUSE_DB}.{CLICKHOUSE_LEADS_TABLE}"
        rows = client.query(q).result_rows
        return rows and rows[0][0] > 0
    except Exception:
        return False


def _col_type(col: str) -> str:
    if col in ("inserted_at", "modified_time"):
        return "DateTime"
    return "String"


def recreate_table(client, column_names):
    logger.info("🗑️ Dropping and recreating `%s.%s`...", CLICKHOUSE_DB, CLICKHOUSE_LEADS_TABLE)
    client.command(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_LEADS_TABLE}")
    cols_def = [f"`{c}` {_col_type(c)}" for c in column_names]
    version_col = "modified_time" if USE_MODIFIED_TIME_VERSION and "modified_time" in column_names else "inserted_at"
    create_sql = f"""
    CREATE TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_LEADS_TABLE} (
        {', '.join(cols_def)}
    ) ENGINE = ReplacingMergeTree({version_col})
    PRIMARY KEY (id) ORDER BY (id)
    """
    client.command(create_sql)
    logger.info("✅ Table recreated. Version column: %s", version_col)


def ensure_table_and_columns(client, records):
    all_keys = sorted({k for r in records for k in r.keys()})
    version_col = "modified_time" if USE_MODIFIED_TIME_VERSION and "modified_time" in all_keys else "inserted_at"
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_LEADS_TABLE} (
        {', '.join([f'`{k}` {_col_type(k)}' for k in all_keys])}
    ) ENGINE = ReplacingMergeTree({version_col}) ORDER BY id
    """
    client.command(create_sql)
    existing = {}
    try:
        existing = get_existing_columns(client)
    except Exception:
        pass
    missing = [k for k in all_keys if k not in existing]
    if missing:
        logger.info("Adding %s missing columns...", len(missing))
        for k in missing:
            client.command(
                f"ALTER TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_LEADS_TABLE} "
                f"ADD COLUMN IF NOT EXISTS `{k}` {_col_type(k)}"
            )


def build_insert_data(records, column_names):
    """
    Build insert data with STRICT type enforcement.
    DateTime columns ALWAYS get datetime objects, never strings.
    This validates BEFORE sending to ClickHouse so errors are caught early.
    """
    datetime_cols = {"inserted_at", "modified_time"}
    data = []
    type_errors = []

    for idx, r in enumerate(records):
        row = []
        for col in column_names:
            if col in datetime_cols:
                raw = r.get(col)
                if col == "modified_time" and not raw:
                    # Fallback to inserted_at if no modified_time
                    raw = r.get("inserted_at")
                parsed = parse_datetime_safe(raw)
                if not isinstance(parsed, datetime):
                    type_errors.append(f"Row {idx}, col {col}: got {type(parsed)} = {parsed!r}")
                    parsed = EPOCH_ZERO
                row.append(parsed)
            else:
                v = r.get(col, "")
                row.append("" if v is None else str(v))
        data.append(row)

    if type_errors:
        logger.warning("⚠️  %s type conversion issues (using fallback):", len(type_errors))
        for e in type_errors[:5]:
            logger.warning("   %s", e)

    return data


def validate_insert_data(data, column_names):
    """
    Pre-flight check: ensure every DateTime column has actual datetime objects.
    Raises early with a clear error instead of failing deep in clickhouse_connect.
    """
    datetime_cols = {i for i, c in enumerate(column_names) if c in ("inserted_at", "modified_time")}

    for row_idx, row in enumerate(data[:10]):  # check first 10 rows
        for col_idx in datetime_cols:
            val = row[col_idx]
            if not isinstance(val, datetime):
                raise TypeError(
                    f"VALIDATION FAILED: Row {row_idx}, column '{column_names[col_idx]}' "
                    f"expected datetime but got {type(val).__name__}: {val!r}"
                )

    logger.info("✅ Pre-insert validation passed (%s rows, %s columns)", len(data), len(column_names))


def insert_records(client, records):
    if not records:
        logger.warning("No records to insert.")
        return

    column_names = sorted({k for r in records for k in r.keys()})

    if RECREATE_TABLE:
        recreate_table(client, column_names)
    else:
        ensure_table_and_columns(client, records)

    logger.info("📤 Building insert data for %s records...", len(records))
    data = build_insert_data(records, column_names)

    # Validate BEFORE inserting — fail fast with clear error
    validate_insert_data(data, column_names)

    logger.info("📤 Inserting %s records into ClickHouse...", len(records))
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        client.insert(CLICKHOUSE_LEADS_TABLE, batch, column_names=column_names)
        logger.info("  ✅ Inserted rows %s → %s", i, i + len(batch))

    logger.info("✅ Insert complete.")


# ======================================================
# MAIN SYNC JOB
# ======================================================
def sync_job():
    start_time = time.time()
    logger.info("🚀 Leads sync started at %s", datetime.now(dt_timezone.utc).isoformat())

    client = get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USERNAME, password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB, send_receive_timeout=120,
    )

    # ── Determine incremental vs full sync ──
    modified_since = None

    if FORCE_FULL_SYNC:
        logger.info("📦 FORCE_FULL_SYNC enabled — fetching ALL leads, no change-detect skip")
    elif INCREMENTAL_HOURS > 0:
        cutoff = datetime.now(dt_timezone.utc) - timedelta(hours=INCREMENTAL_HOURS)
        modified_since = cutoff.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        logger.info("🔄 Incremental sync (explicit): last %s hours", INCREMENTAL_HOURS)
    elif AUTO_INCREMENTAL and not RECREATE_TABLE:
        if table_has_data(client):
            last_sync = get_last_sync_time(client)
            if last_sync:
                modified_since = last_sync
                logger.info("🔄 Auto-incremental from: %s", modified_since)
            else:
                cutoff = datetime.now(dt_timezone.utc) - timedelta(hours=AUTO_INCREMENTAL_FALLBACK_HOURS)
                modified_since = cutoff.strftime("%Y-%m-%dT%H:%M:%S+00:00")
                logger.info("🔄 Auto-incremental fallback: last %s hours", AUTO_INCREMENTAL_FALLBACK_HOURS)
        else:
            logger.info("📦 First run — full sync")
    else:
        logger.info("📦 Full sync mode")

    # ── Fetch from Zoho ──
    leads = fetch_zoho_leads_with_subforms_fast(modified_since=modified_since)

    if leads:
        keys = list(leads[0].keys())
        logger.info("🔎 Sample keys (%s total): %s", len(keys), keys[:30])

    records = prepare_records(leads)

    # ── Validate data types EARLY before any DB work ──
    if records:
        logger.info("🔍 Validating data types before insert...")
        sample = records[0]
        mt = sample.get("modified_time")
        ia = sample.get("inserted_at")
        logger.info("  modified_time: type=%s value=%r", type(mt).__name__, mt)
        logger.info("  inserted_at:   type=%s value=%r", type(ia).__name__, ia)

        # Quick test parse
        test_dt = parse_datetime_safe(mt)
        logger.info("  parsed modified_time: type=%s value=%r", type(test_dt).__name__, test_dt)

        if not isinstance(test_dt, datetime):
            logger.error("❌ FATAL: modified_time did not parse to datetime! Aborting.")
            return

    # ── Change detection (skip if FORCE_FULL_SYNC) ──
    if ONLY_INSERT_CHANGED and not FORCE_FULL_SYNC and records:
        existing_hashes = get_existing_hashes(client)
        before = len(records)
        records = [r for r in records if existing_hashes.get(r["id"]) != r["record_hash"]]
        logger.info("🧹 Change-detect: %s → %s (skipped %s)", before, len(records), before - len(records))

    insert_records(client, records)

    # Force ReplacingMergeTree to deduplicate NOW so queries return latest data
    if FORCE_OPTIMIZE and records:
        logger.info("🔄 Running OPTIMIZE FINAL to deduplicate rows...")
        try:
            client.command(f"OPTIMIZE TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_LEADS_TABLE} FINAL")
            logger.info("✅ OPTIMIZE FINAL complete — stale rows replaced.")
        except Exception as e:
            logger.warning("⚠️  OPTIMIZE FINAL failed (non-fatal): %s", e)

    elapsed = time.time() - start_time
    logger.info("🎉 Leads sync completed in %.1f seconds (%.1f minutes).", elapsed, elapsed / 60)


if __name__ == "__main__":
    sync_job()