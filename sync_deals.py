import os
import time
import json
import hashlib
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone as dt_timezone
from pytz import timezone as pytz_timezone
from dotenv import load_dotenv
from collections import defaultdict, Counter
from clickhouse_connect import get_client

load_dotenv()

# ======================================================
# CONFIG
# ======================================================
CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_API_BASE = os.getenv("ZOHO_API_BASE")  # e.g. https://www.zohoapis.com

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))
CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))

# Concurrency (speed)
DETAIL_WORKERS = int(os.getenv("DETAIL_WORKERS", "12"))      # try 8–16
DETAIL_CHUNK_LOG = int(os.getenv("DETAIL_CHUNK_LOG", "200")) # progress log step

# Turn on only when debugging subforms
DEBUG_SUBFORMS = False  # set True to print row shapes

# ======================================================
# SUBFORMS (TOP-LEVEL KEYS in GET /Deals/{id})
# ======================================================
CAMPAIGNS_SUBFORM_KEY = "Campaigns_Subform_Opps"
EVENTS_SUBFORM_KEY = "Events_Subform_on_Opps"
SUBFORM_KEYS = {CAMPAIGNS_SUBFORM_KEY, EVENTS_SUBFORM_KEY}

# ======================================================
# STAGE MAPPING
# ======================================================
STAGE_VALUES = {
    "Student Enrolled": 15,
    "Visa Issued": 14,
    "Visa Application Submitted": 13,
    "Deposit Paid": 12,
    "Partial Deposit Paid": 11,
    "Offer Accepted": 10,
    "Offer Received": 9,
    "Conditional Offer Accepted": 8,
    "Conditional Offer Received": 7,
    "Application Done": 6,
    "Application Ready for Submission": 5,
    "Awaiting Documents": 4,
    "Course Decided": 3,
    "Open Opportunity": 2,
    "Enrolled , but no Commission": 0,
    "Did Not Enroll (after paying deposit)": -1,
    "Did Not Enroll (after visa issued)": -2,
    "Enrolled, but Dropped Out": -3,
    "Visa Application Rejected": -4,
    "Application Withdrawn": -5,
    "Application Rejected": -6,
    "Offer Declined": -7,
    "Future Intake": -8,
    "Closed-Lost to Competition": -9,
    "Not Interested": -10,
    "Visa Application Withdrawn": -11,
}

# ======================================================
# DESIRED COLUMNS (your existing + subform columns)
# ======================================================
DESIRED_COLUMNS = [
    "student_id", "opportunity_id", "student_name", "count_instances", "stage", "stage_value",
    "most_advanced_stage", "lead_source", "lead_source_main_category", "Digital_Leads_Sub_Category", "lead_source_sub_category",
    "high_school_fair_lead_source", "deal_owner", "counsellor_country_location", "counsellor_city",
    "current_previous_high_school", "current_previous_university", "current_high_school_stream",
    "current_high_school_year", "expected_year_of_graduation", "previous_undergraduate_or_diploma_course_category",
    "year_of_high_school_graduation1", "year_of_graduation", "course_graduate_levels",
    "year_of_university_graduation", "student_country_location", "student_county_state",
    "what_level_of_study_are_you_applying_for", "study_destination", "institution_name",
    "institution_group", "study_destination_type", "which_intake", "enrollment_deferred_to_another_intake",
    "correct_intake", "intake_quarter", "intake_year", "study_type", "application_fee_paid",
    "reason_for_application_rejection", "professional_fee_paid", "reason_for_not_paying_professional_fee",
    "deposit_payment_confirmation", "deposit_amount_paid", "date_of_birth", "reason_for_did_not_enroll",
    "comments_on_enrollment_probability", "enrollment_year", "did_not_enroll_next_steps",
    "visa_is_required", "visa_submitted_through_craydel", "visa_manager", "visa_officer",
    "previous_visa_rejection_with_craydel", "comments_on_enrollment_probability_visa_quality",
    "enrolment_probability_country_risk", "enrolment_probability_timing_risk", "enrollment_probability",
    "source_campaign", "campaign_source", "campaign_name", "campaign_content", "advert_name",
    "advert_set_id", "advert_id", "advert_site_source_name", "advert_placement", "advert_set_name",
    "is_most_advanced", "course_graduate_level", "which_fair_are_you_interested_in",
    "student_world_fair_status", "has_used_the_university_matchmaker", "last_note_added",
    "referred_by_future_shaper", "referred_by_employee", "referred_by_employee_id", "referred_by_university",
    "referred_by_other_universities", "created_at", "created_by_id", "created_by_name",
    "application_submission_date", "application_rejection_date", "date_professional_fee_paid", "deposit_paid_date",
    "expected_deposit_date", "visa_officer_name", "visa_officer_id", "visa_submitted_date",
    "visa_issued_date", "visa_rejected_date", "visa_quality_probability", "visa_target_submission_date",
    "visa_next_action_item", "visa_excalation_who_when_what", "enrolled_date", "last_activity_time",
    "last_note_added_date", "sync_lead_creation_date",
    "sync_lead_id", "offer_letter_received_date", "inserted_at", "last_note_owner", "Student_Gender",
    "Offer_Letter_Upload_document_on_Zoho_first", "Reason_for_Offer_Declined", "Days_Left_until_Deadline",
    "Course_start_date_Course_Start_Date_Extension",
    "Last_Date_to_Enroll_in_Person",

    # --- Campaigns subform (prefixed) ---
    "campaigns_advert_id",
    "campaigns_advert_name",
    "campaigns_advert_placement",
    "campaigns_advert_set_id",
    "campaigns_advert_set_name",
    "campaigns_advert_site_source_name",
    "campaigns_campaign_content",
    "campaigns_campaign_id",
    "campaigns_campaign_name",
    "campaigns_campaign_sources",
    "campaigns_lead_medium",
    "campaigns_page_url",
    "campaigns_referrer_url",
    "campaigns_source_campaign",

    # --- Events subform (prefixed) ---
    "events_comment",
    "events_comments",
    "events_event_date",
    "events_event_location",
    "events_event_name",
    "events_status",
]

# ======================================================
# MAIN FIELD MAPPING
# ======================================================
def sanitize_column_name(name: str) -> str:
    return name.lower().strip().replace(" ", "_").replace("-", "_")

FIELD_MAPPING = {
    "Contact": "student_id",
    "id": "opportunity_id",
    "Deal_Name": "student_name",
    "Stage": "stage",
    "Lead_Source": "lead_source",
    "Lead_Source_Main_Category": "lead_source_main_category",
    "Digital_Leads_Sub_Category": "Digital_Leads_Sub_Category",
    "Lead_Source_Sub_Category": "lead_source_sub_category",
    "High_School_Fair_Lead_Source": "high_school_fair_lead_source",
    "Owner": "deal_owner",
    "Counsellor_Country_Location": "counsellor_country_location",
    "Counsellor_City": "counsellor_city",
    "Current_Previous_High_School1": "current_previous_high_school",
    "Current_Previous_University": "current_previous_university",
    "Current_High_School_Stream": "current_high_school_stream",
    "Current_High_School_Year": "current_high_school_year",
    "Expected_year_of_graduation": "expected_year_of_graduation",
    "Previous_undergraduate_or_diploma_course_category": "previous_undergraduate_or_diploma_course_category",
    "Year_of_High_School_Graduation1": "year_of_high_school_graduation1",
    "Year_of_graduation": "year_of_graduation",
    "Course_Graduate_Levels": "course_graduate_levels",
    "Year_of_University_Graduation": "year_of_university_graduation",
    "Student_Country_Location": "student_country_location",
    "Student_County_State": "student_county_state",
    "What_level_of_study_are_you_applying_for": "what_level_of_study_are_you_applying_for",
    "Study_Destination": "study_destination",
    "Institution_Name": "institution_name",
    "Institution_Group1": "institution_group",
    "Institution_Group": "study_destination_type",
    "Which_Intake": "which_intake",
    "Enrollment_deferred_to_another_intake": "enrollment_deferred_to_another_intake",
    "Correct_Intake": "correct_intake",
    "Intake_Quarter": "intake_quarter",
    "Intake_Year": "intake_year",
    "Study_type": "study_type",
    "Application_fee_paid": "application_fee_paid",
    "Reason_for_Application_Rejected": "reason_for_application_rejection",
    "Professional_Fee_Paid": "professional_fee_paid",
    "Reason_for_not_Paying_Professional_Fee": "reason_for_not_paying_professional_fee",
    "Deposit_Payment_Confirmation": "deposit_payment_confirmation",
    "Deposit_Amount_Paid": "deposit_amount_paid",
    "Date_of_Birth": "date_of_birth",
    "Reason_for_Did_Not_Enroll": "reason_for_did_not_enroll",
    "Comments_on_Enrollment_Probability": "comments_on_enrollment_probability",
    "Enrollment_Year": "enrollment_year",
    "Did_Not_Enroll_Next_Steps": "did_not_enroll_next_steps",
    "Visa_is_required": "visa_is_required",
    "Visa_submitted_through_Craydel": "visa_submitted_through_craydel",
    "Visa_Manager": "visa_manager",
    "Visa_Officer": "visa_officer",
    "Previous_Visa_Rejection_with_Craydel": "previous_visa_rejection_with_craydel",
    "Comments_on_Enrollment_Probability_Visa_Quality": "comments_on_enrollment_probability_visa_quality",
    "Enrolment_Probability_Country_Risk": "enrolment_probability_country_risk",
    "Enrolment_Probability_Timing_Risk": "enrolment_probability_timing_risk",
    "Enrolment": "enrollment_probability",
    "Source_Campaign": "source_campaign",
    "Campaign_Source": "campaign_source",
    "Campaign_Name": "campaign_name",
    "Campaign_Content": "campaign_content",
    "Advert_Name": "advert_name",
    "Advert_Set_ID": "advert_set_id",
    "Advert_ID": "advert_id",
    "Advert_Site_Source_Name": "advert_site_source_name",
    "Advert_Placement": "advert_placement",
    "Advert_Set_Name": "advert_set_name",
    "Is_Most_Advanced": "is_most_advanced",
    "Course_Graduate_Level": "course_graduate_level",
    "Which_Fair_are_You_Interested_In": "which_fair_are_you_interested_in",
    "Student_world_fair_status": "student_world_fair_status",
    "Has_Used_The_University_Matchmaker": "has_used_the_university_matchmaker",
    "Last_Note_Added": "last_note_added",
    "Referred_By": "referred_by_future_shaper",
    "Referred_By_Employee": "referred_by_employee",
    "Referred_By_Employee_ID": "referred_by_employee_id",
    "Referred_By_University": "referred_by_university",
    "Referred_By_Other_Universites": "referred_by_other_universities",
    "APPLICATION_SUBMITTED_DATE": "application_submission_date",
    "APPLICATION_REJECTED_DATE": "application_rejection_date",
    "Date_Professional_Fee_Paid": "date_professional_fee_paid",
    "DEPOSIT_PAID_DATE": "deposit_paid_date",
    "Expected_Deposit_Date": "expected_deposit_date",
    "Visa_Officer_Name": "visa_officer_name",
    "Visa_Officer_ID": "visa_officer_id",
    "VISA_SUBMITTED_DATE": "visa_submitted_date",
    "VISA_ISSUED_DATE": "visa_issued_date",
    "VISA_REJECTED_DATE": "visa_rejected_date",
    "Enrolment_Probability_Visa": "visa_quality_probability",
    "Target_Submission_Date": "visa_target_submission_date",
    "Visa_Next_Action_Item": "visa_next_action_item",
    "Visa_Escalation_who_when_and_what": "visa_excalation_who_when_what",
    "ENROLLED_DATE": "enrolled_date",
    "Last_Activity_Time": "last_activity_time",
    "Last_Note_Added_Date": "last_note_added_date",
    "Sync_Lead_Creation_Date": "sync_lead_creation_date",
    "Sync_Lead_ID": "sync_lead_id",
    "OFFER_LETTER_RECEIVED_DATE": "offer_letter_received_date",
    "Last_Note_Owner": "last_note_owner",
    "Student_Gender": "Student_Gender",
    "Offer_Letter_Upload_document_on_Zoho_first": "Offer_Letter_Upload_document_on_Zoho_first",
    "Reason_for_Offer_Declined": "Reason_for_Offer_Declined",

    # ✅ KEY FIX: Days_Left_until_Deadline is TOP-LEVEL in Deals, map it here
    "Days_Left_until_Deadline": "Days_Left_until_Deadline",
    "Last_Date_to_Enroll_in_Person": "Last_Date_to_Enroll_in_Person",
    "Course_start_date_Course_Start_Date_Extension": "Course_start_date_Course_Start_Date_Extension",
}

# ======================================================
# SUBFORM FIELD MAPS
# ======================================================
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
    "Pick_List_3": "events_comment",
    "Comments": "events_comments",
    "Event_Date": "events_event_date",
    "Event_Location": "events_event_location",
    "Event_Name": "events_event_name",
    "Status": "events_status",
}

# ======================================================
# HELPERS
# ======================================================
def record_hash_for_row(row: dict) -> str:
    values = [str(row.get(col, "")) for col in DESIRED_COLUMNS]
    return hashlib.sha256("|".join(values).encode()).hexdigest()

def normalize_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        if "name" in value and value.get("name") is not None:
            return str(value.get("name"))
        if "id" in value and value.get("id") is not None:
            return str(value.get("id"))
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    return str(value)

def parse_subform_value(value):
    """
    Handles subforms that may be:
      - list[dict]
      - list[str JSON]
      - JSON string of list[dict]
    Returns list[dict]
    """
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
        print(f"[DEBUG] {subform_key} raw_type={type(subform_value).__name__} parsed_rows={len(rows)}")
        if rows:
            print(f"[DEBUG] {subform_key} first_row_keys={list(rows[0].keys())[:60]}")

    if not rows:
        return

    chosen = pick_latest_subform_row(rows)
    if not isinstance(chosen, dict):
        return

    mapping = CAMPAIGNS_SUBFORM_FIELD_MAP if subform_key == CAMPAIGNS_SUBFORM_KEY else EVENTS_SUBFORM_FIELD_MAP
    for in_key, out_col in mapping.items():
        if in_key in chosen:
            flat[out_col] = normalize_value(chosen.get(in_key))

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
    response = requests.post(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()["access_token"]

def zoho_get(session: requests.Session, url: str, headers: dict, params=None, timeout=90):
    """
    Retry/backoff only when needed.
    Honors Retry-After if present.
    """
    last_exc = None
    for attempt in range(1, 6):
        try:
            r = session.get(url, headers=headers, params=params, timeout=timeout)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else min(60, 2 ** attempt)
                print(f"⚠️ 429 rate limit. Wait {wait}s then retry: {url}")
                time.sleep(wait)
                continue

            if r.status_code in (500, 502, 503, 504):
                wait = min(30, 2 ** attempt)
                print(f"⚠️ {r.status_code} server error. Wait {wait}s then retry: {url}")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r

        except Exception as e:
            last_exc = e
            wait = min(30, 2 ** attempt)
            print(f"⚠️ Request error: {e}. Wait {wait}s retry: {url}")
            time.sleep(wait)

    if last_exc:
        raise last_exc
    raise RuntimeError("zoho_get failed without exception (unexpected)")

def fetch_deal_ids(headers: dict):
    """
    Fast: fetch IDs only.
    """
    print("📥 Fetching deal IDs from Zoho CRM...")
    session = requests.Session()

    all_ids, page = [], 1
    while True:
        url = f"{ZOHO_API_BASE}/crm/v2/Deals"
        params = {"per_page": 200, "page": page, "fields": "id"}
        res = zoho_get(session, url, headers, params=params, timeout=90)
        payload = res.json()

        records = payload.get("data", [])
        ids = [r.get("id") for r in records if r.get("id")]
        all_ids.extend(ids)

        if not payload.get("info", {}).get("more_records", False):
            break
        page += 1

    print(f"✅ Total deal IDs fetched: {len(all_ids)}")
    return all_ids

# Thread-local Session (requests.Session is not safely shared across threads)
_thread_local = threading.local()

def get_thread_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session

def fetch_deal_by_id(headers: dict, deal_id: str) -> dict:
    """
    GET /Deals/{id} -> includes subforms (if record has them).
    """
    session = get_thread_session()
    url = f"{ZOHO_API_BASE}/crm/v2/Deals/{deal_id}"
    res = zoho_get(session, url, headers, params=None, timeout=90)
    payload = res.json()
    data = payload.get("data", [])
    return data[0] if data else {}

def fetch_zoho_deals_with_subforms_fast():
    """
    2-step:
      1) fetch IDs (fast)
      2) fetch each deal by ID IN PARALLEL (fast)
    """
    access_token = get_zoho_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

    ids = fetch_deal_ids(headers)

    print(f"📥 Fetching full deals by ID in parallel (workers={DETAIL_WORKERS})...")
    deals = []
    errors = 0

    start = time.time()
    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
        futures = {ex.submit(fetch_deal_by_id, headers, deal_id): deal_id for deal_id in ids}

        done = 0
        for fut in as_completed(futures):
            deal_id = futures[fut]
            try:
                deal = fut.result()
                if deal:
                    deals.append(deal)
            except Exception as e:
                errors += 1
                if errors <= 20:
                    print(f"❌ Failed deal {deal_id}: {e}")

            done += 1
            if done % DETAIL_CHUNK_LOG == 0:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                print(f"  ...fetched {done}/{len(ids)} deals | {rate:.2f} deals/sec | errors={errors}")

    print(f"✅ Total full deals fetched: {len(deals)} (errors={errors})")
    return deals

# ======================================================
# NORMALIZATION
# ======================================================
def normalize_record(record: dict) -> dict:
    flat = {"student_id": "", "opportunity_id": record.get("id", "")}

    # prefill subform columns (including Days_Left_until_Deadline)
    for col in [
        "campaigns_advert_id","campaigns_advert_name","campaigns_advert_placement",
        "campaigns_advert_set_id","campaigns_advert_set_name","campaigns_advert_site_source_name",
        "campaigns_campaign_content","campaigns_campaign_id","campaigns_campaign_name",
        "campaigns_campaign_sources","campaigns_lead_medium","campaigns_page_url",
        "campaigns_referrer_url","campaigns_source_campaign",
        "events_comment","events_comments","events_event_date","events_event_location",
        "events_event_name","events_status",
    ]:
        flat[col] = ""

    for key, value in record.items():
        # SUBFORMS
        if key in SUBFORM_KEYS:
            flatten_subform_into_flat(flat, key, value)
            continue

        field_name = FIELD_MAPPING.get(key, sanitize_column_name(key))

        # student_id
        if key in ["Contact", "Contact_Name"] and isinstance(value, dict):
            flat["student_id"] = value.get("id", "") or ""
            continue

        # created_at/by
        if key == "Created_By":
            if isinstance(value, list) and value and isinstance(value[0], dict):
                created_by = value[0]
                flat["created_at"] = created_by.get("created_time", "") or ""
                flat["created_by_id"] = created_by.get("id", "") or ""
                flat["created_by_name"] = created_by.get("name", "") or ""
            elif isinstance(value, dict):
                flat["created_at"] = value.get("created_time", "") or ""
                flat["created_by_id"] = value.get("id", "") or ""
                flat["created_by_name"] = value.get("name", "") or ""
            continue

        if isinstance(value, dict) and "name" in value:
            flat[field_name] = value.get("name") or ""
            continue

        flat[field_name] = "" if value is None else str(value)

    return flat

def prepare_records(raw_records):
    normalized = [normalize_record(r) for r in raw_records]

    student_counter = Counter(r.get("student_id", "") for r in normalized)

    student_stage_map = defaultdict(list)
    for r in normalized:
        sid = r.get("student_id", "")
        r["count_instances"] = str(student_counter.get(sid, 0))
        stage = r.get("stage", "")
        r["stage_value"] = str(STAGE_VALUES.get(stage, -99))
        if sid:
            student_stage_map[sid].append((stage, STAGE_VALUES.get(stage, -99)))

    most_advanced = {
        sid: max(stages, key=lambda x: x[1])[0]
        for sid, stages in student_stage_map.items()
        if stages
    }

    inserted_at = datetime.now(pytz_timezone("Africa/Nairobi")).replace(microsecond=0)

    final = []
    for r in normalized:
        sid = r.get("student_id", "")
        r["most_advanced_stage"] = most_advanced.get(sid, "")
        r["inserted_at"] = inserted_at

        row = {col: r.get(col, "") for col in DESIRED_COLUMNS}
        row["record_hash"] = record_hash_for_row(row)
        final.append(row)

    return final

# ======================================================
# CLICKHOUSE
# ======================================================
def recreate_table(client):
    print(f"🗑️ Dropping and recreating `{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}`...")
    client.command(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")

    columns_def = [
        f"`{col}` DateTime" if col == "inserted_at" else f"`{col}` String"
        for col in DESIRED_COLUMNS
    ]
    columns_def.append("`record_hash` String")

    create_sql = f"""
    CREATE TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
        {', '.join(columns_def)}
    ) ENGINE = ReplacingMergeTree(inserted_at)
    PRIMARY KEY (opportunity_id)
    ORDER BY (opportunity_id)
    """
    client.command(create_sql)
    print("✅ Table ready.")

def insert_records(client, records):
    print(f"📤 Inserting {len(records)} records into ClickHouse...")
    column_names = [*DESIRED_COLUMNS, "record_hash"]

    data = []
    for r in records:
        row = []
        for col in column_names:
            if col == "inserted_at":
                row.append(r.get(col))
            else:
                row.append(str(r.get(col, "")) if r.get(col) is not None else "")
        data.append(row)

    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i : i + BATCH_SIZE]
        client.insert(CLICKHOUSE_TABLE, batch, column_names=column_names)
        print(f"  ✅ Inserted rows {i} → {i + len(batch)}")

    print("✅ Insert complete.")

# ======================================================
# MAIN
# ======================================================
def sync_job():
    print(f"\n🚀 Sync started at {datetime.now(dt_timezone.utc).isoformat()}")

    client = get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USERNAME,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
        send_receive_timeout=120,
    )

    recreate_table(client)

    # FAST fetch
    deals = fetch_zoho_deals_with_subforms_fast()

    # sanity check
    if deals:
        keys = list(deals[0].keys())
        print("🔎 Sample keys:", keys[:40])
        print("🔎 Campaigns key present:", CAMPAIGNS_SUBFORM_KEY in deals[0])
        print("🔎 Events key present:", EVENTS_SUBFORM_KEY in deals[0])

    records = prepare_records(deals)

    if records:
        sample = records[0]
        print("🧪 Sample flattened subform values:")
        print("  campaigns_campaign_name:", sample.get("campaigns_campaign_name"))
        print("  campaigns_advert_id:", sample.get("campaigns_advert_id"))
        print("  events_event_name:", sample.get("events_event_name"))
        print("  events_event_date:", sample.get("events_event_date"))
        print("  Days_Left_until_Deadline:", sample.get("Days_Left_until_Deadline"))
        print("  Last_Date_to_Enroll_in_Person:", sample.get("Last_Date_to_Enroll_in_Person"))

    insert_records(client, records)

    print("🎉 Sync job completed successfully.\n")

if __name__ == "__main__":
    sync_job()
