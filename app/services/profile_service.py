import chardet
import csv
import io
from datetime import datetime
from typing import List, Dict, Any
from app.database.supabase_client import supabase
import asyncio

# Mapping for column names to snake_case
COLUMN_NAME_MAP = {
    "Full name (as per NRIC)": "full_name_nric",
    "What course are you from?": "course",
    "If you are doing Masters, what is your masters course?": "masters_course",
    "If you are taking any DDP, Double Major or Minor, please specify: (eg. DDP with Business Administration)Ê": "ddp_or_minor",
    "Which intake batch are you from?": "intake_batch",
    "(If applicable) Where did you go (or will be going) for SEP/summer/winter (school), NOC (location and company), internships (company)": "overseas_experience",
    "Self write-up (e.g. Yuxuan's self write-up below). It'll be publicly available so you can also use it as a personal showcase page! (Limit: 200 words)": "self_writeup",
    "Upload a picture of yourself! Example on the right": "picture_url",
    "Notable Achievements (if any, up to 3!) Example on the right": "notable_achievements",
    "Any interests/hobbies? (Up to 3!) Example on the right": "hobbies",
    "LinkedIn Link (if any)": "linkedin_link",
    "Personal Email": "personal_email",
    "Instagram Link (if any)": "instagram_link",
    # Add more if your headers differ
}

# These should match your table columns exactly
PROFILES_COLUMNS = set([
    "profile_id", "full_name_nric", "course", "masters_course", "ddp_or_minor",
    "intake_batch", "overseas_experience", "self_writeup", "picture_url",
    "notable_achievements", "hobbies", "linkedin_link", "personal_email",
    "instagram_link", "updated_at"
])
FLAGGED_PROFILES_COLUMNS = PROFILES_COLUMNS | {"issues"} # issues is an extra column

def translate_row_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    """Translate the keys of a row using COLUMN_NAME_MAP."""
    return {COLUMN_NAME_MAP.get(k, k): v for k, v in row.items()}

def detect_issues(profile_data: Dict[str, Any]) -> List[str]:
    issues = []
    email = profile_data.get("personal_email", "")  # Use mapped key
    if "@" not in email or email.strip() == "":
        issues.append("Invalid or missing email")
    return issues

async def process_csv(file_bytes: bytes, upload_id: str):
    detection = chardet.detect(file_bytes)
    encoding = detection.get("encoding") or "utf-8"

    try:
        text = file_bytes.decode(encoding)
    except UnicodeDecodeError as e:
        print(f"Failed to decode CSV with encoding {encoding}: {e}")
        return

    f = io.StringIO(text)
    reader = csv.DictReader(f)

    self_writeup_col = None
    for col in reader.fieldnames:
        if col.startswith("Self write-up"):
            self_writeup_col = col
            break

    if not self_writeup_col:
        print("No 'Self write-up' column found in CSV")
        return

    required_columns = [
        "Full name (as per NRIC)",
        "What course are you from?",
        "Which intake batch are you from?",
        self_writeup_col,
    ]

    for row in reader:
        if any(not row.get(col) or row.get(col).strip() == "" for col in required_columns):
            continue
        if row[self_writeup_col].strip() == "":
            continue

        mapped_row = translate_row_keys(row)
        full_name_raw = mapped_row.get("full_name_nric")
        if not full_name_raw:
            continue

        # Compose the row for upsert: only use columns that exist in the table
        profile_id = full_name_raw.title()
        mapped_row["profile_id"] = profile_id
        mapped_row["updated_at"] = datetime.utcnow().isoformat()
        issues = detect_issues(mapped_row)
        
        if issues:
            # Add issues as a JSON array
            flagged_row = {k: v for k, v in mapped_row.items() if k in FLAGGED_PROFILES_COLUMNS}
            flagged_row["issues"] = issues
            supabase.table("flagged_profiles").upsert(flagged_row).execute()
        else:
            profiles_row = {k: v for k, v in mapped_row.items() if k in PROFILES_COLUMNS}
            supabase.table("profiles").upsert(profiles_row).execute()