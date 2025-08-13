import csv
import io
import uuid
from datetime import datetime
from typing import List, Dict, Any
from app.database.supabase_client import supabase
import asyncio

def detect_issues(profile_data: Dict[str, Any]) -> List[str]:
    issues = []
    # Example validation: check if "Personal Email" is valid
    email = profile_data.get("Personal Email", "")
    if "@" not in email or email.strip() == "":
        issues.append("Invalid or missing email")
    # Add more validation rules as needed
    return issues

async def process_csv(file_bytes: bytes, upload_id: str):
    f = io.StringIO(file_bytes.decode())
    reader = csv.DictReader(f)

    for row in reader:
        profile_id = row.get("Full name (as per NRIC)")
        if not profile_id:
            continue

        issues = detect_issues(row)
        data_dict = dict(row)

        if issues:
            # Upsert flagged profile in Supabase table 'flagged_profiles'
            await supabase.table("flagged_profiles").upsert({
                "profile_id": profile_id,
                "data": data_dict,
                "issues": issues,
                "updated_at": datetime.utcnow().isoformat(),
            }).execute()
        else:
            # Upsert clean profile in Supabase table 'profiles'
            await supabase.table("profiles").upsert({
                "profile_id": profile_id,
                "data": data_dict,
                "updated_at": datetime.utcnow().isoformat(),
            }).execute()

    # Optionally, you can update an uploads table with status here