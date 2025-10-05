import chardet
import csv
import io
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
from app.database.supabase_client import supabase
import asyncio
import re

# Comprehensive mapping for column names to snake_case, handling different versions of forms
COLUMN_NAME_MAP = {
    # Names/IDs
    "Name": "full_name",
    "Full Name (as per NRIC)": "full_name",
    
    # # Course/Programme
    # "B.Eng. Major": "bachelor_course",
    # "Major": "bachelor_course",

    # Masters (if any)
    "Major (in full)": "masters_course",

    # DDP/Minor
    "Special Programmes (DDP outside of CDE, Second Majors, Minors)": "ddp_or_minor",
    "Specialisation in ...": "ddp_or_minor",  # for variants
    "Second Major in ...": "ddp_or_minor",
    "Minor in ...": "ddp_or_minor",

    # Intake batch
    "Year of Admission": "intake_batch",

    # Overseas Experience
    "(If applicable) Where did you go (or will be going) for SEP/summer/winter (school), NOC (location and company), internships (company)": "overseas_experience",

    # Write-up
    "Please provide a short write-up of yourself.": "self_writeup",
    "Self write-up (e.g. Yuxuan's self write-up below). It'll be publicly available so you can also use it as a personal showcase page! (Limit: 200 words)": "self_writeup",

    # Photo
    "Upload a picture of yourself.": "picture_url",
    "Upload a picture of yourself! Example on the right": "picture_url",

    # Achievements
    "Notable Achievements (max 3)": "notable_achievements",
    "Notable Achievements (if any, up to 3!) Example on the right": "notable_achievements",

    # Hobbies
    "Any interests/ hobbies (max 3)": "hobbies",
    "Any interests/hobbies? (Up to 3!) Example on the right": "hobbies",

    # Socials
    "Linkedin Profile URL": "linkedin_link",
    "LinkedIn Link (if any)": "linkedin_link",
    "Instagram Profile URL": "instagram_link",
    "Instagram Link (if any)": "instagram_link",
    "Github Profile URL": "github_link",
}

# Match columns exactly
# TODO: remove overseas_experience
PROFILES_COLUMNS = set([
    "full_name", "bachelor_course", "masters_course", "ddp_or_minor",
    "intake_batch", "overseas_experience", "self_writeup", "picture_url",
    "notable_achievements", "hobbies", "linkedin_link",
    "instagram_link", "github_link", "last_modified"
])
STAGING_COLUMNS = PROFILES_COLUMNS | {"issues"} # issues is an extra column

def split_bullet_points(text: str) -> List[str]:
    """Split text into bullet points, handling various bullet point styles and standardize to * format."""
    if not text:
        return []
    
    # Split by newlines first
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    # If no newlines, try splitting by common bullet point markers
    if len(lines) == 1:
        text = lines[0]
        for separator in ['•', '●', '·', '∙', '⚫', '⬤', '○', '◯', '☉', '*', '-', ';', ',', '.']:
            if separator in text:
                lines = [line.strip() for line in text.split(separator) if line.strip()]
                break
    
    # Clean up each line and standardize format
    cleaned_points = []
    for line in lines:
        # Remove any existing bullet point markers
        line = re.sub(r'^[-•●·∙⚫⬤○◯☉*-;,.]\s*', '', line.strip())
        if line:
            cleaned_points.append(f"{line}")
    
    return cleaned_points

def clean_bachelor_course(course: str) -> str:
    """
    Removes faculty prefix (e.g., 'MPE - ') and trailing semicolons from the bachelor_course field.
    """
    if not isinstance(course, str):
        return course
    # Remove leading 'XXX - ' (any uppercase letters/spaces) and trailing semicolons/spaces
    course = re.sub(r'^[A-Z\s]+-\s*', '', course)
    course = course.strip().rstrip(';')
    if course == "Multidisciplinary Programme (Computer Engineering)":
        course = "Computer Engineering"
    return course

def is_non_empty(val):
    return isinstance(val, str) and val.strip() != ""

def translate_row_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    """Translate the keys of a row using COLUMN_NAME_MAP."""
    translated = {COLUMN_NAME_MAP.get(k, k): v for k, v in row.items()}

    bachelor_course = None
    if is_non_empty(row.get("B.Eng. Major", "")):
        bachelor_course = row["B.Eng. Major"]
    elif is_non_empty(row.get("Major", "")):
        bachelor_course = row["Major"]
    if bachelor_course is not None:
        translated["bachelor_course"] = bachelor_course
    
    # Process intake_batch
    if "intake_batch" in translated:
        batch = translated["intake_batch"]
        if batch:
            # Extract AY##/## pattern
            match = re.search(r'AY\d{2}/\d{2}', batch)
            if match:
                translated["intake_batch"] = match.group(0)

    return translated

def detect_and_fix_issues(profile_data: Dict[str, Any]) -> tuple[List[str], Dict[str, Any]]:
    """Detect issues and fix them in one pass, returning both issues and fixed data."""
    issues = []
    fixed_data = profile_data.copy()

    # Notable Achievements
    achievements = fixed_data.get("notable_achievements", "")
    if achievements:
        points = split_bullet_points(achievements)
        points = capitalise_first_word(points)  
        if not points:
            issues.append("Notable achievements format is invalid")
        fixed_data["notable_achievements"] = points

    # Hobbies
    hobbies = fixed_data.get("hobbies", "")
    if hobbies:
        points = split_bullet_points(hobbies)
        points = capitalise_first_word(points) 
        if not points:
            issues.append("Hobbies format is invalid")
        fixed_data["hobbies"] = points

    # Social links: validate and fix
    for key, pattern, prefix in [
        ("instagram_link", r"^https?://(www\.)?instagram\.com/.+", "https://instagram.com/"),
        ("linkedin_link", r"^https?://(www\.)?linkedin\.com/in/.+", "https://linkedin.com/in/"),
        ("github_link", r"^https?://(www\.)?github\.com/.+", "https://github.com/"),
    ]:
        val = fixed_data.get(key, "")
        if val:
            if not re.match(pattern, val):
                issues.append(f"{key.replace('_', ' ').title()} appears invalid")
                # Fix the link
                if "/" not in val:
                    fixed_data[key] = prefix + val.strip().lstrip("@")
                elif not val.startswith("http"):
                    fixed_data[key] = "https://" + val.strip().lstrip("/")

    return issues, fixed_data

def clean_text(text):
    """
    Cleans the input text by removing leading numbers, dashes, or periods from each line.
    """
    if not isinstance(text, str):
        return text
    lines = text.splitlines()
    cleaned_lines = [re.sub(r'^[\d\.\-\s]+', '', line.strip()) for line in lines]
    cleaned_text = '\n'.join(cleaned_lines).strip()
    return cleaned_text if cleaned_text else None

def capitalise_first_word(points: list[str]) -> list[str]:
    """
    Capitalises the first word of each bullet point.
    """
    def cap_first(s):
        s = s.strip()
        return s[:1].upper() + s[1:] if s else s
    return [cap_first(point) for point in points]

def clean_row_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in row.items():
        if isinstance(v, str):
            row[k] = clean_text(v)
            if k == "bachelor_course":
                row[k] = clean_bachelor_course(row[k])
        elif isinstance(v, list):
            row[k] = [clean_text(x) if isinstance(x, str) else x for x in v]
        elif pd.isna(v):
            row[k] = None
    return row

def read_profiles_file(file_bytes: bytes, filename: str) -> list[dict]:
    """
    Reads a CSV or XLSX file and returns list of dicts.
    Detects file type by extension.
    """
    ext = filename.lower().split('.')[-1]
    if ext == 'csv':
        # Try to detect encoding, fallback to utf-8-sig
        try:
            detection = chardet.detect(file_bytes)
            encoding = detection.get("encoding") or "utf-8-sig"
            text = file_bytes.decode(encoding)
        except Exception:
            text = file_bytes.decode('utf-8-sig')
        df = pd.read_csv(io.StringIO(text))
    elif ext == 'xlsx':
        df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
    else:
        raise ValueError("Unsupported file type!")
    # Convert DataFrame to list of dicts
    return df.to_dict(orient='records')

async def process_profiles_file(file_bytes: bytes, filename: str, upload_id: str):
    # Clear the staging table first
    names = [
        row["full_name"]
        for row in supabase.table("staging").select("full_name").execute().data
    ]
    if names:
        supabase.table("staging").delete().in_("full_name", names).execute()
    
    rows = read_profiles_file(file_bytes, filename)
    
    deduped = {}
    duplicate_rows = []
    for i, row in enumerate(rows):
        mapped_row = translate_row_keys(row)
        mapped_row = clean_row_fields(mapped_row)
        full_name = mapped_row.get("full_name")
        if not full_name:
            continue
        key = full_name.title()
        if key in deduped:
            duplicate_rows.append({
                "row_index": i,
                "duplicate_full_name": key,
                "duplicate_row_data": row,
                "kept_row_index": deduped[key].get("row_index", None),
                "kept_row_data": deduped[key],
            })
        mapped_row["row_index"] = i  # Store for tracking
        deduped[key] = mapped_row    # Always keep the latest

    for full_name, mapped_row in deduped.items():
        mapped_row["full_name"] = full_name
        mapped_row["last_modified"] = datetime.utcnow().isoformat()
        issues, fixed_row = detect_and_fix_issues(mapped_row)
        
        if isinstance(fixed_row.get("notable_achievements"), list):
            fixed_row["notable_achievements"] = fixed_row["notable_achievements"]
        if isinstance(fixed_row.get("hobbies"), list):
            fixed_row["hobbies"] = fixed_row["hobbies"]
        # Prepare row for staging
        if issues:
            staging_row = {k: v for k, v in fixed_row.items() if k in PROFILES_COLUMNS}
            staging_row["issues"] = issues
        else:
            staging_row = {k: v for k, v in fixed_row.items() if k in PROFILES_COLUMNS}
        supabase.table("staging").upsert(staging_row).execute()
    
    if duplicate_rows:
        print(f"{len(duplicate_rows)} duplicate rows dropped (kept the latest for each name):")
        for entry in duplicate_rows:
            print(f"Row {entry['row_index']} is a duplicate for full_name '{entry['duplicate_full_name']}' (original kept at row {entry['kept_row_index']})")
    else:
        print("No duplicate rows found.")
