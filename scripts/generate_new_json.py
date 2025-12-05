#!/usr/bin/env python3
"""
Transform D&E-Scholars census data into database_new.json.

Place this script in ./scripts and the data files in ../data.

Requirements:
    pip install pandas openpyxl
"""

import json
import re
from pathlib import Path

import pandas as pd

# ---------------- user-configurable constants ---------------- #

LAST_UPDATED = "2025-12-05"  # <-- set desired ISO date here
DEDUP_KEY = "Email"          # column used to detect duplicates

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
INPUT_JSON = DATA_DIR / "database.json"
INPUT_XLSX = DATA_DIR / "D&E-Scholars Census AY25_26(1-103).xlsx"
OUTPUT_JSON = DATA_DIR / "database_new.json"

REMOVE_BLANK_WRITEUPS = False

# ---------------- helper functions ---------------- #

def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    return re.sub(r'\s+', '-', text)

def clean(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    return value or None

def split_lines(value):
    value = clean(value)
    if not value:
        return None
    parts = [line.strip(" -*•") for line in value.splitlines() if line.strip(" -*•")]
    return parts or None

def derive_ay_components(year_of_admission):
    """
    Returns (key, display) where:
        key     -> e.g. "ay25-26" (used for top-level dict keys)
        display -> e.g. "AY25/26" (stored inside each profile)
    """
    year = clean(year_of_admission)
    if not year:
        return "unknown", None

    match = re.search(r'(AY\d{2}/\d{2})', year.upper())
    if not match:
        return "unknown", None

    display = match.group(1)
    key = display.lower().replace('/', '-')
    return key, display

def raw_bachelors(row):
    return clean(row.get("B.Eng. Major"))

def extract_bachelors(row):
    raw = raw_bachelors(row)
    if raw:
        cleaned = re.sub(r'^[A-Z&\s]+-\s*', '', raw)
        cleaned = cleaned.rstrip(';').strip()
        if cleaned:
            return cleaned
    return clean(row.get("Major (in full)"))

def derive_bucket(row):
    raw = raw_bachelors(row)
    if raw:
        match = re.match(r'^([A-Z]{2,4})', raw)
        if match:
            return match.group(1)
    return "misc"

def extract_masters(row):
    academic_career = clean(row.get("Academic Career"))
    if academic_career != "E-Scholars Graduate":
        return None
    degree = clean(row.get("Degree Classification"))
    major_full = clean(row.get("Major (in full)"))
    if degree and major_full:
        return f"{degree} in {major_full}"
    return None

def get_name(row):
    return clean(row.get("Name")) or clean(row.get("Full Name (as per NRIC)"))

# ---------------- main script ---------------- #

def main():
    if not INPUT_XLSX.exists():
        raise FileNotFoundError(f"Excel file not found: {INPUT_XLSX}")

    df = pd.read_excel(INPUT_XLSX, sheet_name="Sheet1", engine="openpyxl")

    duplicates = df[df.duplicated(subset=[DEDUP_KEY], keep=False)]
    if not duplicates.empty:
        print("Duplicate rows detected (showing key columns):")
        print(duplicates[[DEDUP_KEY, "Name", "Completion time"]])
        print("-" * 60)

    df = df.drop_duplicates(subset=[DEDUP_KEY], keep="first")

    new_db = {}
    removed_profiles = []

    for _, row in df.iterrows():
        name = get_name(row)
        if not name:
            continue

        ay_key, admit_display = derive_ay_components(row.get("Year of Admission"))
        writeup = clean(row.get("Please provide a short write-up of yourself."))
        if REMOVE_BLANK_WRITEUPS and not writeup:
            removed_profiles.append((name, admit_display))
            continue  # skip adding this profile
        elif not writeup:
            # keep the profile, but set writeup to None
            writeup = None

        bucket = derive_bucket(row)
        slug = slugify(name)

        profile = {
            "name": name,
            "admit_year": admit_display,
            "academic_career": clean(row.get("Academic Career")),
            "bachelors": extract_bachelors(row),
            "masters": extract_masters(row),
            "writeup": writeup,
            "picture_url": clean(row.get("Upload a picture of yourself.")),
            "notable_achievements": split_lines(row.get("Notable Achievements (max 3)")),
            "interests_hobbies": split_lines(row.get("Any interests/ hobbies (max 3)")),
            "linkedin_url": clean(row.get("Linkedin Profile URL")),
            "instagram_url": clean(row.get("Instagram Profile URL")),
            "github_url": clean(row.get("Github Profile URL")),
            "last_updated": LAST_UPDATED,
        }

        new_db.setdefault(ay_key, {}).setdefault(bucket, {})[slug] = profile

    # Save JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(new_db, f, ensure_ascii=False, indent=4)

    print(f"Wrote {OUTPUT_JSON}")

    if REMOVE_BLANK_WRITEUPS and removed_profiles:
        print("\nProfiles removed due to blank write-up:")
        for name, admit in removed_profiles:
            print(f"  - {name} ({admit or 'Unknown AY'})")
    elif REMOVE_BLANK_WRITEUPS:
        print("\nNo profiles removed for blank write-ups.")

if __name__ == "__main__":
    main()