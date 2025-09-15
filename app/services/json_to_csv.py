import json
import csv
import re
from datetime import datetime, timezone, timedelta
from app.database.supabase_client import supabase

INPUT_JSON = "./data/database.json"
OUTPUT_CSV = "./data/database_profiles.csv"
PROFILE_COLUMNS = [
    "full_name", "bachelor_course", "masters_course", "ddp_or_minor",
    "intake_batch", "overseas_experience", "self_writeup", "picture_url",
    "notable_achievements", "hobbies", "linkedin_link",
    "instagram_link", "github_link", "updated_at"
]
PROFILES_TABLE = "profiles"

def clear_profiles_table():
    # Delete all rows in the profiles table
    supabase.table(PROFILES_TABLE).delete().neq("full_name", "").execute()
    print("Cleared all rows in 'profiles' table.")

def insert_profiles_from_csv():
    with open(OUTPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        # Remove empty rows (if any)
        rows = [row for row in rows if row.get("full_name")]
        # Insert in batches if needed
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            supabase.table(PROFILES_TABLE).upsert(batch).execute()
    print(f"Inserted {len(rows)} rows into 'profiles' table.")

def split_bullet_points(text):
    """
    Split text into bullet points or lines.
    """
    if not text:
        return []
    # Split by newline, strip each line, ignore empty
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    # If only one line, try splitting by other separators
    if len(lines) == 1:
        single_line = lines[0]
        for sep in ['•', '*', '-', ';', ',', '.', '·']:
            if sep in single_line:
                lines = [segment.strip() for segment in single_line.split(sep) if segment.strip()]
                break
    # Remove existing bullet characters at start
    cleaned = [re.sub(r'^[\u2022\-\*\·]+\s*', '', line) for line in lines]
    return cleaned

def capitalise_first_word(points):
    """
    Capitalise only the first letter of each point.
    """
    def cap_first(s):
        s = s.strip()
        return s[:1].upper() + s[1:] if s else s
    return [cap_first(point) for point in points]

def extract_ay(admit_year):
    if not admit_year:
        return None
    match = re.search(r"AY\d{2}/\d{2}", str(admit_year))
    return match.group(0) if match else None

def main():
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for ay, faculties in data.items():
        for faculty, students in faculties.items():
            for student_id, entry in students.items():
                row = {}
                row["full_name"] = entry.get("name") or entry.get("full_name")
                row["bachelor_course"] = entry.get("major", "")
                row["masters_course"] = entry.get("masters_course", "")
                row["ddp_or_minor"] = entry.get("ddp_or_minor", "")
                row["intake_batch"] = extract_ay(entry.get("admit_year"))
                row["overseas_experience"] = entry.get("overseas_experience", "")
                row["self_writeup"] = entry.get("self_writeup") or entry.get("writeup", "")
                row["picture_url"] = entry.get("picture_url", "")
                
                # Notable Achievements
                notable = entry.get("notable_achievements", "")
                if isinstance(notable, list):
                    points = [str(x) for x in notable]
                else:
                    points = split_bullet_points(notable)
                points = capitalise_first_word(points)
                row["notable_achievements"] = json.dumps(points, ensure_ascii=False)

                # Hobbies
                hobbies = entry.get("interests_hobbies", "")
                if isinstance(hobbies, list):
                    points = [str(x) for x in hobbies]
                else:
                    points = split_bullet_points(hobbies)
                points = capitalise_first_word(points)
                row["hobbies"] = json.dumps(points, ensure_ascii=False)

                row["linkedin_link"] = entry.get("linkedin_link") or entry.get("linkedin_url", "")
                row["instagram_link"] = entry.get("instagram_link") or entry.get("instagram_url", "")
                row["github_link"] = entry.get("github_url", "")
                row["updated_at"] = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()

                filtered_row = {k: row.get(k, "") for k in PROFILE_COLUMNS}
                rows.append(filtered_row)

    # Export to CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PROFILE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    
    # Populate Supabase table
    clear_profiles_table()
    insert_profiles_from_csv()

if __name__ == "__main__":
    main()