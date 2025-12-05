# Script to clean admit_year field to only include AY##/## format
# Run: python scripts/clean_admit_year.py

import json
import re

INPUT_JSON = "./data/database.json"
OUTPUT_JSON = "./data/database.json"

def extract_ay(admit_year):
    """Extract AY##/## pattern from admit_year string."""
    if not admit_year:
        return None
    match = re.search(r"AY\d{2}/\d{2}", str(admit_year))
    return match.group(0) if match else None

def clean_admit_years():
    """Clean admit_year field for all individuals."""
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    count = 0
    for ay, faculties in data.items():
        if ay == "last_updated":
            continue
        for faculty, students in faculties.items():
            for student_id, entry in students.items():
                old_admit_year = entry.get("admit_year")
                if old_admit_year:
                    cleaned = extract_ay(old_admit_year)
                    if cleaned:
                        entry["admit_year"] = cleaned
                        count += 1
    
    # Write back to the same file
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    print(f"Successfully cleaned {count} admit_year entries.")
    print(f"Updated database saved to {OUTPUT_JSON}")

if __name__ == "__main__":
    clean_admit_years()
