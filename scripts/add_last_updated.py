# Script to add 'last_updated' field to each individual in the database
# python -m scripts.add_last_updated "2024-09-30"

import json
from datetime import datetime
import sys

INPUT_JSON = "./data/database.json"
OUTPUT_JSON = "./data/database.json"

def add_last_updated(last_updated_date: str):
    """
    Add last_updated field to each individual with a specified date.
    
    Args:
        last_updated_date: Date string in format 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS'
    
    Raises:
        ValueError: If date format is invalid
    """
    
    # Validate and parse the date
    try:
        # Try ISO format with time first
        if 'T' in last_updated_date:
            datetime.fromisoformat(last_updated_date)
            parsed_date = last_updated_date
        else:
            # Try YYYY-MM-DD format
            datetime.strptime(last_updated_date, '%Y-%m-%d')
            parsed_date = last_updated_date
    except ValueError:
        raise ValueError(
            f"Invalid date format: '{last_updated_date}'. "
            "Please use 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS' format."
        )
    
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    count = 0
    for ay, faculties in data.items():
        for faculty, students in faculties.items():
            for student_id, entry in students.items():
                entry["last_updated"] = parsed_date
                count += 1
    
    # Write back to the same file
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    print(f"Successfully added 'last_updated' field to {count} individuals.")
    print(f"Date set to: {parsed_date}")
    print(f"Updated database saved to {OUTPUT_JSON}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.add_last_updated <date>")
        print("Date format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")
        print("Example: python -m scripts.add_last_updated 2024-09-30")
        sys.exit(1)
    
    date_arg = sys.argv[1]
    add_last_updated(date_arg)
