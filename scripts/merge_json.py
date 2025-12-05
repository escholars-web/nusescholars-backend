#!/usr/bin/env python3
"""
Merge legacy database.json with database_new.json into database_merged.json.

- Works out of ./scripts, expects the JSON files in ../data.
- Renames legacy "major" -> "bachelors" and inserts "masters": null.
- When the same profile exists in both files, overwrite each field only if
  the new value is not null. Update last_updated when any field actually changes.

Requirements: Python 3.8+
"""

import json
from copy import deepcopy
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
LEGACY_PATH = DATA_DIR / "database.json"
NEW_PATH = DATA_DIR / "database_new.json"
OUTPUT_PATH = DATA_DIR / "database_merged.json"


def load_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Cannot find {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_legacy(db):
    """
    Return a deep-copied structure where:
    - 'major' is renamed to 'bachelors'
    - 'masters' is added (default None)
    """
    legacy_copy = deepcopy(db)
    for ay_key, ay_dict in legacy_copy.items():
        if ay_key == "last_updated" or not isinstance(ay_dict, dict):
            continue
        for bucket_key, bucket_dict in ay_dict.items():
            if not isinstance(bucket_dict, dict):
                continue
            for profile in bucket_dict.values():
                if "major" in profile:
                    profile["bachelors"] = profile.pop("major")
                else:
                    profile.setdefault("bachelors", None)
                profile.setdefault("masters", None)
    return legacy_copy


def ensure_nested(container, ay_key, bucket_key):
    return container.setdefault(ay_key, {}).setdefault(bucket_key, {})


def merge_profiles(old_profile, new_profile):
    """
    Update old_profile in place:
      - For each field (except last_updated), replace only if new value is not None.
      - Track if any field changed; if so, update last_updated to new_profile's value (when provided).
    """
    changed = False
    for field, new_value in new_profile.items():
        if field == "last_updated":
            continue
        if new_value is not None:
            if old_profile.get(field) != new_value:
                old_profile[field] = new_value
                changed = True
    if changed and new_profile.get("last_updated"):
        old_profile["last_updated"] = new_profile["last_updated"]


def main():
    legacy_db = load_json(LEGACY_PATH)
    new_db = load_json(NEW_PATH)

    merged = normalize_legacy(legacy_db)

    for ay_key, buckets in new_db.items():
        for bucket_key, profiles in buckets.items():
            dest_bucket = ensure_nested(merged, ay_key, bucket_key)
            for slug, new_profile in profiles.items():
                if slug not in dest_bucket:
                    dest_bucket[slug] = deepcopy(new_profile)
                else:
                    merge_profiles(dest_bucket[slug], new_profile)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=4)

    print(f"Merged database written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()