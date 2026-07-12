#!/usr/bin/env python3
"""
Merge legacy database.json with database_new.json into database_merged.json.

- Works out of ./scripts, expects the JSON files in ../data.
- Renames legacy "major" -> "bachelors" and inserts "masters": null.
- Graduate buckets (e.g., "MS") are normalized to "masters".
- Masters entries in database_new.json are matched back to their original bucket
  in database.json (e.g., CEG) and merged there. If no prior record is found,
  they are skipped and reported.
- For overlapping profiles, each field is overwritten only when the new value
  is not None. last_updated refreshes when any field changes.
- New profiles with writeup == null are ignored unless they already existed in
  the legacy database.

Requirements: Python 3.8+
"""

import json
from copy import deepcopy
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
LEGACY_PATH = DATA_DIR / "database.json"
NEW_PATH = DATA_DIR / "database_new_cleaned.json"
OUTPUT_PATH = DATA_DIR / "database_merged.json"

MASTER_BUCKET_ALIASES = {"ms", "masters"}


def load_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Cannot find {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_bucket_name(bucket_key: str) -> str:
    if isinstance(bucket_key, str) and bucket_key.lower() in MASTER_BUCKET_ALIASES:
        return "masters"
    return bucket_key


def normalize_legacy(db):
    """
    Return a normalized deep copy where:
    - Graduate buckets collapse to "masters".
    - 'major' is renamed to 'bachelors'.
    - 'masters' always exists (default None).
    """
    normalized = {}
    if isinstance(db.get("last_updated"), str):
        normalized["last_updated"] = db["last_updated"]

    for ay_key, ay_dict in db.items():
        if ay_key == "last_updated" or not isinstance(ay_dict, dict):
            continue
        normalized.setdefault(ay_key, {})
        for bucket_key, bucket_dict in ay_dict.items():
            if not isinstance(bucket_dict, dict):
                continue
            bucket_name = normalize_bucket_name(bucket_key)
            normalized[ay_key].setdefault(bucket_name, {})
            for slug, profile in bucket_dict.items():
                profile_copy = deepcopy(profile)
                if "major" in profile_copy:
                    profile_copy["bachelors"] = profile_copy.pop("major")
                else:
                    profile_copy.setdefault("bachelors", None)
                profile_copy.setdefault("masters", None)
                normalized[ay_key][bucket_name][slug] = profile_copy
    return normalized


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
        if new_value is not None and old_profile.get(field) != new_value:
            old_profile[field] = new_value
            changed = True
    if changed and new_profile.get("last_updated"):
        old_profile["last_updated"] = new_profile["last_updated"]


def find_profile_location(db, slug):
    """
    Return (ay_key, bucket_key, bucket_dict) where slug resides, or None.
    """
    for ay_key, ay_dict in db.items():
        if ay_key == "last_updated" or not isinstance(ay_dict, dict):
            continue
        for bucket_key, bucket_dict in ay_dict.items():
            if isinstance(bucket_dict, dict) and slug in bucket_dict:
                return ay_key, bucket_key, bucket_dict
    return None


def clean_empty_structures(db):
    """
    Remove empty buckets/AYs created when migrating profiles.
    """
    empty_ays = []
    for ay_key, ay_dict in db.items():
        if ay_key == "last_updated" or not isinstance(ay_dict, dict):
            continue
        empty_buckets = [
            bucket for bucket, bucket_dict in ay_dict.items()
            if isinstance(bucket_dict, dict) and not bucket_dict
        ]
        for bucket in empty_buckets:
            del ay_dict[bucket]
        if not ay_dict:
            empty_ays.append(ay_key)
    for ay_key in empty_ays:
        del db[ay_key]


def main():
    legacy_db = load_json(LEGACY_PATH)
    new_db = load_json(NEW_PATH)

    merged = normalize_legacy(legacy_db)
    missing_masters = []
    skipped_null_writeups = []

    for ay_key, buckets in new_db.items():
        if ay_key == "last_updated" or not isinstance(buckets, dict):
            continue

        for raw_bucket_key, profiles in buckets.items():
            if not isinstance(profiles, dict):
                continue

            bucket_key = normalize_bucket_name(raw_bucket_key)

            if bucket_key == "masters":
                for slug, new_profile in profiles.items():
                    location = find_profile_location(merged, slug)
                    if location is None:
                        missing_masters.append({
                            "ay": ay_key,
                            "slug": slug,
                            "name": new_profile.get("name"),
                        })
                        continue
                    _, _, bucket_dict = location
                    merge_profiles(bucket_dict[slug], new_profile)
                continue

            dest_bucket = ensure_nested(merged, ay_key, bucket_key)
            for slug, new_profile in profiles.items():
                if slug not in dest_bucket:
                    location = find_profile_location(merged, slug)
                    if location is None and new_profile.get("writeup") is None:
                        skipped_null_writeups.append({
                            "ay": ay_key,
                            "bucket": bucket_key,
                            "slug": slug,
                            "name": new_profile.get("name"),
                        })
                        continue
                    dest_bucket[slug] = deepcopy(new_profile)
                else:
                    merge_profiles(dest_bucket[slug], new_profile)

    clean_empty_structures(merged)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=4)

    print(f"Merged database written to {OUTPUT_PATH}")

    if missing_masters:
        print("\nMasters profiles missing prior entries (not merged):")
        for item in missing_masters:
            print(f"  - {item['name'] or item['slug']} (AY: {item['ay']})")

    if skipped_null_writeups:
        print("\nProfiles skipped due to null writeups and no legacy record:")
        for item in skipped_null_writeups:
            label = item["name"] or item["slug"]
            print(f"  - {label} (AY: {item['ay']}, Bucket: {item['bucket']})")


if __name__ == "__main__":
    main()