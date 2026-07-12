#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Dict, Any

MAJOR_ORDER = [
    "MPE",
    "EEE",
    "BME",
    "ESP",
    "EVE",
    "ISE",
    "MLE",
    "CEG",
    "CHE",
    "CVE",
    "IPM",
    "DS",
    "masters",
]

def order_key(major_key: str) -> int:
    try:
        return MAJOR_ORDER.index(major_key)
    except ValueError:
        # place unlisted majors after the known ones, preserving alphabetical order
        return len(MAJOR_ORDER)

def sort_majors_in_batch(batch_data: Dict[str, Any]) -> Dict[str, Any]:
    sorted_items = sorted(
        batch_data.items(),
        key=lambda kv: (order_key(kv[0]), kv[0])
    )
    return {k: v for k, v in sorted_items}

def main():
    src_path = Path("data/database_merged.json")
    dst_path = Path("data/database_sorted.json")

    data = json.loads(src_path.read_text(encoding="utf-8"))

    for batch_name, batch_payload in data.items():
        if not isinstance(batch_payload, dict) or batch_name == "last_updated":
            continue
        data[batch_name] = sort_majors_in_batch(batch_payload)

    dst_path.write_text(json.dumps(data, indent=4, ensure_ascii=False), encoding="utf-8")
    print(f"Sorted majors written to {dst_path}")

if __name__ == "__main__":
    main()