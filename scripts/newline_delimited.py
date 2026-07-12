#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Any

SRC_PATH = Path("data/database_new_cleaned.json")
DST_PATH = Path("data/database_new_cleaned.json")


def convert_to_newline_delimited(node: Any) -> None:
    """
    Recursively walk the JSON-like structure and turn
    'notable_achievements' / 'interests_hobbies' lists into newline strings.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ("notable_achievements", "interests_hobbies") and isinstance(value, list):
                node[key] = "\n".join(value)
            else:
                convert_to_newline_delimited(value)
    elif isinstance(node, list):
        for item in node:
            convert_to_newline_delimited(item)


def main() -> None:
    if not SRC_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {SRC_PATH}")

    data = json.loads(SRC_PATH.read_text(encoding="utf-8"))
    convert_to_newline_delimited(data)

    DST_PATH.write_text(json.dumps(data, indent=4, ensure_ascii=False), encoding="utf-8")
    print(f"Converted lists to newline-delimited strings -> {DST_PATH}")


if __name__ == "__main__":
    main()