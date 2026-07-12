#!/usr/bin/env python3
import json
import re
from pathlib import Path
from typing import Any, Dict

WORD_PATTERN = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]+('[A-Za-z]+)?")

def to_title_case(value: str) -> str:
    def repl(match: re.Match) -> str:
        word = match.group(0)
        return word[0].upper() + word[1:].lower()
    return WORD_PATTERN.sub(repl, value)

def normalise_names(node: Any) -> None:
    if isinstance(node, dict):
        if "name" in node and isinstance(node["name"], str):
            node["name"] = to_title_case(node["name"])
        for child in node.values():
            normalise_names(child)
    elif isinstance(node, list):
        for item in node:
            normalise_names(item)

def main():
    src_path = Path("data/database_new.json")
    dst_path = Path("data/database_new_cleaned.json")

    data = json.loads(src_path.read_text(encoding="utf-8"))
    normalise_names(data)

    dst_path.write_text(json.dumps(data, indent=4, ensure_ascii=False), encoding="utf-8")
    print(f"Names cleaned and written to {dst_path}")

if __name__ == "__main__":
    main()