#!/usr/bin/env python3
"""Minimal JSON-command Tool fixture for configured service hosts."""

import json
import sys


def main() -> None:
    request = json.load(sys.stdin)
    text = request["input"]["text"]
    if not isinstance(text, str):
        raise ValueError("input.text must be a string")
    json.dump({"text": text.upper()}, sys.stdout, ensure_ascii=False)


if __name__ == "__main__":
    main()
