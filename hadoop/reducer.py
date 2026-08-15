#!/usr/bin/env python3
"""Hadoop Streaming reducer — sum counts for sorted mapper keys."""

import sys


def main():
    current = None
    total = 0
    for raw in sys.stdin:
        line = raw.rstrip("\n")
        if not line:
            continue
        key, _, value = line.partition("\t")
        try:
            n = int(value or "1")
        except ValueError:
            n = 1
        if current is None:
            current = key
        if key != current:
            sys.stdout.write(f"{current}\t{total}\n")
            current = key
            total = 0
        total += n
    if current is not None:
        sys.stdout.write(f"{current}\t{total}\n")


if __name__ == "__main__":
    main()
