#!/usr/bin/env python3
import sys


status_code = 0


def in_venv():
    return sys.prefix != sys.base_prefix


if __name__ == "__main__":
    if in_venv():
        status_code += (1 << 0)
    print(status_code)

