#!/usr/bin/env python3
"""
utils.py
Shared helpers for log parsing, time parsing, safe file ops.
"""

import os
import re
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

TIME_FMT = "%Y-%m-%d %H:%M:%S"

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def parse_timestamp(ts_str):
    """
    Parse timestamps found in logs. Expects format 'YYYY-MM-DD HH:MM:SS' (no fractional seconds).
    Returns datetime or None.
    """
    try:
        return datetime.strptime(ts_str.strip(), TIME_FMT)
    except Exception:
        # try trimming any fractional seconds
        ts = re.sub(r"\.\d+", "", ts_str)
        try:
            return datetime.strptime(ts.strip(), TIME_FMT)
        except Exception:
            return None

def read_text_lines(path):
    with open(path, 'r', errors='ignore') as f:
        return [line.rstrip('\n') for line in f]

def safe_write_csv(df, path):
    ensure_dir(os.path.dirname(path) or ".")
    df.to_csv(path, index=False)
    logging.info("Wrote CSV: %s", path)
