#!/usr/bin/env python3
"""
process_rtt.py
Extract [HC] lines from abu_isp_log.csv, parse EMA RTT per ISP, output cleaned CSVs and a summary.
Usage:
  python3 process_rtt.py --log logs/abu_isp_log.csv --outdir processed/
"""

import re
import argparse
import pandas as pd
from utils import parse_timestamp, ensure_dir, safe_write_csv
import logging

logging.getLogger().setLevel(logging.INFO)

HC_PATTERN = re.compile(r'^\s*(?P<ts>[\d\-\s:]+)\s*,.*\[HC\].*')

def extract_ema_fields(text):
    """
    Given the field portion with ISP info, returns dict of EMA floats e.g.
    "Glo: rtt=34.5ms | ema=32.1ms | stdev=1.2ms ; MTN: ... "
    We'll match patterns like 'Glo:.*?ema=(\d+(\.\d+)?)'
    """
    records = {}
    for isp in ['Glo', 'MTN', 'Airtel', 'GLO', 'MTN', 'AIRTEL']:
        m = re.search(rf'{isp}.*?ema\s*=\s*([0-9]+(?:\.[0-9]+)?)', text, re.IGNORECASE)
        if m:
            records[isp.capitalize()] = float(m.group(1))
    return records

def main(log, outdir):
    ensure_dir(outdir)
    lines = []
    with open(log, 'r', errors='ignore') as f:
        lines = f.readlines()

    rows = []
    for line in lines:
        m = HC_PATTERN.search(line)
        if not m:
            continue
        ts = parse_timestamp(m.group('ts'))
        # entire line may have ISP segment after the "[HC]" tag; find everything after [HC]
        rest = line.split('[HC]',1)[-1]
        ema_map = extract_ema_fields(rest)
        if ema_map:
            for isp, ema in ema_map.items():
                rows.append({'timestamp': ts, 'ISP': isp, 'EMA_RTT_ms': ema, 'raw_line': line.strip()})

    if not rows:
        logging.warning("No [HC] EMA rows found in %s", log)
        return

    df = pd.DataFrame(rows)
    # drop any rows with NaT
    df = df.dropna(subset=['timestamp'])
    # Convert timestamp to string in desired standardized format
    df['timestamp'] = df['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
    ema_values_csv = outdir.rstrip('/') + '/ema_values.csv'
    safe_write_csv(df, ema_values_csv)

    # produce cleaned RTT summary aggregated per ISP (average)
    summary = df.groupby('ISP')['EMA_RTT_ms'].agg(['count','mean','std']).reset_index()
    summary = summary.rename(columns={'count':'samples','mean':'avg_ema_rtt_ms','std':'stddev_ms'})
    safe_write_csv(summary, outdir.rstrip('/') + '/rtt_summary.csv')
    logging.info("Processed RTT EMAs -> %s , %s", ema_values_csv, outdir.rstrip('/') + '/rtt_summary.csv')

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--log', required=True, help='Path to abu_isp_log.csv')
    ap.add_argument('--outdir', default='processed', help='Output directory')
    args = ap.parse_args()
    main(args.log, args.outdir)
