#!/usr/bin/env python3
"""
extract_ema_from_csv.py (improved)

Reads abu_isp_log.csv (CSV with header timestamp,event,details),
extracts rows where event contains 'HC', parses EMA values from details,
normalizes ISP names (Air -> Airtel), and writes:
  - logs/ema_raw.txt         (raw HC lines, tab-separated)
  - processed/ema_values.csv (timestamp,ISP,EMA_raw,EMA_ms)
  - processed/rtt_summary.csv (ISP,samples,avg_ema_ms,stddev_ms)

Notes:
- If EMA value < 1, treat as seconds and convert to milliseconds (x1000).
- Otherwise assume it's already in milliseconds.
- Normalization mapping can be extended in ISP_NORMALIZE.
"""
import csv, os, re, statistics, sys

LOG = "abu_isp_log.csv"
OUT_LOG_RAW = "logs/ema_raw.txt"
OUT_DIR = "processed"
OUT_VALS = os.path.join(OUT_DIR, "ema_values.csv")
OUT_SUM = os.path.join(OUT_DIR, "rtt_summary.csv")

os.makedirs("logs", exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

# Normalize ISP labels here (case-insensitive keys)
ISP_NORMALIZE = {
    'air': 'Airtel',
    'airtel': 'Airtel',
    'mtn': 'MTN',
    'glo': 'Glo',
    'globe': 'Glo'
}

# Pattern to find segments like "Glo:gw=True inet=True ema=0.1" or "Airtel:...ema=0.1"
isp_pattern = re.compile(r'([A-Za-z]+)[^|]*?ema=([0-9]+(?:\.[0-9]+)?)', re.IGNORECASE)

hc_rows = []

# Read CSV and collect HC rows
if not os.path.exists(LOG):
    print(f"ERROR: {LOG} not found. Run controller to generate it.", file=sys.stderr)
    sys.exit(1)

with open(LOG, newline='', errors='ignore') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        event = (row.get('event') or "").strip()
        if not event:
            continue
        if 'HC' in event.upper():  # covers 'HC' or variations
            hc_rows.append(row)

# Write raw HC lines for traceability (tab-separated)
with open(OUT_LOG_RAW, 'w') as f:
    for r in hc_rows:
        f.write(f"{r.get('timestamp','')}\t{r.get('event','')}\t{r.get('details','')}\n")

# Parse EMA entries and normalize ISPs
values = []      # list of tuples (timestamp, isp_normalized, ema_raw, ema_ms)
isp_buckets = {} # isp -> [ema_ms, ...]

for r in hc_rows:
    ts = (r.get('timestamp','') or "").strip()
    details = (r.get('details','') or "")
    # Split on " | " or just search repeatedly
    # Use finditer to catch repeated occurrences
    for m in isp_pattern.finditer(details):
        isp_raw = m.group(1).strip()
        ema_raw_str = m.group(2)
        # normalize ISP
        isp_key = isp_raw.lower()
        isp_norm = ISP_NORMALIZE.get(isp_key, isp_raw.capitalize())
        try:
            ema_raw = float(ema_raw_str)
        except:
            continue
        # convert if < 1 (assume seconds)
        if ema_raw < 1.0:
            ema_ms = ema_raw * 1000.0
        else:
            ema_ms = ema_raw
        ema_ms = round(ema_ms, 3)
        values.append((ts, isp_norm, ema_raw, ema_ms))
        isp_buckets.setdefault(isp_norm, []).append(ema_ms)

# Write processed per-sample CSV
with open(OUT_VALS, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp","ISP","EMA_raw","EMA_ms"])
    for ts, isp, ema_raw, ema_ms in values:
        writer.writerow([ts, isp, ema_raw, ema_ms])

# Write summary CSV (avg and stddev)
with open(OUT_SUM, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["ISP","samples","avg_ema_ms","stddev_ms"])
    for isp, lst in sorted(isp_buckets.items()):
        samples = len(lst)
        avg = round(statistics.mean(lst),3) if samples else 0.0
        std = round(statistics.pstdev(lst),3) if samples > 1 else 0.0
        writer.writerow([isp, samples, avg, std])

# Print diagnostics
print("Wrote:", OUT_LOG_RAW, OUT_VALS, OUT_SUM)
print("Processed HC rows:", len(hc_rows))
print("Extracted EMA samples:", len(values))
print("ISPs found and sample counts:")
for isp, lst in sorted(isp_buckets.items()):
    print(f" - {isp}: {len(lst)} samples, avg {round(statistics.mean(lst),3) if len(lst) else 0.0} ms")

