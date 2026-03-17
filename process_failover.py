#!/usr/bin/env python3
"""
process_failover.py

Robust pairing of [FAILOVER] -> next [ROUTE] events from abu_isp_log.csv.
Writes: processed/failover_timeline.csv

Behavior:
 - Parses CSV-style abu_isp_log.csv with columns including 'timestamp','event','details'.
 - Extracts FAILOVER and ROUTE (case-insensitive).
 - Sorts events chronologically and pairs each FAILOVER with the first ROUTE whose timestamp >= failover_ts.
 - If ISP names appear in details (e.g. "Glo -> MTN" or "MTN -> Airtel"), the script will prefer ROUTE events
   whose details mention the same ISP name (best-effort).
 - Outputs summary statistics and writes rows with failover_ts, route_update_ts, delta (s), and detail text.
"""
import csv, os, statistics, sys, re
from dateutil import parser as dparser
from datetime import datetime

LOG = "abu_isp_log.csv"
OUT = "processed/failover_timeline.csv"
os.makedirs("processed", exist_ok=True)

def parse_ts(ts_str):
    if not ts_str:
        return None
    try:
        return dparser.parse(ts_str)
    except Exception:
        # fallback: try basic formats
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(ts_str.strip(), fmt)
            except Exception:
                continue
    return None

def extract_isp_names(detail):
    """
    Heuristic: extract ISP-like tokens from details, e.g. "Glo", "MTN", "Airtel", "None" etc.
    Returns set of normalized lower-case tokens.
    """
    if not detail:
        return set()
    # pick alpha tokens of length 2..12 (avoid long sentences)
    toks = re.findall(r'\b([A-Za-z]{2,12})\b', detail)
    # normalize common variants
    norm = {t.strip().lower() for t in toks if t.strip()}
    # map simple variants (optional)
    mapped = set()
    for t in norm:
        if t in ('air','airtel','airtel'):
            mapped.add('airtel')
        elif t in ('glo','globe'):
            mapped.add('glo')
        elif t in ('mtn',):
            mapped.add('mtn')
        elif t in ('none',):
            mapped.add('none')
        else:
            mapped.add(t)
    return mapped

# Read CSV-style log
if not os.path.exists(LOG):
    print("ERROR: log file not found:", LOG, file=sys.stderr)
    sys.exit(1)

failovers = []  # list of tuples (ts, details, rownum)
routes = []     # list of tuples (ts, details, rownum)

with open(LOG, newline='', errors='ignore') as f:
    # try to detect if it's CSV with header
    first = f.readline()
    f.seek(0)
    # if header contains 'timestamp' and 'event' assume CSV
    if 'timestamp' in first.lower() and 'event' in first.lower():
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            ev = (row.get('event') or '').strip()
            tsraw = (row.get('timestamp') or '').strip()
            det = (row.get('details') or '').strip()
            if not ev:
                continue
            ts = parse_ts(tsraw)
            if ts is None:
                # some logs may have timestamp in first token; try splitting first token of rowstring
                continue
            up = ev.upper()
            if 'FAILOVER' in up:
                failovers.append((ts, det, i))
            elif 'ROUTE' in up:
                routes.append((ts, det, i))
    else:
        # fallback: parse lines starting with timestamp, then comma, then event
        lines = f.read().splitlines()
        for i, line in enumerate(lines, start=1):
            parts = line.split(',', 2)
            if len(parts) < 2:
                continue
            tsraw = parts[0].strip()
            ev = parts[1].strip()
            det = parts[2].strip() if len(parts) >=3 else ''
            ts = parse_ts(tsraw)
            if ts is None:
                continue
            up = ev.upper()
            if 'FAILOVER' in up:
                failovers.append((ts, det, i))
            elif 'ROUTE' in up:
                routes.append((ts, det, i))

# sort chronologically
failovers.sort(key=lambda x: x[0])
routes.sort(key=lambda x: x[0])

# pair each failover with earliest route ts >= failover_ts
results = []
r_idx = 0
used_routes = set()

for f_ts, f_det, f_row in failovers:
    # collect candidate routes with ts >= f_ts
    candidate_idx = None
    candidate = None
    # first pass: prefer route that mentions same ISP token (best-effort)
    f_isps = extract_isp_names(f_det)
    # scan routes from current r_idx onwards to find earliest eligible
    for j in range(r_idx, len(routes)):
        r_ts, r_det, r_row = routes[j]
        if r_ts < f_ts:
            continue
        # if ISP tokens present, prefer matching tokens
        if f_isps:
            r_isps = extract_isp_names(r_det)
            if f_isps & r_isps:
                candidate_idx = j
                candidate = (r_ts, r_det, r_row)
                break
        # otherwise, pick first route with ts >= failover
        if candidate is None:
            candidate_idx = j
            candidate = (r_ts, r_det, r_row)
            break
    if candidate:
        r_ts, r_det, r_row = candidate
        delta = (r_ts - f_ts).total_seconds()
        # advance r_idx to candidate_idx+1 to avoid reusing earlier routes
        if candidate_idx is not None:
            r_idx = candidate_idx + 1
            used_routes.add(candidate_idx)
        results.append({
            'failover_ts': f_ts.isoformat(sep=' ', timespec='milliseconds'),
            'route_update_ts': r_ts.isoformat(sep=' ', timespec='milliseconds'),
            'failover_seconds': round(delta, 3),
            'failover_row': f_row,
            'route_row': r_row,
            'failover_details': f_det,
            'route_details': r_det
        })
    else:
        # no matching route found after this failover
        results.append({
            'failover_ts': f_ts.isoformat(sep=' ', timespec='milliseconds'),
            'route_update_ts': '',
            'failover_seconds': '',
            'failover_row': f_row,
            'route_row': '',
            'failover_details': f_det,
            'route_details': ''
        })

# write CSV
fieldnames = ['failover_ts','route_update_ts','failover_seconds','failover_row','route_row','failover_details','route_details']
with open(OUT, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for r in results:
        writer.writerow(r)

# summary
matched = [r['failover_seconds'] for r in results if isinstance(r['failover_seconds'], (int,float))]
unmatched = [r for r in results if not isinstance(r['failover_seconds'], (int,float))]
print("Failover events (total):", len(failovers))
print("Route events (total):", len(routes))
print("Matched pairs:", len(matched))
print("Unmatched failovers (no following route):", len(unmatched))
if matched:
    print("Avg failover (s):", round(statistics.mean(matched),3))
    print("Min failover (s):", round(min(matched),3))
    print("Max failover (s):", round(max(matched),3))
else:
    print("No matched failovers/routes found.")
print("Wrote", OUT)

# diagnostics for suspicious values
if matched:
    big = [x for x in matched if x > 5.0]  # threshold for "large"
    if big:
        print("Warning: found", len(big), "failovers >5s (examples):", big[:5])
