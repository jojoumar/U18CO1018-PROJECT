#!/usr/bin/env python3
"""
process_failover_from_csv.py
Parses abu_isp_log.csv CSV for events containing FAILOVER and ROUTE,
creates processed/failover_timeline.csv with columns:
  failover_ts, route_update_ts, failover_seconds, failover_line, route_line
Also prints average/min/max failover time.
"""
import csv, os, datetime, statistics

LOG = "abu_isp_log.csv"
OUT = "processed/failover_timeline.csv"
os.makedirs("processed", exist_ok=True)

failovers = []
routes = []

with open(LOG, newline='', errors='ignore') as f:
    reader = csv.DictReader(f)
    for row in reader:
        ev = (row.get('event') or '').strip().upper()
        ts = row.get('timestamp','').strip()
        if not ts:
            continue
        try:
            t = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except:
            # try fallback
            t = datetime.datetime.fromisoformat(ts)
        if 'FAILOVER' in ev:
            failovers.append((t, row.get('details','').strip()))
        if 'ROUTE' in ev:
            routes.append((t, row.get('details','').strip()))

rows = []
for i, f in enumerate(failovers):
    if i < len(routes):
        r = routes[i]
        delta = (r[0] - f[0]).total_seconds()
        rows.append({
            'failover_ts': f[0].strftime("%Y-%m-%d %H:%M:%S"),
            'route_update_ts': r[0].strftime("%Y-%m-%d %H:%M:%S"),
            'failover_seconds': delta,
            'failover_line': f[1],
            'route_line': r[1]
        })

# write CSV
with open(OUT, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['failover_ts','route_update_ts','failover_seconds','failover_line','route_line'])
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

# stats
if rows:
    times = [r['failover_seconds'] for r in rows]
    print("Failover events:", len(times))
    print("Avg failover (s):", round(statistics.mean(times),3))
    print("Min failover (s):", round(min(times),3))
    print("Max failover (s):", round(max(times),3))
else:
    print("No paired FAILOVER/ROUTE events found in", LOG)

print("Wrote", OUT)
