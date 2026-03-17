#!/usr/bin/env python3
"""
process_qos.py (improved)

- Accepts one or two tc snapshot files (--tcfiles). If two are provided, and bytes are present,
  measured_mbps is computed from delta_bytes / interval. If only one file is provided, the script
  falls back to parsing rate/ceil tokens heuristically.
- Accepts a JSON mapping --class_ceils to provide configured ceilings when tc text lacks them.
- Outputs: processed/qos_utilization.csv and processed/qos_raw_parsed.csv

Usage examples:
  # single snapshot, with manual class ceilings
  python3 scripts/process_qos.py --tcfiles logs/qos_tc_stats.txt --class_ceils '{"1:10":1000,"1:20":1000}' 

  # two snapshots separated by interval seconds -> use delta bytes calculation
  python3 scripts/process_qos.py --tcfiles logs/tc1.txt logs/tc2.txt --interval 5 --class_ceils '{"1:10":1000}'
"""
import argparse, os, re, json, csv
from collections import defaultdict
import pandas as pd
import logging
from math import isfinite

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

CLASS_RE = re.compile(r'class\s+\S+\s+([\d:]+)', re.IGNORECASE)
# find Sent X bytes OR bytes: X
SENT_RE = re.compile(r'[Ss]ent[:\s]+([\d,]+)\s+bytes|([\d,]+)\s+bytes', re.IGNORECASE)
# rate/ceil patterns: 'rate 1000Mbit' or 'ceil 10Gbit' or tokens like '10Gbit'
RATE_RE = re.compile(r'(?:rate|ceil)?\s*[:=]?\s*([\d\.]+)\s*([KMGkmg])?bit', re.IGNORECASE)
UNIT_MULT = {'k': 1.0/1000.0, 'm': 1.0, 'g': 1000.0, '': 1.0}

OUT_DIR_DEFAULT = "processed"
os.makedirs(OUT_DIR_DEFAULT, exist_ok=True)

def normalize_classid(cid):
    return cid.strip()

def parse_snapshot(path):
    """Return dict classid -> {sent_bytes: int|None, parsed_ceil_mbps: float|None, raw:block}"""
    out = {}
    text = open(path, 'r', errors='ignore').read()
    # split into class blocks (heuristic)
    blocks = re.split(r'\n(?=class )', text)
    for b in blocks:
        m = CLASS_RE.search(b)
        if not m:
            continue
        cid = normalize_classid(m.group(1))
        sent = None
        # try to find direct 'Sent X bytes' style first
        ms = re.search(r'[Ss]ent[:\s]+([\d,]+)\s+bytes', b)
        if ms:
            sent = int(ms.group(1).replace(',', ''))
        else:
            # fallback: first 'N bytes' occurrence in the block
            m2 = re.search(r'([\d,]+)\s+bytes', b)
            if m2:
                sent = int(m2.group(1).replace(',',''))
        # find ceil/rate tokens
        ceil = None
        for m3 in RATE_RE.finditer(b):
            val = float(m3.group(1))
            unit = (m3.group(2) or '').lower()
            ceil = val * UNIT_MULT.get(unit, 1.0)
            # prefer larger token (if many present keep last/most explicit)
        out[cid] = {'sent_bytes': sent, 'parsed_ceil_mbps': ceil, 'raw': b.strip()}
    return out

def compute_from_two_snapshots(t1, t2, interval_s, class_ceils):
    # class list
    classes = sorted(set(list(t1.keys()) + list(t2.keys())))
    rows = []
    for cid in classes:
        v1 = t1.get(cid, {})
        v2 = t2.get(cid, {})
        s1 = v1.get('sent_bytes')
        s2 = v2.get('sent_bytes')
        measured = None
        if s1 is not None and s2 is not None:
            delta = max(0, s2 - s1)
            measured = (delta * 8.0) / 1e6 / float(interval_s)  # Mbit/s
            measured = round(measured, 3)
        # ceiling preference: CLI provided map > parsed ceil
        ceil = None
        if class_ceils and cid in class_ceils:
            ceil = float(class_ceils[cid])
        else:
            ceil = v2.get('parsed_ceil_mbps') or v1.get('parsed_ceil_mbps')
        util = ''
        if measured is not None and ceil not in (None, 0, ''):
            try:
                util = round((measured/float(ceil))*100.0, 3)
            except Exception:
                util = ''
        rows.append({
            'classid': cid,
            'sent_t1_bytes': s1 if s1 is not None else '',
            'sent_t2_bytes': s2 if s2 is not None else '',
            'measured_mbps': measured if measured is not None else '',
            'ceil_mbps': round(ceil,3) if ceil not in (None,'') else '',
            'utilization_percent': util
        })
    return rows

def compute_from_single_snapshot(t1, class_ceils):
    # Use parsed rate tokens (parsed_ceil_mbps) as measured value if no bytes available
    classes = sorted(list(t1.keys()))
    rows = []
    for cid in classes:
        v = t1.get(cid, {})
        measured = v.get('parsed_ceil_mbps')  # this may be the only info available
        ceil = None
        if class_ceils and cid in class_ceils:
            ceil = float(class_ceils[cid])
        else:
            ceil = v.get('parsed_ceil_mbps')
        util = ''
        if measured not in (None, '') and ceil not in (None, '') and ceil != 0:
            util = round((measured/float(ceil))*100.0,3) if ceil else ''
        rows.append({
            'classid': cid,
            'sent_t1_bytes': v.get('sent_bytes',''),
            'sent_t2_bytes': '',
            'measured_mbps': round(measured,3) if measured not in (None,'') else '',
            'ceil_mbps': round(ceil,3) if ceil not in (None,'') else '',
            'utilization_percent': util
        })
    return rows

def main(tcfiles, interval, outdir, class_ceils):
    ensure_dir(outdir := outdir or OUT_DIR_DEFAULT)
    # parse provided class ceilings map
    class_map = class_ceils or {}
    # parse snapshots
    if len(tcfiles) == 0:
        logging.error("No tcfiles provided.")
        return
    if len(tcfiles) == 1:
        t1 = parse_snapshot(tcfiles[0])
        rows = compute_from_single_snapshot(t1, class_map)
        # write raw parsed
        raw_out = os.path.join(outdir, 'qos_raw_parsed.csv')
        with open(raw_out, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['classid','t1_sent_bytes','parsed_ceil_mbps','raw_block'])
            for cid, v in sorted(t1.items()):
                w.writerow([cid, v.get('sent_bytes',''), v.get('parsed_ceil_mbps',''), v.get('raw','')])
    else:
        # use first two snapshots only (caller should supply t1 then t2)
        t1 = parse_snapshot(tcfiles[0])
        t2 = parse_snapshot(tcfiles[1])
        if not interval:
            logging.error("Two snapshots given but no --interval provided. Aborting.")
            return
        rows = compute_from_two_snapshots(t1, t2, interval, class_map)
        # write raw parsed
        raw_out = os.path.join(outdir, 'qos_raw_parsed.csv')
        with open(raw_out, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['classid','t1_sent_bytes','t2_sent_bytes','t1_raw','t2_raw'])
            classes = sorted(set(list(t1.keys()) + list(t2.keys())))
            for cid in classes:
                w.writerow([cid, t1.get(cid,{}).get('sent_bytes',''), t2.get(cid,{}).get('sent_bytes',''),
                            t1.get(cid,{}).get('raw',''), t2.get(cid,{}).get('raw','')])
    # write utilization CSV
    out_vals = os.path.join(outdir, 'qos_utilization.csv')
    df = pd.DataFrame(rows)
    df.to_csv(out_vals, index=False)
    logging.info("Wrote: %s", out_vals)
    # small human summary
    logging.info("Summary (per class):")
    for r in rows:
        logging.info(" %s : measured=%s Mbps  ceil=%s Mbps  util=%s%%", r['classid'], r['measured_mbps'], r['ceil_mbps'], r['utilization_percent'])

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--tcfiles', nargs='+', required=True, help='One or two tc snapshot files (t1 [t2])')
    ap.add_argument('--interval', type=float, default=0.0, help='Seconds between snapshots (required if two files)')
    ap.add_argument('--outdir', default='processed')
    ap.add_argument('--class_ceils', default='{}', help='JSON map classid->ceil_mbps e.g. \'{"1:10":1000}\'')
    args = ap.parse_args()
    try:
        class_ceils = json.loads(args.class_ceils)
    except Exception:
        logging.error("Failed to parse --class_ceils JSON. Using empty map.")
        class_ceils = {}
    main(args.tcfiles, args.interval, args.outdir, class_ceils)
