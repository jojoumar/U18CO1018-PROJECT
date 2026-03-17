#!/usr/bin/env python3
"""
scripts/parse_qos_stats.py
Parse raw logs/qos_utilization.csv into processed/qos_parsed.csv and computed utilization.

Usage:
  python3 scripts/parse_qos_stats.py \
    --input logs/qos_utilization.csv \
    --outdir processed \
    --class-ceils '{"1:1":10000, "1:10":1000}'   # configured ceilings in Mbps (optional)

If you prefer units in the raw file (e.g., "10Gbit"), you can omit --class-ceils
and the script will still extract the numeric rates when present.
"""
import os, argparse, csv, json, re

UNIT_MULT = {
    'kbit': 1.0/1024.0,
    'kbits': 1.0/1024.0,
    'mbit': 1.0,
    'mbits': 1.0,
    'gbit': 1024.0,
    'gbits': 1024.0
}

rate_re = re.compile(r'([0-9]+(?:\.[0-9]+)?)\s*([KkMmGg][bB]it)s?$')

def parse_rate_token(tok):
    """Return rate in Mbps if token encodes a rate like '10Gbit' or '500Kbit'. Otherwise None."""
    if not tok:
        return None
    m = rate_re.search(tok)
    if not m:
        return None
    val = float(m.group(1))
    unit = m.group(2).lower()
    mult = UNIT_MULT.get(unit, None)
    if mult is None:
        # unknown unit - try crude fallback
        return val
    return val * mult

def is_number(s):
    try:
        float(s)
        return True
    except:
        return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', default='logs/qos_utilization.csv')
    ap.add_argument('--outdir', default='processed')
    ap.add_argument('--class-ceils', default='{}', help='JSON map e.g. \'{"1:1":100,"1:10":10}\' (Mbps)')
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    class_ceils = json.loads(args.class_ceils)

    parsed_rows = []
    with open(args.input, 'r', errors='ignore') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            # Trim whitespace from each token
            tokens = [t.strip() for t in row if t is not None]
            # Best-effort: classid is first token
            classid = tokens[0] if len(tokens) >= 1 else ''
            # gather candidates for rate and measured
            rate_mbps = None
            measured_mbps = None
            extra = ''
            if len(tokens) >= 2:
                # try parse second as rate
                r = parse_rate_token(tokens[1])
                if r is not None:
                    rate_mbps = r
                else:
                    # might be flag like 'prio' or textual; keep as extra
                    extra = tokens[1]
            if len(tokens) >= 3:
                # third token may be 'ceil' keyword or numeric (measured)
                if is_number(tokens[2]):
                    measured_mbps = float(tokens[2])
                else:
                    r2 = parse_rate_token(tokens[2])
                    if r2 is not None:
                        rate_mbps = r2
                    else:
                        # tokens like 'ceil' or 'prio'
                        extra = (extra + ' ' + tokens[2]).strip() if extra else tokens[2]

            # fallback: if no explicit rate but class_ceils provided, use it
            configured_mbps = class_ceils.get(classid, None)
            utilization = None
            if measured_mbps is not None and configured_mbps:
                try:
                    utilization = (measured_mbps / float(configured_mbps)) * 100.0
                except:
                    utilization = None
            parsed_rows.append({
                'classid': classid,
                'raw_tokens': '|'.join(tokens),
                'configured_mbps': configured_mbps if configured_mbps is not None else (rate_mbps if rate_mbps is not None else ''),
                'measured_mbps': measured_mbps if measured_mbps is not None else '',
                'utilization_percent': round(utilization,3) if utilization is not None else ''
            })

    out_parsed = os.path.join(args.outdir, 'qos_parsed.csv')
    out_util = os.path.join(args.outdir, 'qos_utilization.csv')

    with open(out_parsed, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['classid','raw_tokens','configured_mbps','measured_mbps','utilization_percent'])
        writer.writeheader()
        for r in parsed_rows:
            writer.writerow(r)

    # also write a simpler utilization file
    with open(out_util, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['classid','configured_mbps','measured_mbps','utilization_percent'])
        for r in parsed_rows:
            writer.writerow([r['classid'], r['configured_mbps'], r['measured_mbps'], r['utilization_percent']])

    print("Wrote:", out_parsed, out_util)
    print("Tip: pass --class-ceils '{\"1:1\":10000}' to compute utilization if you know configured ceilings (Mbps).")

if __name__ == '__main__':
    main()
