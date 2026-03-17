#!/usr/bin/env python3
"""
process_throughput_improved.py

Parse iperf3 plain-text output log files and produce:
 - processed/throughput_summary.csv   (one row per file)
 - processed/throughput_by_isp.csv    (aggregated per ISP)

Usage:
  python3 scripts/process_throughput_improved.py --dir logs --out processed
"""
import os, re, argparse, logging
import pandas as pd
from math import isfinite

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

RECV_RE = re.compile(r'([\d\.]+)\s+(Kbits/sec|Mbits/sec|Gbits/sec)\s+receiver', re.IGNORECASE)
UNIT_MULT = {
    'Kbits/sec': 1.0/1024.0,   # convert Kbits -> Mbits
    'Mbits/sec': 1.0,
    'Gbits/sec': 1024.0
}

def parse_iperf_file(path):
    """Return list of measured Mbps values parsed from iperf3 text output."""
    try:
        text = open(path, 'r', errors='ignore').read()
    except Exception as e:
        logging.warning("Failed to read %s: %s", path, e)
        return []
    matches = RECV_RE.findall(text)
    bw_list = []
    for val, unit in matches:
        try:
            bw = float(val) * UNIT_MULT.get(unit, 1.0)
            if isfinite(bw):
                bw_list.append(round(bw, 6))
        except Exception:
            continue
    return bw_list

def infer_isp_from_name(fname):
    """Heuristic mapping from filename to ISP label (case-insensitive)."""
    n = fname.lower()
    if 'glo' in n:
        return 'Glo'
    if 'mtn' in n:
        return 'MTN'
    if 'air' in n or 'airtel' in n:
        return 'Airtel'
    # fallback: try iperf_h1_192.168.1.10 style or bind ip
    m = re.search(r'192\.168\.(\d+)\.10', fname)
    if m:
        octet = m.group(1)
        if octet == '1':
            return 'Glo'
        if octet == '2':
            return 'MTN'
        if octet == '3':
            return 'Airtel'
    return 'Unknown'

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def main(indir, outdir):
    ensure_dir(outdir)
    files = sorted([f for f in os.listdir(indir) if f.lower().endswith('.txt')])
    if not files:
        logging.error("No .txt files found in %s", indir)
        return

    rows = []
    for fname in files:
        # consider files named like iperf_glo.txt, iperf_mtn.txt, iperf_airtel.txt
        path = os.path.join(indir, fname)
        bws = parse_iperf_file(path)
        isp = infer_isp_from_name(fname)
        if not bws:
            logging.info("No receiver lines in %s (skipping)", fname)
            rows.append({
                'file': fname,
                'ISP': isp,
                'samples': 0,
                'avg_mbps': '',
                'min_mbps': '',
                'max_mbps': ''
            })
            continue
        avg = round(sum(bws)/len(bws), 3)
        mn = round(min(bws), 3)
        mx = round(max(bws), 3)
        rows.append({
            'file': fname,
            'ISP': isp,
            'samples': len(bws),
            'avg_mbps': avg,
            'min_mbps': mn,
            'max_mbps': mx
        })
        logging.info("%s -> ISP=%s: avg=%.3f Mbps (n=%d)", fname, isp, avg, len(bws))

    df = pd.DataFrame(rows)
    summary_path = os.path.join(outdir, 'throughput_summary.csv')
    df.to_csv(summary_path, index=False)
    logging.info("Wrote: %s", summary_path)

    # Aggregate per ISP (ignore blank avg_mbps)
    df_valid = df[df['avg_mbps']!=''].copy()
    if not df_valid.empty:
        df_valid['avg_mbps'] = pd.to_numeric(df_valid['avg_mbps'])
        agg = df_valid.groupby('ISP').agg(
            files=('file','count'),
            samples_total=('samples','sum'),
            avg_mbps_mean=('avg_mbps','mean'),
            avg_mbps_min=('avg_mbps','min'),
            avg_mbps_max=('avg_mbps','max')
        ).reset_index()
        # round numeric columns
        agg[['avg_mbps_mean','avg_mbps_min','avg_mbps_max']] = agg[['avg_mbps_mean','avg_mbps_min','avg_mbps_max']].round(3)
        byisp_path = os.path.join(outdir, 'throughput_by_isp.csv')
        agg.to_csv(byisp_path, index=False)
        logging.info("Wrote: %s", byisp_path)
        print("\nPer-ISP summary:")
        print(agg.to_string(index=False))
    else:
        logging.warning("No valid throughput samples found to aggregate per ISP.")

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', default='logs', help='Directory with iperf logs')
    ap.add_argument('--out', '--outdir', dest='outdir', default='processed')
    args = ap.parse_args()
    main(args.dir, args.outdir)
