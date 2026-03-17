#!/usr/bin/env python3
"""
plot_throughput_improved.py
Reads processed/throughput_by_isp.csv and produces:
 - plots/throughput_bar.png
 - plots/throughput_box.png (if multiple files per ISP)
"""
import os, sys
import pandas as pd
import matplotlib.pyplot as plt

IN = "processed/throughput_by_isp.csv"
IN_FILE = "processed/throughput_summary.csv"
OUT_DIR = "plots"
os.makedirs(OUT_DIR, exist_ok=True)

if not os.path.exists(IN) and not os.path.exists(IN_FILE):
    print("No throughput CSVs found. Run process_throughput_improved.py first.", file=sys.stderr)
    sys.exit(1)

# Prefer aggregated by-ISP CSV, else compute basic from per-file
if os.path.exists(IN):
    df = pd.read_csv(IN)
    # columns: ISP, files, samples_total, avg_mbps_mean, avg_mbps_min, avg_mbps_max
    plt.figure(figsize=(6,4))
    plt.bar(df['ISP'], df['avg_mbps_mean'])
    plt.ylabel('Average Throughput (Mbps)')
    plt.xlabel('ISP')
    plt.title('Average Throughput per ISP')
    plt.tight_layout()
    out = os.path.join(OUT_DIR, 'throughput_bar.png')
    plt.savefig(out, dpi=150)
    plt.close()
    print("Wrote", out)

# boxplot from per-file values if available
if os.path.exists(IN_FILE):
    dfp = pd.read_csv(IN_FILE)
    # filter valid rows
    dfp = dfp[dfp['avg_mbps']!='']
    if not dfp.empty:
        dfp['avg_mbps'] = pd.to_numeric(dfp['avg_mbps'])
        groups = dfp.groupby('ISP')['avg_mbps'].apply(list)
        if len(groups) > 0:
            plt.figure(figsize=(6,4))
            plt.boxplot(groups.tolist(), labels=groups.index)
            plt.ylabel('Per-file average throughput (Mbps)')
            plt.xlabel('ISP')
            plt.title('Throughput spread by ISP')
            plt.tight_layout()
            out2 = os.path.join(OUT_DIR, 'throughput_box.png')
            plt.savefig(out2, dpi=150)
            plt.close()
            print("Wrote", out2)
