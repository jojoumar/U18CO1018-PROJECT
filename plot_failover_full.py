#!/usr/bin/env python3
"""
plot_failover_full.py

Reads processed/failover_timeline.csv and produces:
  - plots/failover_histogram.png
  - plots/failover_timeline.png
  - plots/failover_ecdf.png

Requires: pandas, matplotlib, numpy
"""
import os, sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

IN = "processed/failover_timeline.csv"
OUT_DIR = "plots"
os.makedirs(OUT_DIR, exist_ok=True)

if not os.path.exists(IN):
    print(f"ERROR: {IN} not found. Run process_failover.py first.", file=sys.stderr)
    sys.exit(1)

df = pd.read_csv(IN)
# coerce numeric
df['failover_seconds'] = pd.to_numeric(df['failover_seconds'], errors='coerce')
# drop missing
df_clean = df.dropna(subset=['failover_seconds']).copy()
if df_clean.empty:
    print("No numeric failover_seconds found. Exiting.", file=sys.stderr)
    sys.exit(1)

# convert failover_ts to datetime if possible
if 'failover_ts' in df_clean.columns:
    df_clean['t'] = pd.to_datetime(df_clean['failover_ts'], errors='coerce')
else:
    df_clean['t'] = pd.NaT

# ========== Histogram ==========
plt.figure(figsize=(6,4))
bins = np.histogram_bin_edges(df_clean['failover_seconds'], bins='auto')
plt.hist(df_clean['failover_seconds'], bins=bins, edgecolor='black', alpha=0.75)
plt.xlabel('Failover time (s)')
plt.ylabel('Count')
plt.title('Distribution of Failover Times (N={})'.format(len(df_clean)))
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
hist_file = os.path.join(OUT_DIR, 'failover_histogram.png')
plt.savefig(hist_file, dpi=150)
plt.close()
print("Wrote", hist_file)

# ========== Timeline (scatter + moving average) ==========
plt.figure(figsize=(10,4))
# if timestamps exist use them on x axis; otherwise use index
if df_clean['t'].notnull().any():
    x = df_clean['t']
    plt.scatter(x, df_clean['failover_seconds'], s=30, alpha=0.8)
    # moving average (window in number of points)
    window = max(3, int(len(df_clean)/10))
    ma = df_clean['failover_seconds'].rolling(window=window, min_periods=1, center=True).mean()
    plt.plot(x, ma, linestyle='--', linewidth=1.2, color='orange', alpha=0.9, label=f'{window}-pt MA')
    plt.gcf().autofmt_xdate()
else:
    x = np.arange(len(df_clean))
    plt.scatter(x, df_clean['failover_seconds'], s=30, alpha=0.8)
    ma = df_clean['failover_seconds'].rolling(window=max(3,int(len(df_clean)/10)), min_periods=1, center=True).mean()
    plt.plot(x, ma, linestyle='--', linewidth=1.2, color='orange', alpha=0.9)

plt.xlabel('Time')
plt.ylabel('Failover time (s)')
plt.title('Failover time vs time (N={})'.format(len(df_clean)))
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
timeline_file = os.path.join(OUT_DIR, 'failover_timeline.png')
plt.savefig(timeline_file, dpi=150)
plt.close()
print("Wrote", timeline_file)

# ========== ECDF (empirical CDF) ==========
vals = np.sort(df_clean['failover_seconds'].values)
y = np.arange(1, len(vals)+1) / len(vals)
plt.figure(figsize=(6,4))
plt.step(vals, y, where='post')
plt.xlabel('Failover time (s)')
plt.ylabel('Fraction of events ≤ x')
plt.title('ECDF of Failover Times (N={})'.format(len(vals)))
plt.grid(alpha=0.3)
plt.tight_layout()
ecdf_file = os.path.join(OUT_DIR, 'failover_ecdf.png')
plt.savefig(ecdf_file, dpi=150)
plt.close()
print("Wrote", ecdf_file)

# ========== Save a small numeric summary (optional append) ==========
summary_file = os.path.join('processed','failover_summary_stats.csv')
s = df_clean['failover_seconds'].describe()
s_df = pd.DataFrame(s).transpose()
s_df.to_csv(summary_file, index=False)
print("Wrote stats:", summary_file)

