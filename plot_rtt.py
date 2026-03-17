#!/usr/bin/env python3
"""
plot_rtt.py (improved)

Reads:
  - processed/ema_values.csv   (timestamp,ISP,EMA_raw,EMA_ms)
  - processed/rtt_summary.csv  (optional)

Produces:
  - plots/ema_timeseries.png
  - plots/avg_rtt_bar.png
  - updates processed/results_summary.csv (merges RTT averages; preserves other columns if present)

Notes:
- Normalizes common ISP name variants (Air -> Airtel, mtn -> MTN, glo -> Glo).
- Robust to missing input files: prints guidance and exits cleanly.
"""
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

IN_VALUES = "processed/ema_values.csv"
IN_SUM = "processed/rtt_summary.csv"
OUT_DIR = "plots"
PROCESSED_OUT = "processed/results_summary.csv"

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs("processed", exist_ok=True)

# Normalization mapping (lowercase keys)
ISP_NORMALIZE = {
    'air': 'Airtel',
    'airtel': 'Airtel',
    'mtn': 'MTN',
    'glo': 'Glo',
    'globe': 'Glo'
}

def normalize_isp(name):
    if not isinstance(name, str):
        return name
    k = name.strip().lower()
    return ISP_NORMALIZE.get(k, name.strip().capitalize())

# --- Load EMA values ---
if not os.path.exists(IN_VALUES):
    print(f"ERROR: {IN_VALUES} not found. Run scripts/extract_ema_from_csv.py first.", file=sys.stderr)
    sys.exit(1)

df = pd.read_csv(IN_VALUES, parse_dates=['timestamp'], infer_datetime_format=True)
# Normalize ISP labels
df['ISP'] = df['ISP'].apply(normalize_isp)

# Ensure EMA_ms numeric
df['EMA_ms'] = pd.to_numeric(df['EMA_ms'], errors='coerce')

# Drop rows with missing EMA_ms or timestamp
df = df.dropna(subset=['EMA_ms','timestamp'])

if df.empty:
    print("No EMA samples found after cleaning. Exiting.", file=sys.stderr)
    sys.exit(1)

# Sort by timestamp for plotting
df = df.sort_values('timestamp')

# --- Time-series plot ---
plt.figure(figsize=(10,5))
for isp, g in df.groupby('ISP'):
    # downsample for plotting if extremely dense
    series = g.set_index('timestamp')['EMA_ms']
    plt.plot(series.index, series.values, marker='o', linestyle='-', label=isp)
plt.xlabel('Time')
plt.ylabel('EMA RTT (ms)')
plt.title('EMA RTT over time per ISP')
plt.legend()
plt.grid(True)
plt.tight_layout()
ts_out = os.path.join(OUT_DIR, 'ema_timeseries.png')
plt.savefig(ts_out)
plt.close()
print("Wrote time-series plot:", ts_out)

# --- Average RTT summary ---
if os.path.exists(IN_SUM):
    df_sum = pd.read_csv(IN_SUM)
    # normalize ISP column if present
    if 'ISP' in df_sum.columns:
        df_sum['ISP'] = df_sum['ISP'].astype(str).apply(normalize_isp)
    # ensure column names we expect
    if 'avg_ema_ms' not in df_sum.columns and 'avg_ema' in df_sum.columns:
        df_sum = df_sum.rename(columns={'avg_ema':'avg_ema_ms'})
else:
    df_sum = df.groupby('ISP')['EMA_ms'].agg(['count','mean','std']).reset_index()
    df_sum = df_sum.rename(columns={'count':'samples','mean':'avg_ema_ms','std':'stddev_ms'})

# safe conversions/rounding
df_sum['avg_ema_ms'] = pd.to_numeric(df_sum['avg_ema_ms'], errors='coerce').round(3)
if 'samples' not in df_sum.columns:
    df_sum['samples'] = df.groupby('ISP').size().reindex(df_sum['ISP']).fillna(0).astype(int)

# --- Bar chart of averages ---
plt.figure(figsize=(6,4))
plt.bar(df_sum['ISP'], df_sum['avg_ema_ms'])
plt.xlabel('ISP')
plt.ylabel('Avg EMA RTT (ms)')
plt.title('Average EMA RTT per ISP')
plt.tight_layout()
bar_out = os.path.join(OUT_DIR, 'avg_rtt_bar.png')
plt.savefig(bar_out)
plt.close()
print("Wrote average RTT bar chart:", bar_out)

# --- Merge into results_summary.csv ---
# If results_summary exists, read and update RTT fields; otherwise create minimal table
if os.path.exists(PROCESSED_OUT):
    results = pd.read_csv(PROCESSED_OUT)
    # ensure ISP column exists
    if 'ISP' not in results.columns:
        print(f"Existing {PROCESSED_OUT} missing 'ISP' column; backing up and creating new summary.", file=sys.stderr)
        results.to_csv(PROCESSED_OUT + ".bak", index=False)
        results = pd.DataFrame(columns=['ISP'])
else:
    results = pd.DataFrame(columns=['ISP'])

# merge on ISP, prefer existing Throughput/Avg_Failover columns if present
merged = pd.merge(df_sum[['ISP','samples','avg_ema_ms']], results, on='ISP', how='outer', suffixes=('_rtt',''))
# cleanup: ensure samples and avg_ema_ms columns are present and numeric
merged['samples'] = merged['samples'].fillna(0).astype(int)
merged['avg_ema_ms'] = pd.to_numeric(merged['avg_ema_ms'], errors='coerce').round(3)
# preserve existing throughput/failover columns if they exist in original results
# If not present, create placeholders
if 'Throughput_Mbps' not in merged.columns:
    merged['Throughput_Mbps'] = ''
if 'Avg_Failover_s' not in merged.columns:
    merged['Avg_Failover_s'] = ''

# reorder columns for readability
cols_order = ['ISP','samples','avg_ema_ms','Throughput_Mbps','Avg_Failover_s']
for c in cols_order:
    if c not in merged.columns:
        merged[c] = ''
merged = merged[cols_order]

merged.to_csv(PROCESSED_OUT, index=False)
print("Wrote/updated:", PROCESSED_OUT)

print("\nDone. If you want a different plot style or to include confidence/error bars, tell me and I'll add it.")
