#!/usr/bin/env python3
"""
plot_qos.py - plot results in processed/qos_utilization.csv
Produces:
 - plots/qos_utilization_bar.png  (measured vs configured)
 - plots/qos_utilization_percent.png (util %)
"""
import os, sys, pandas as pd, matplotlib.pyplot as plt
os.makedirs("plots", exist_ok=True)

infile = "processed/qos_utilization.csv"
if not os.path.exists(infile):
    print("No processed/qos_utilization.csv found. Run scripts/process_qos.py first.")
    sys.exit(1)

df = pd.read_csv(infile)
df['measured_mbps'] = pd.to_numeric(df['measured_mbps'], errors='coerce')
df['ceil_mbps'] = pd.to_numeric(df['ceil_mbps'], errors='coerce')
df['utilization_percent'] = pd.to_numeric(df['utilization_percent'], errors='coerce')

x = df['classid'].astype(str)

# Bar: measured vs ceil
plt.figure(figsize=(8,4))
plt.bar(x, df['ceil_mbps'].fillna(0), label='Ceil (Mbps)', alpha=0.6)
plt.bar(x, df['measured_mbps'].fillna(0), label='Measured (Mbps)', alpha=0.9)
plt.ylabel('Mbps')
plt.title('Measured throughput vs configured ceil per class')
plt.legend()
plt.tight_layout()
plt.savefig('plots/qos_utilization_bar.png')
plt.close()
print("Wrote plots/qos_utilization_bar.png")

# Bar: utilization %
plt.figure(figsize=(8,4))
plt.bar(x, df['utilization_percent'].fillna(0))
plt.ylabel('Utilization (%)')
plt.title('Utilization percent per class')
plt.tight_layout()
plt.savefig('plots/qos_utilization_percent.png')
plt.close()
print("Wrote plots/qos_utilization_percent.png")
