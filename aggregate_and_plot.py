#!/usr/bin/env python3
"""
aggregate_and_plot.py
Read processed CSVs and produce results_summary.csv and plots in plots/
Usage:
  python3 aggregate_and_plot.py --processed processed/ --plots plots/
"""

import os, argparse
import pandas as pd
import matplotlib.pyplot as plt
from utils import ensure_dir, safe_write_csv
import logging
logging.getLogger().setLevel(logging.INFO)

def main(processed_dir, plots_dir):
    ensure_dir(plots_dir)
    # Load files if they exist
    rtt_path = os.path.join(processed_dir, 'rtt_summary.csv')
    tp_path = os.path.join(processed_dir, 'throughput_summary.csv')
    fo_path = os.path.join(processed_dir, 'failover_timeline.csv')
    qos_path = os.path.join(processed_dir, 'qos_utilization.csv')

    # Build a basic summary DataFrame keyed by ISP
    summary_rows = []
    if os.path.exists(rtt_path):
        df_rtt = pd.read_csv(rtt_path)
        for _, row in df_rtt.iterrows():
            summary_rows.append({'ISP': row['ISP'], 'avg_rtt_ms': row['avg_ema_rtt_ms'], 'rtt_samples': int(row['samples'])})
    if os.path.exists(tp_path):
        df_tp = pd.read_csv(tp_path)
        # Attempt to infer ISP from filename (common naming pattern iperf_glo.txt etc.)
        for _, r in df_tp.iterrows():
            isp = 'Unknown'
            fn = r['file'].lower()
            if 'glo' in fn: isp='Glo'
            if 'mtn' in fn: isp='Mtn'
            if 'air' in fn or 'airt' in fn: isp='Airtel'
            # find summary row
            existing = next((x for x in summary_rows if x['ISP'].lower()==isp.lower()), None)
            if existing:
                existing['throughput_mbps'] = r['avg_mbps']
            else:
                summary_rows.append({'ISP': isp, 'throughput_mbps': r['avg_mbps']})
    if os.path.exists(fo_path):
        df_fo = pd.read_csv(fo_path)
        if not df_fo.empty:
            avg_fail = df_fo['failover_seconds'].mean()
        else:
            avg_fail = None
    else:
        avg_fail = None
    # QoS aggregated per class (not ISP)
    qos_summary = None
    if os.path.exists(qos_path):
        df_q = pd.read_csv(qos_path)
        qos_summary = df_q.groupby('classid')['utilization_percent'].agg(['mean','count']).reset_index()
        safe_write_csv(qos_summary, os.path.join(processed_dir, 'qos_summary.csv'))

    summary_df = pd.DataFrame(summary_rows).drop_duplicates(subset=['ISP'])
    safe_write_csv(summary_df, os.path.join(processed_dir, 'results_summary.csv'))
    logging.info("Wrote results_summary.csv")

    # Plots
    if not summary_df.empty:
        # Throughput bar
        if 'throughput_mbps' in summary_df:
            plt.figure()
            plotdf = summary_df[['ISP','throughput_mbps']].dropna()
            if not plotdf.empty:
                plt.bar(plotdf['ISP'], plotdf['throughput_mbps'])
                plt.title('Average Throughput per ISP (Mbps)')
                plt.xlabel('ISP')
                plt.ylabel('Mbps')
                plt.savefig(os.path.join(plots_dir, 'throughput_per_isp.png'))
                logging.info("Saved plot: throughput_per_isp.png")

        # RTT bar
        if 'avg_rtt_ms' in summary_df:
            plt.figure()
            plotdf = summary_df[['ISP','avg_rtt_ms']].dropna()
            if not plotdf.empty:
                plt.bar(plotdf['ISP'], plotdf['avg_rtt_ms'])
                plt.title('Average EMA RTT per ISP (ms)')
                plt.xlabel('ISP')
                plt.ylabel('ms')
                plt.savefig(os.path.join(plots_dir, 'rtt_per_isp.png'))
                logging.info("Saved plot: rtt_per_isp.png")

    # Failover timeline plot (if available)
    if os.path.exists(fo_path):
        df_fo = pd.read_csv(fo_path)
        if not df_fo.empty:
            # create simple bar of each failover duration
            plt.figure(figsize=(8,4))
            plt.bar(range(len(df_fo)), df_fo['failover_seconds'])
            plt.title('Failover durations (s)')
            plt.xlabel('Failover event index')
            plt.ylabel('Seconds')
            plt.savefig(os.path.join(plots_dir, 'failover_durations.png'))
            logging.info("Saved plot: failover_durations.png")

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--processed', default='processed')
    ap.add_argument('--plots', default='plots')
    args = ap.parse_args()
    main(args.processed, args.plots)
