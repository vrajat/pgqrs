#!/usr/bin/env python3
# Generate xkcd-style WAL charts from section3_wal_data.csv
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

CSV = "section3_wal_data.csv"
OUT_DIR = Path(".")
df = pd.read_csv(CSV)

# Choose which batch to focus on. Recommended: batch=10 highlights the largest relative WAL reduction.
DEFAULT_BATCH = 10

def safe_str(label):
    return str(label).replace("_", " ")

def plot_wal_bytes(batch):
    sub = df[df['batch'] == batch].copy().reset_index(drop=True)
    if sub.empty:
        print(f"No rows for batch={batch}")
        return
    labels = [safe_str(s) for s, b in zip(sub['scenario'], sub['batch'])]
    wal = sub['wal_bytes'].astype(int).tolist()
    colors = ['tab:orange' if 'logged' in s else 'tab:blue' for s in sub['scenario']]

    plt.xkcd()
    fig, ax = plt.subplots(figsize=(9,4))
    x = np.arange(len(labels))
    ax.bar(x, wal)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha='right')
    ax.set_ylabel("WAL bytes")
    ax.set_title(f"WAL bytes per run (batch={batch})")
    # Add numeric labels
    maxv = max(wal) if wal else 1
    for i, v in enumerate(wal):
        ax.text(i, v + maxv*0.01, f"{v:,}", ha='center', fontsize=8)
    fig.tight_layout()
    out = OUT_DIR / f"wal_bytes_batch{batch}_xkcd.png"
    fig.savefig(out, dpi=200)
    fig.savefig(OUT_DIR / f"wal_bytes_batch{batch}_xkcd.svg")
    plt.close(fig)
    print("Saved", out)

def plot_wal_ppm(batch):
    sub = df[df['batch'] == batch].copy().reset_index(drop=True)
    if sub.empty:
        print(f"No rows for batch={batch}")
        return
    labels = [safe_str(s) for s, b in zip(sub['scenario'], sub['batch'])]
    wal_ppm = sub['wal_per_million'].astype(float).tolist()
    colors = ['tab:orange' if 'logged' in s else 'tab:blue' for s in sub['scenario']]

    plt.xkcd()
    fig, ax = plt.subplots(figsize=(9,4))
    x = np.arange(len(labels))
    ax.bar(x, wal_ppm)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha='right')
    ax.set_ylabel("WAL bytes per 1M messages")
    ax.set_title(f"WAL per million messages (batch={batch})")
    maxv = max(wal_ppm) if wal_ppm else 1
    for i, v in enumerate(wal_ppm):
        ax.text(i, v + maxv*0.01, f"{int(v):,}", ha='center', fontsize=8)
    fig.tight_layout()
    out = OUT_DIR / f"wal_ppm_batch{batch}_xkcd.png"
    fig.savefig(out, dpi=200)
    fig.savefig(OUT_DIR / f"wal_ppm_batch{batch}_xkcd.svg")
    plt.close(fig)
    print("Saved", out)

def plot_msgs_per_second(batch):
    sub = df[df['batch'] == batch].copy().reset_index(drop=True)
    if sub.empty:
        print(f"No rows for batch={batch}")
        return
    labels = [safe_str(s) + f" (b{int(b)})" for s, b in zip(sub['scenario'], sub['batch'])]
    # approximate messages/sec = reqs_per_s * batch
    msgs_per_s = (sub['reqs_per_s'] * sub['batch']).astype(float).tolist()
    colors = ['tab:orange' if 'logged' in s else 'tab:blue' for s in sub['scenario']]

    plt.xkcd()
    fig, ax = plt.subplots(figsize=(9,4))
    x = np.arange(len(labels))
    ax.bar(x, msgs_per_s, color=colors)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha='right')
    ax.set_ylabel("Approx messages/sec")
    ax.set_title(f"Approx messages/sec (batch={batch})")
    maxv = max(msgs_per_s) if msgs_per_s else 1
    for i, v in enumerate(msgs_per_s):
        ax.text(i, v + maxv*0.01, f"{v:.1f}", ha='center', fontsize=8)
    fig.tight_layout()
    out = OUT_DIR / f"msgs_per_sec_batch{batch}_xkcd.png"
    fig.savefig(out, dpi=200)
    fig.savefig(OUT_DIR / f"msgs_per_sec_batch{batch}_xkcd.svg")
    plt.close(fig)
    print("Saved", out)

if __name__ == "__main__":
    # Default: batch that best shows the relative WAL reduction
    batch_to_plot = DEFAULT_BATCH
    print("Available batches in CSV:", sorted(df['batch'].unique()))
    plot_wal_bytes(batch_to_plot)
    plot_wal_ppm(batch_to_plot)
    plot_msgs_per_second(batch_to_plot)

    # If you want to generate for both batches, uncomment below:
    # for b in sorted(df['batch'].unique()):
    #     plot_wal_bytes(b); plot_wal_ppm(b); plot_msgs_per_second(b)
