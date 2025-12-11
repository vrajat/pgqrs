import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv("results/section2_throughput.csv")

def plot_messages(batch):
    sub = df[df.batch == batch]
    labels = sub.scenario.tolist()
    vals = sub.messages.tolist()
    plt.xkcd()
    fig, ax = plt.subplots(figsize=(6,4))
    ax.bar(labels, vals)
    ax.set_ylabel("Messages processed")
    ax.set_title(f"Throughput: naive vs FOR UPDATE SKIP LOCKED (batch={batch})")
    for i, v in enumerate(vals):
        ax.text(i, v + max(vals)*0.01, f"{v:,}", ha='center')
    fig.tight_layout()
    fig.savefig(f"messages_batch{batch}_xkcd.png")
    plt.close(fig)

def plot_latency(batch):
    sub = df[df.batch == batch]
    labels = sub.scenario.tolist()
    vals = sub.median_ms.tolist()
    plt.xkcd()
    fig, ax = plt.subplots(figsize=(6,4))
    ax.bar(labels, vals)
    ax.set_ylabel("Median latency (ms)")
    ax.set_title(f"Median latency: naive vs FOR UPDATE SKIP LOCKED (batch={batch})")
    for i, v in enumerate(vals):
        ax.text(i, v + max(vals)*0.01, f"{v:.1f} ms", ha='center')
    fig.tight_layout()
    fig.savefig(f"latency_batch{batch}_xkcd.png")
    plt.close(fig)

for b in sorted(df.batch.unique()):
    plot_messages(b)
    plot_latency(b)
