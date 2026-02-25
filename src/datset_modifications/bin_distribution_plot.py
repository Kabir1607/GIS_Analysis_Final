"""
Bar graph of bin population from the binned Dataset 1.
"""

import csv
import os
from collections import Counter

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = r"c:\Users\Kdixter\Desktop\GIS_Analysis_Final\raw_data\dataset_1"
BINNED_PATH = os.path.join(BASE, "dataset_1_binned.csv")
OUTPUT_PATH = os.path.join(BASE, "bin_distribution.png")

# Count points per bin
bin_counts = Counter()
with open(BINNED_PATH, "r", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        bin_counts[row["bin"]] += 1

# Sort descending
sorted_bins = sorted(bin_counts.items(), key=lambda x: -x[1])
names = [b[0] for b in sorted_bins]
counts = [b[1] for b in sorted_bins]
total = sum(counts)

# Colors
colors = ["#2d6a4f", "#52b788", "#3a86ff", "#e63946", "#ff9f1c", "#a7c957"]

fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.bar(names, counts, color=colors[:len(names)], edgecolor="white", linewidth=0.8)

for bar, count in zip(bars, counts):
    pct = count / total * 100
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + total * 0.008,
            f"{count:,}\n({pct:.1f}%)", ha="center", va="bottom", fontsize=10, fontweight="bold")

ax.set_ylabel("Number of Points", fontsize=12)
ax.set_xlabel("Bin", fontsize=12)
ax.set_title("Dataset 1 — Bin Distribution (2024 data only)", fontsize=14, fontweight="bold")
ax.grid(axis="y", alpha=0.3, linestyle="--")
ax.set_ylim(0, max(counts) * 1.15)

plt.tight_layout()
plt.savefig(OUTPUT_PATH, dpi=150, bbox_inches="tight")
print(f"Saved to: {OUTPUT_PATH}")
