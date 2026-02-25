"""
Generate a bar graph of label distributions for Dataset 1.
Uses the class descriptions from the dataset itself as labels.
Saves the plot to raw_data/dataset_1/.
"""

import csv
import os
from collections import Counter

try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use("Agg")
except ImportError:
    print("matplotlib not found. Installing...")
    import subprocess
    subprocess.check_call(["pip", "install", "matplotlib"])
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

BASE = r"c:\Users\Kdixter\Desktop\GIS_Analysis_Final\raw_data"
DS1_PATH = os.path.join(BASE, "dataset_1", "Arunachal_ground_points - Point.csv")
OUTPUT_PATH = os.path.join(BASE, "dataset_1", "label_distribution.png")

# Read dataset
label_counts = Counter()
label_to_desc = {}

with open(DS1_PATH, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        label = row.get("label", "").strip()
        cls = row.get("class", "").strip()
        desc = row.get("class description", "").strip()
        if label:
            label_counts[label] += 1
            if label not in label_to_desc:
                label_to_desc[label] = f"{label} - {cls} ({desc})" if desc else f"{label} - {cls}"

# Sort by label number
sorted_labels = sorted(label_counts.keys(), key=lambda x: int(x) if x.isdigit() else 0)
counts = [label_counts[l] for l in sorted_labels]
bar_labels = [label_to_desc.get(l, l) for l in sorted_labels]

# Create the bar graph
fig, ax = plt.subplots(figsize=(16, 8))

colors = plt.cm.viridis([i / len(sorted_labels) for i in range(len(sorted_labels))])
bars = ax.bar(range(len(sorted_labels)), counts, color=colors, edgecolor="white", linewidth=0.5)

# Add count labels on top of each bar
for bar, count in zip(bars, counts):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(counts) * 0.01,
            f"{count:,}", ha="center", va="bottom", fontsize=8, fontweight="bold")

ax.set_xticks(range(len(sorted_labels)))
ax.set_xticklabels(bar_labels, rotation=45, ha="right", fontsize=9)
ax.set_ylabel("Number of Points", fontsize=12)
ax.set_xlabel("Label - Class (Description)", fontsize=12)
ax.set_title("Dataset 1: Label Distribution", fontsize=14, fontweight="bold")
ax.grid(axis="y", alpha=0.3, linestyle="--")

plt.tight_layout()
plt.savefig(OUTPUT_PATH, dpi=150, bbox_inches="tight")
print(f"Bar graph saved to: {OUTPUT_PATH}")
print(f"\nLabel counts:")
for label in sorted_labels:
    print(f"  {label_to_desc.get(label, label):<45} {label_counts[label]:>6,}")
print(f"\nTotal points: {sum(counts):,}")
