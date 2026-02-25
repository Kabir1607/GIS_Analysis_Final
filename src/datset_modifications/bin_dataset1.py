"""
Bin Dataset 1 points according to binning.csv.
- Adds a 'bin' column based on binning.csv label-to-bin mapping
- Only includes 2024 data
- Drops shifting cultivation (205) and labels without a bin
- Saves shifting cultivation points separately
"""

import csv
import os
import re

BASE = r"c:\Users\Kdixter\Desktop\GIS_Analysis_Final\raw_data\dataset_1"
DS1_PATH = os.path.join(BASE, "Arunachal_ground_points - Point.csv")
BINNING_PATH = os.path.join(BASE, "binning.csv")
OUTPUT_BINNED = os.path.join(BASE, "dataset_1_binned.csv")
OUTPUT_SC = os.path.join(BASE, "dataset_1_shifting_cultivation.csv")

BIN_NAMES = ["Forest", "Tree based Ag", "Water", "Urban", "Non-Tree Ag", "Grassland/Open"]


def extract_year(date_str):
    """Extract a 4-digit year from a date string."""
    if not date_str or date_str == "########":
        return None
    match = re.search(r'(20\d{2}|19\d{2})', date_str)
    if match:
        return int(match.group(1))
    match = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})$', date_str)
    if match:
        yr = int(match.group(3))
        return 2000 + yr if yr < 50 else 1900 + yr
    return None


def load_binning(path):
    """Load binning.csv and return a dict: label -> bin_name."""
    label_to_bin = {}
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            label = row.get("Labels", "").strip()
            if not label:
                continue
            for bin_name in BIN_NAMES:
                if row.get(bin_name, "").strip() == "1":
                    label_to_bin[label] = bin_name
                    break
    return label_to_bin


def main():
    # Load binning mapping
    label_to_bin = load_binning(BINNING_PATH)
    print("Label-to-bin mapping loaded:")
    for label, bin_name in sorted(label_to_bin.items(), key=lambda x: int(x[0])):
        print(f"  {label} -> {bin_name}")

    labels_without_bin = []
    with open(BINNING_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            label = row.get("Labels", "").strip()
            if label and label not in label_to_bin:
                labels_without_bin.append(label)
    print(f"\nLabels WITHOUT bin (will be dropped): {labels_without_bin}")

    # Read Dataset 1 and split
    with open(DS1_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        all_rows = list(reader)

    print(f"\nTotal rows in Dataset 1: {len(all_rows):,}")

    binned_rows = []
    sc_rows = []
    dropped_no_bin = 0
    dropped_not_2024 = 0
    dropped_no_label = 0

    for row in all_rows:
        label = row.get("label", "").strip()
        date = row.get("date collected", "").strip()
        year = extract_year(date)

        # Skip empty labels
        if not label:
            dropped_no_label += 1
            continue

        # Shifting cultivation -> separate file (2024 only)
        if label == "205":
            if year == 2024:
                sc_rows.append(row)
            else:
                dropped_not_2024 += 1
            continue

        # Check if label has a bin
        bin_name = label_to_bin.get(label)
        if not bin_name:
            dropped_no_bin += 1
            continue

        # Only 2024 data
        if year != 2024:
            dropped_not_2024 += 1
            continue

        # Add bin column and keep
        row["bin"] = bin_name
        binned_rows.append(row)

    # Write binned output
    out_header = header + ["bin"]
    with open(OUTPUT_BINNED, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_header, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(binned_rows)

    # Write shifting cultivation output
    with open(OUTPUT_SC, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sc_rows)

    # Summary
    print(f"\n{'='*50}")
    print("RESULTS")
    print(f"{'='*50}")
    print(f"  Binned points (2024, excl. SC):  {len(binned_rows):,}  -> {OUTPUT_BINNED}")
    print(f"  Shifting cultivation (2024):     {len(sc_rows):,}  -> {OUTPUT_SC}")
    print(f"  Dropped (no label):              {dropped_no_label:,}")
    print(f"  Dropped (no bin assigned):        {dropped_no_bin:,}")
    print(f"  Dropped (not from 2024):          {dropped_not_2024:,}")
    print(f"  Total accounted for:              {len(binned_rows) + len(sc_rows) + dropped_no_label + dropped_no_bin + dropped_not_2024:,}")

    # Bin distribution
    from collections import Counter
    bin_counts = Counter(r["bin"] for r in binned_rows)
    print(f"\nBin distribution:")
    for bin_name, count in sorted(bin_counts.items(), key=lambda x: -x[1]):
        print(f"  {bin_name:<20} {count:>6,}")


if __name__ == "__main__":
    main()
