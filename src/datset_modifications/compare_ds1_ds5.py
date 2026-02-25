"""
Compare Dataset 1 and Dataset 5 to detect any differences in data.
Outputs a detailed report to the console.
"""

import csv
import os

BASE = r"c:\Users\Kdixter\Desktop\GIS_Analysis_Final\raw_data"
DS1_PATH = os.path.join(BASE, "dataset_1", "Arunachal_ground_points - Point.csv")
DS5_PATH = os.path.join(BASE, "dataset_5", "Arunachal_ground_points.xlsx - Point.csv")


def read_csv(path):
    """Read a CSV file and return header + list of row dicts."""
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = list(reader)
    return header, rows


def compare_datasets():
    print("=" * 70)
    print("COMPARISON REPORT: Dataset 1 vs Dataset 5")
    print("=" * 70)
    print()

    # --- Read both datasets ---
    header1, rows1 = read_csv(DS1_PATH)
    header5, rows5 = read_csv(DS5_PATH)

    print(f"Dataset 1: {os.path.basename(DS1_PATH)}")
    print(f"  Rows: {len(rows1)}")
    print(f"  Columns ({len(header1)}): {header1}")
    print()
    print(f"Dataset 5: {os.path.basename(DS5_PATH)}")
    print(f"  Rows: {len(rows5)}")
    print(f"  Columns ({len(header5)}): {header5}")
    print()

    # =====================================================================
    # 1. COLUMN / HEADER DIFFERENCES
    # =====================================================================
    print("-" * 70)
    print("1. COLUMN / HEADER DIFFERENCES")
    print("-" * 70)

    if header1 == header5:
        print("  Headers are IDENTICAL.")
    else:
        only_in_ds1 = set(header1) - set(header5)
        only_in_ds5 = set(header5) - set(header1)
        if only_in_ds1:
            print(f"  Columns only in Dataset 1: {only_in_ds1}")
        if only_in_ds5:
            print(f"  Columns only in Dataset 5: {only_in_ds5}")

        # Check for column name differences at same position
        for i in range(min(len(header1), len(header5))):
            if header1[i] != header5[i]:
                print(f"  Column {i+1} differs: DS1='{header1[i]}' vs DS5='{header5[i]}'")
    print()

    # =====================================================================
    # 2. ROW COUNT DIFFERENCE
    # =====================================================================
    print("-" * 70)
    print("2. ROW COUNT")
    print("-" * 70)
    if len(rows1) == len(rows5):
        print(f"  Both datasets have {len(rows1)} rows.")
    else:
        print(f"  Dataset 1 has {len(rows1)} rows, Dataset 5 has {len(rows5)} rows.")
        print(f"  Difference: {abs(len(rows1) - len(rows5))} rows.")
    print()

    # =====================================================================
    # 3. LABEL DIFFERENCES
    # =====================================================================
    print("-" * 70)
    print("3. LABEL DIFFERENCES")
    print("-" * 70)

    labels1 = set(r.get("label", "").strip() for r in rows1)
    labels5 = set(r.get("label", "").strip() for r in rows5)

    only_in_ds1_labels = labels1 - labels5
    only_in_ds5_labels = labels5 - labels1
    common_labels = labels1 & labels5

    print(f"  Unique labels in Dataset 1: {len(labels1)} -> {sorted(labels1)}")
    print(f"  Unique labels in Dataset 5: {len(labels5)} -> {sorted(labels5)}")
    print(f"  Labels in common: {len(common_labels)}")

    if only_in_ds1_labels:
        print(f"  Labels ONLY in Dataset 1: {sorted(only_in_ds1_labels)}")
    if only_in_ds5_labels:
        print(f"  Labels ONLY in Dataset 5: {sorted(only_in_ds5_labels)}")
    if not only_in_ds1_labels and not only_in_ds5_labels:
        print("  Both datasets use the exact same set of labels.")
    print()

    # =====================================================================
    # 4. LABEL DISTRIBUTION COMPARISON
    # =====================================================================
    print("-" * 70)
    print("4. LABEL DISTRIBUTION (count per label)")
    print("-" * 70)

    from collections import Counter
    count1 = Counter(r.get("label", "").strip() for r in rows1)
    count5 = Counter(r.get("label", "").strip() for r in rows5)

    all_labels = sorted(labels1 | labels5)
    print(f"  {'Label':<10} {'DS1 Count':>10} {'DS5 Count':>10} {'Diff':>10}")
    print(f"  {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

    for label in all_labels:
        c1 = count1.get(label, 0)
        c5 = count5.get(label, 0)
        diff = c5 - c1
        marker = " <--- DIFFERENT" if diff != 0 else ""
        print(f"  {label:<10} {c1:>10} {c5:>10} {diff:>+10}{marker}")
    print()

    # =====================================================================
    # 5. CLASS DESCRIPTION DIFFERENCES (for same label)
    # =====================================================================
    print("-" * 70)
    print("5. CLASS / CLASS DESCRIPTION DIFFERENCES (per label)")
    print("-" * 70)

    def get_label_mappings(rows):
        """For each label, collect all unique (class, class description) pairs."""
        mappings = {}
        for r in rows:
            label = r.get("label", "").strip()
            cls = r.get("class", "").strip()
            desc = r.get("class description", "").strip()
            mappings.setdefault(label, set()).add((cls, desc))
        return mappings

    map1 = get_label_mappings(rows1)
    map5 = get_label_mappings(rows5)

    found_diff = False
    for label in sorted(common_labels):
        if map1.get(label) != map5.get(label):
            found_diff = True
            print(f"  Label '{label}':")
            print(f"    DS1: {map1.get(label)}")
            print(f"    DS5: {map5.get(label)}")

    if not found_diff:
        print("  No differences in class/class description for common labels.")
    print()

    # =====================================================================
    # 6. ROW-BY-ROW COMPARISON (using common columns)
    # =====================================================================
    print("-" * 70)
    print("6. ROW-BY-ROW COMPARISON")
    print("-" * 70)

    common_cols = [c for c in header1 if c in header5]
    print(f"  Comparing on common columns: {common_cols}")

    max_rows = min(len(rows1), len(rows5))
    diff_count = 0
    diff_examples = []

    for i in range(max_rows):
        for col in common_cols:
            val1 = rows1[i].get(col, "").strip()
            val5 = rows5[i].get(col, "").strip()
            if val1 != val5:
                diff_count += 1
                if len(diff_examples) < 20:  # Show first 20 examples
                    diff_examples.append(
                        f"  Row {i+1}, Column '{col}': DS1='{val1}' vs DS5='{val5}'"
                    )

    print(f"  Total cell-level differences found: {diff_count}")
    if diff_examples:
        print(f"  First {len(diff_examples)} differences:")
        for ex in diff_examples:
            print(ex)
    else:
        print("  The datasets are IDENTICAL on a row-by-row, cell-by-cell basis.")
    print()

    # =====================================================================
    # 7. COORDINATE DIFFERENCES
    # =====================================================================
    print("-" * 70)
    print("7. COORDINATE (lat/lon) COMPARISON")
    print("-" * 70)

    lat_diffs = 0
    lon_diffs = 0
    coord_examples = []

    for i in range(max_rows):
        lat1 = rows1[i].get("lat", "").strip()
        lat5 = rows5[i].get("lat", "").strip()
        lon1 = rows1[i].get("lon", "").strip()
        lon5 = rows5[i].get("lon", "").strip()

        if lat1 != lat5:
            lat_diffs += 1
            if len(coord_examples) < 5:
                coord_examples.append(
                    f"  Row {i+1}: lat DS1={lat1} vs DS5={lat5}"
                )
        if lon1 != lon5:
            lon_diffs += 1
            if len(coord_examples) < 5:
                coord_examples.append(
                    f"  Row {i+1}: lon DS1={lon1} vs DS5={lon5}"
                )

    print(f"  Latitude differences: {lat_diffs}")
    print(f"  Longitude differences: {lon_diffs}")
    if coord_examples:
        print("  Examples:")
        for ex in coord_examples:
            print(ex)
    else:
        print("  All coordinates match exactly.")
    print()

    # =====================================================================
    # 8. SUMMARY
    # =====================================================================
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)

    issues = []
    if header1 != header5:
        issues.append("Column headers differ")
    if len(rows1) != len(rows5):
        issues.append(f"Row count differs ({len(rows1)} vs {len(rows5)})")
    if only_in_ds1_labels:
        issues.append(f"Labels only in DS1: {sorted(only_in_ds1_labels)}")
    if only_in_ds5_labels:
        issues.append(f"Labels only in DS5: {sorted(only_in_ds5_labels)}")
    if diff_count > 0:
        issues.append(f"{diff_count} cell-level differences found")
    if found_diff:
        issues.append("Class/description mismatches for some labels")

    if issues:
        print("  DIFFERENCES DETECTED:")
        for i, issue in enumerate(issues, 1):
            print(f"    {i}. {issue}")
    else:
        print("  NO DIFFERENCES DETECTED. The datasets appear to be identical.")


if __name__ == "__main__":
    compare_datasets()
