"""
Cross-compare Datasets 1, 3, and 4 to find:
  - Shared GPS points (matched by lat/lon)
  - Label disagreements among shared points
  - Date analysis for disagreeing points
  - Output: disagreements_year_same.csv and disagreements_year_different.csv
"""

import csv
import os
import re
from collections import defaultdict
from itertools import combinations

BASE = r"c:\Users\Kdixter\Desktop\GIS_Analysis_Final\raw_data"
OUTPUT_DIR = os.path.join(BASE, "dataset_conflicts")

DATASETS = {
    "DS1": os.path.join(BASE, "dataset_1", "Arunachal_ground_points - Point.csv"),
    "DS3": os.path.join(BASE, "dataset_3", "Arunachal_ground_points_working_file - Point.csv"),
    "DS4": os.path.join(BASE, "dataset_4", "Arunachal_ground_points_working_file - Chiging's data (1).csv"),
}


def extract_year(date_str):
    """Try to extract a 4-digit year from a date string. Returns None if unable."""
    if not date_str or date_str == "########":
        return None
    # Look for 4-digit year
    match = re.search(r'(20\d{2}|19\d{2})', date_str)
    if match:
        return int(match.group(1))
    # Try 2-digit year at end like dd/mm/yy
    match = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})$', date_str)
    if match:
        yr = int(match.group(3))
        return 2000 + yr if yr < 50 else 1900 + yr
    return None


def round_coord(val, decimals=6):
    """Round a coordinate string to given decimals for matching."""
    try:
        return round(float(val), decimals)
    except (ValueError, TypeError):
        return None


def read_dataset(path, ds_name):
    """Read dataset and index rows by (lat, lon) coordinate pair."""
    coord_index = defaultdict(list)
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lat = round_coord(row.get("lat", "").strip())
            lon = round_coord(row.get("lon", "").strip())
            if lat is not None and lon is not None:
                coord_index[(lat, lon)].append({
                    "dataset": ds_name,
                    "SNo": row.get("SNo", "").strip(),
                    "label": row.get("label", "").strip(),
                    "class": row.get("class", "").strip(),
                    "class_description": row.get("class description", "").strip(),
                    "lat": row.get("lat", "").strip(),
                    "lon": row.get("lon", "").strip(),
                    "date_collected": row.get("date collected", "").strip(),
                    "GPS_ID": row.get("GPS ID", "").strip(),
                    "location": row.get("location", "").strip(),
                })
    return coord_index


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Read all datasets
    print("Reading datasets...")
    data = {}
    for ds_name, path in DATASETS.items():
        data[ds_name] = read_dataset(path, ds_name)
        print(f"  {ds_name}: {sum(len(v) for v in data[ds_name].values())} rows, "
              f"{len(data[ds_name])} unique coordinates")

    print()
    ds_names = sorted(data.keys())

    # =================================================================
    # PAIRWISE COMPARISON
    # =================================================================
    all_disagreements_same_year = []
    all_disagreements_diff_year = []

    report_lines = []
    report_lines.append("=" * 70)
    report_lines.append("CROSS-COMPARISON REPORT: Datasets 1, 3, and 4")
    report_lines.append("=" * 70)
    report_lines.append("")

    for ds_a, ds_b in combinations(ds_names, 2):
        report_lines.append("-" * 70)
        report_lines.append(f"PAIR: {ds_a} vs {ds_b}")
        report_lines.append("-" * 70)

        coords_a = set(data[ds_a].keys())
        coords_b = set(data[ds_b].keys())
        shared_coords = coords_a & coords_b
        only_a = coords_a - coords_b
        only_b = coords_b - coords_a

        report_lines.append(f"  Unique coords in {ds_a}: {len(coords_a)}")
        report_lines.append(f"  Unique coords in {ds_b}: {len(coords_b)}")
        report_lines.append(f"  Shared coordinates: {len(shared_coords)}")
        report_lines.append(f"  Only in {ds_a}: {len(only_a)}")
        report_lines.append(f"  Only in {ds_b}: {len(only_b)}")

        # Check label disagreements among shared coords
        agree_count = 0
        disagree_count = 0
        same_year_disagree = 0
        diff_year_disagree = 0

        for coord in shared_coords:
            rows_a = data[ds_a][coord]
            rows_b = data[ds_b][coord]

            labels_a = set(r["label"] for r in rows_a)
            labels_b = set(r["label"] for r in rows_b)

            if labels_a == labels_b:
                agree_count += 1
            else:
                disagree_count += 1

                # Get years from both sides
                years_a = set(extract_year(r["date_collected"]) for r in rows_a)
                years_b = set(extract_year(r["date_collected"]) for r in rows_b)
                years_a.discard(None)
                years_b.discard(None)

                # Determine if years are same or different
                years_same = bool(years_a & years_b) if years_a and years_b else None

                for ra in rows_a:
                    for rb in rows_b:
                        if ra["label"] != rb["label"]:
                            year_a = extract_year(ra["date_collected"])
                            year_b = extract_year(rb["date_collected"])

                            record = {
                                "lat": ra["lat"],
                                "lon": ra["lon"],
                                f"{ds_a}_SNo": ra["SNo"],
                                f"{ds_a}_label": ra["label"],
                                f"{ds_a}_class": ra["class"],
                                f"{ds_a}_class_description": ra["class_description"],
                                f"{ds_a}_date": ra["date_collected"],
                                f"{ds_a}_year": year_a if year_a else "",
                                f"{ds_a}_GPS_ID": ra["GPS_ID"],
                                f"{ds_a}_location": ra["location"],
                                f"{ds_b}_SNo": rb["SNo"],
                                f"{ds_b}_label": rb["label"],
                                f"{ds_b}_class": rb["class"],
                                f"{ds_b}_class_description": rb["class_description"],
                                f"{ds_b}_date": rb["date_collected"],
                                f"{ds_b}_year": year_b if year_b else "",
                                f"{ds_b}_GPS_ID": rb["GPS_ID"],
                                f"{ds_b}_location": rb["location"],
                                "pair": f"{ds_a}_vs_{ds_b}",
                            }

                            if year_a is not None and year_b is not None:
                                if year_a == year_b:
                                    all_disagreements_same_year.append(record)
                                    same_year_disagree += 1
                                else:
                                    all_disagreements_diff_year.append(record)
                                    diff_year_disagree += 1
                            else:
                                # Unknown year — put in same-year for review
                                record["note"] = "year_unknown"
                                all_disagreements_same_year.append(record)
                                same_year_disagree += 1

        report_lines.append(f"  Label AGREE on shared coords: {agree_count}")
        report_lines.append(f"  Label DISAGREE on shared coords: {disagree_count}")
        report_lines.append(f"    -> Same year, different label: {same_year_disagree}")
        report_lines.append(f"    -> Different year, different label: {diff_year_disagree}")
        report_lines.append("")

    # =================================================================
    # THREE-WAY OVERLAP
    # =================================================================
    report_lines.append("-" * 70)
    report_lines.append("THREE-WAY OVERLAP: DS1 & DS3 & DS4")
    report_lines.append("-" * 70)

    all_coords = [set(data[ds].keys()) for ds in ds_names]
    three_way = all_coords[0] & all_coords[1] & all_coords[2]
    report_lines.append(f"  Coordinates present in ALL three datasets: {len(three_way)}")

    three_way_disagree = 0
    for coord in three_way:
        labels = {}
        for ds in ds_names:
            labels[ds] = set(r["label"] for r in data[ds][coord])
        all_same = (labels[ds_names[0]] == labels[ds_names[1]] == labels[ds_names[2]])
        if not all_same:
            three_way_disagree += 1

    report_lines.append(f"  Three-way label disagreements: {three_way_disagree}")
    report_lines.append("")

    # =================================================================
    # WRITE REPORT
    # =================================================================
    report_text = "\n".join(report_lines)
    print(report_text)

    report_path = os.path.join(OUTPUT_DIR, "dataset_comparisons.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"\nReport written to: {report_path}")

    # =================================================================
    # WRITE DISAGREEMENT CSVs
    # =================================================================
    def get_all_keys(records):
        """Get ordered unique keys from a list of dicts."""
        keys = []
        for rec in records:
            for k in rec:
                if k not in keys:
                    keys.append(k)
        return keys

    if all_disagreements_same_year:
        keys = get_all_keys(all_disagreements_same_year)
        path = os.path.join(OUTPUT_DIR, "disagreements_year_same.csv")
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_disagreements_same_year)
        print(f"Wrote {len(all_disagreements_same_year)} rows to: {path}")
    else:
        print("No same-year disagreements found.")

    if all_disagreements_diff_year:
        keys = get_all_keys(all_disagreements_diff_year)
        path = os.path.join(OUTPUT_DIR, "disagreements_year_different.csv")
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_disagreements_diff_year)
        print(f"Wrote {len(all_disagreements_diff_year)} rows to: {path}")
    else:
        print("No different-year disagreements found.")

    # =================================================================
    # METADATA SUGGESTIONS
    # =================================================================
    print()
    print("=" * 70)
    print("SUGGESTIONS FOR HANDLING DIFFERENT METADATA")
    print("=" * 70)
    suggestions = """
Datasets 1/3 use Metadata_1 (32 labels) while Dataset 4 uses Metadata_2
(25 labels, a subset). Here are some approaches to harmonize them:

1. USE METADATA_1 AS THE MASTER REFERENCE
   Since Metadata_2 is a strict subset of Metadata_1, adopt Metadata_1
   as the canonical label reference. Any labels in DS4 already exist in
   Metadata_1, so no remapping is needed -- only DS1/DS3 have extra labels
   that DS4 doesn't use.

2. CREATE A UNIFIED METADATA FILE
   Merge both into a single 'metadata_unified.csv' that contains all 32
   labels from Metadata_1. Add a column indicating which metadata version
   each label comes from (or both).

3. BUILD A LABEL HIERARCHY / GROUPING
   Some labels could be grouped into broader categories for analysis:
     - Forest types: 100-109, 301, 306, 307 -> "Forest"
     - Agriculture: 200-207 -> "Agriculture/Plantation"
     - Open/Degraded: 300-305 -> "Open/Degraded land"
     - Water: 400 -> "Water"
     - Built-up: 500-501 -> "Built-up"
     - Disturbance: 600-602 -> "Disturbance"
   This can help when comparing across datasets that use different
   granularity levels.

4. RESOLVE CONFLICTING DESCRIPTIONS
   Some labels have conflicting descriptions across datasets (e.g.,
   label 109 = "Temperate forests" in metadata vs "Old_logged/Secondary"
   in data, label 601 = "Hills" in metadata vs "alpine and snow" in data).
   Decision: pick one authoritative description per label and apply
   consistently.

5. RECLASSIFY DATASET 3's LABEL 206
   DS3 uses label 206 for "Arecanut plantation", but Metadata_1 assigns
   Arecanut to label 207. Either:
     a) Remap DS3's 206 -> 207 (if truly arecanut), OR
     b) Accept it as "Monoculture" (label 206) if arecanut is considered
        a type of monoculture.
"""
    print(suggestions)


if __name__ == "__main__":
    main()
