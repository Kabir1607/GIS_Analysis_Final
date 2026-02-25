import csv
import os

base = r"c:\Users\Kdixter\Desktop\GIS_Analysis_Final\raw_data"

datasets = {
    "Dataset 1": os.path.join(base, "dataset_1", "Arunachal_ground_points - Point.csv"),
    "Dataset 2": os.path.join(base, "dataset_2", "Arunachal_ground_points - KP-FAK.csv"),
    "Dataset 3": os.path.join(base, "dataset_3", "Arunachal_ground_points_working_file - Point.csv"),
    "Dataset 4": os.path.join(base, "dataset_4", "Arunachal_ground_points_working_file - Chiging's data (1).csv"),
}

output_lines = []
output_lines.append("DATASET LABELS REPORT")
output_lines.append("=" * 60)
output_lines.append("")

for name, path in datasets.items():
    label_class_map = {}
    row_count = 0
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1
            label = row["label"].strip()
            cls = row.get("class", "").strip()
            cls_desc = row.get("class description", "").strip()
            if label not in label_class_map:
                label_class_map[label] = (cls, cls_desc)

    output_lines.append("-" * 60)
    output_lines.append(f"{name} ({os.path.basename(path)})")
    output_lines.append(f"Total rows: {row_count}")
    output_lines.append(f"Unique labels: {len(label_class_map)}")
    output_lines.append("-" * 60)
    output_lines.append("")

    header = f"  {'Label':<12} {'Class':<12} {'Class Description'}"
    output_lines.append(header)
    output_lines.append(f"  {'-'*10:<12} {'-'*10:<12} {'-'*25}")

    # Sort: numeric labels first (by value), then text labels alphabetically
    def sort_key(x):
        try:
            return (0, int(x), "")
        except ValueError:
            return (1, 0, x)

    for label in sorted(label_class_map.keys(), key=sort_key):
        cls, desc = label_class_map[label]
        output_lines.append(f"  {label:<12} {cls:<12} {desc}")

    output_lines.append("")

out_path = os.path.join(base, "dataset_labels.txt")
with open(out_path, "w", encoding="utf-8") as f:
    f.write("\n".join(output_lines))

print(f"Done! Written to {out_path}")
print()
print("\n".join(output_lines))
