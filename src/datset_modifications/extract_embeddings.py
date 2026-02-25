"""
Extract Google Satellite Embedding V1 (2024 annual) for all binned Dataset 1 points.
Also extracts Sentinel-2 cloud cover statistics for December 2024 at each point.

Uses ee.Feature.map + reduceRegion for reliable embedding extraction.

Outputs:
  - raw_data/dataset_1/dataset_1_embeddings.csv
"""

import csv
import json
import os
import time
import ee

# ── Configuration ──────────────────────────────────────────────────────────
EE_PROJECT = "gis-hub-464402"
BASE = r"c:\Users\Kdixter\Desktop\GIS_Analysis_Final\raw_data\dataset_1"
INPUT_PATH = os.path.join(BASE, "dataset_1_binned.csv")
OUTPUT_PATH = os.path.join(BASE, "dataset_1_embeddings.csv")

EMBEDDING_COLLECTION = "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL"
CLOUD_COLLECTION = "COPERNICUS/S2_CLOUD_PROBABILITY"
S2_COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"

EMBEDDING_BANDS = [f"A{i:02d}" for i in range(64)]
BATCH_SIZE = 2000  # Smaller batches for mapped reduceRegion


def init_ee():
    """Initialize Earth Engine."""
    ee.Initialize(project=EE_PROJECT)
    print("Earth Engine initialized.")


def load_points(path):
    """Load binned CSV and return list of dicts."""
    points = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                points.append({
                    "SNo": row["SNo"],
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                    "label": row["label"],
                    "bin": row["bin"],
                    "class_description": row.get("class description", ""),
                })
            except (ValueError, KeyError) as e:
                print(f"  Skipping row {row.get('SNo', '?')}: {e}")
    return points


def extract_embeddings_batch(points_batch, embedding_image):
    """Extract 64-dim embeddings using mapped reduceRegion."""
    features = []
    for p in points_batch:
        geom = ee.Geometry.Point([p["lon"], p["lat"]])
        feat = ee.Feature(geom, {"SNo": p["SNo"]})
        features.append(feat)

    fc = ee.FeatureCollection(features)

    def sample_point(feature):
        geom = feature.geometry()
        values = embedding_image.reduceRegion(
            reducer=ee.Reducer.first(),
            geometry=geom,
            scale=10,
        )
        return feature.set(values)

    sampled = fc.map(sample_point)
    return sampled.getInfo()


def extract_cloud_batch(points_batch, cloud_mean, scene_stack):
    """Extract cloud stats using mapped reduceRegion."""
    features = []
    for p in points_batch:
        geom = ee.Geometry.Point([p["lon"], p["lat"]])
        feat = ee.Feature(geom, {"SNo": p["SNo"]})
        features.append(feat)

    fc = ee.FeatureCollection(features)

    def sample_cloud(feature):
        geom = feature.geometry()
        cloud_vals = cloud_mean.reduceRegion(
            reducer=ee.Reducer.first(),
            geometry=geom,
            scale=10,
        )
        scene_vals = scene_stack.reduceRegion(
            reducer=ee.Reducer.first(),
            geometry=geom,
            scale=10,
        )
        return feature.set({
            "cloud_mean_prob": cloud_vals.get("probability"),
            "total_scenes": scene_vals.get("total_scenes"),
            "clear_scenes": scene_vals.get("clear_scenes"),
        })

    sampled = fc.map(sample_cloud)
    return sampled.getInfo()


def main():
    init_ee()

    # Load points
    points = load_points(INPUT_PATH)
    print(f"Loaded {len(points):,} points from binned dataset.")

    # Prepare EE imagery
    print("Loading Satellite Embedding V1 (2024 annual)...")
    embedding_image = (
        ee.ImageCollection(EMBEDDING_COLLECTION)
        .filterDate("2024-01-01", "2025-01-01")
        .mosaic()
    )

    print("Loading Sentinel-2 Cloud Probability (Dec 2024)...")
    cloud_mean = (
        ee.ImageCollection(CLOUD_COLLECTION)
        .filterDate("2024-12-01", "2025-01-01")
        .mean()
    )

    s2_col = (
        ee.ImageCollection(S2_COLLECTION)
        .filterDate("2024-12-01", "2025-01-01")
    )
    total_scenes = s2_col.select("B4").count().rename("total_scenes")
    clear_scenes = (
        s2_col.filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
        .select("B4")
        .count()
        .rename("clear_scenes")
    )
    scene_stack = total_scenes.addBands(clear_scenes)

    # Process in batches
    total_batches = (len(points) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Processing {total_batches} batches of up to {BATCH_SIZE} points each...\n")

    embedding_results = {}
    cloud_results = {}

    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(points))
        batch = points[start:end]
        print(f"  Batch {batch_idx + 1}/{total_batches} "
              f"(points {start + 1}-{end})...", end=" ", flush=True)

        t0 = time.time()

        # Embeddings
        try:
            emb_info = extract_embeddings_batch(batch, embedding_image)
            for feat in emb_info.get("features", []):
                props = feat["properties"]
                sno = str(props.get("SNo", ""))
                embedding_results[sno] = {
                    band: props.get(band) for band in EMBEDDING_BANDS
                }
        except Exception as e:
            print(f"\n    WARNING: Embedding extraction failed: {e}")

        # Cloud stats
        try:
            cloud_info = extract_cloud_batch(batch, cloud_mean, scene_stack)
            for feat in cloud_info.get("features", []):
                props = feat["properties"]
                sno = str(props.get("SNo", ""))
                cloud_results[sno] = {
                    "cloud_mean_prob": props.get("cloud_mean_prob"),
                    "total_scenes": props.get("total_scenes"),
                    "clear_scenes": props.get("clear_scenes"),
                }
        except Exception as e:
            print(f"\n    WARNING: Cloud stats failed: {e}")

        elapsed = time.time() - t0
        emb_ok = sum(1 for s in embedding_results.values()
                     if any(v is not None for v in s.values()))
        print(f"done ({elapsed:.1f}s) [embeddings so far: {emb_ok:,}]")

        # Throttle
        if batch_idx < total_batches - 1:
            time.sleep(1)

    # Merge and write output
    print(f"\nMerging results and writing to {OUTPUT_PATH}...")

    out_header = [
        "SNo", "lat", "lon", "label", "class_description", "bin"
    ] + EMBEDDING_BANDS + [
        "cloud_mean_prob", "total_scenes", "clear_scenes", "cloudy_pct"
    ]

    written = 0
    with_emb = 0
    with open(OUTPUT_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_header)
        writer.writeheader()

        for p in points:
            sno = p["SNo"]
            row = {
                "SNo": sno,
                "lat": p["lat"],
                "lon": p["lon"],
                "label": p["label"],
                "class_description": p["class_description"],
                "bin": p["bin"],
            }

            # Add embeddings
            emb = embedding_results.get(sno, {})
            has_emb = any(v is not None for v in emb.values())
            if has_emb:
                with_emb += 1
            for band in EMBEDDING_BANDS:
                row[band] = emb.get(band, "") if has_emb else ""

            # Add cloud stats
            cld = cloud_results.get(sno, {})
            row["cloud_mean_prob"] = cld.get("cloud_mean_prob", "")
            row["total_scenes"] = cld.get("total_scenes", "")
            row["clear_scenes"] = cld.get("clear_scenes", "")

            total = cld.get("total_scenes")
            clear = cld.get("clear_scenes")
            if total and clear and total > 0:
                row["cloudy_pct"] = round((1 - clear / total) * 100, 1)
            else:
                row["cloudy_pct"] = ""

            writer.writerow(row)
            written += 1

    # Summary
    print(f"\n{'='*60}")
    print("EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"  Points written:              {written:,}")
    print(f"  Points WITH embeddings:      {with_emb:,}")
    print(f"  Points missing embeddings:   {written - with_emb:,}")
    print(f"  Output: {OUTPUT_PATH}")

    # Cloud cover summary
    all_cloudy = [
        cloud_results[s]["cloud_mean_prob"]
        for s in cloud_results
        if cloud_results[s].get("cloud_mean_prob") is not None
    ]
    if all_cloudy:
        avg_cloud = sum(all_cloudy) / len(all_cloudy)
        print(f"\n  Cloud Cover Analysis (Dec 2024):")
        print(f"    Points with cloud data:   {len(all_cloudy):,}")
        print(f"    Mean cloud probability:   {avg_cloud:.1f}%")
        print(f"    Min cloud probability:    {min(all_cloudy):.1f}%")
        print(f"    Max cloud probability:    {max(all_cloudy):.1f}%")

        # Distribution buckets
        low = sum(1 for c in all_cloudy if c < 20)
        mid = sum(1 for c in all_cloudy if 20 <= c < 50)
        high = sum(1 for c in all_cloudy if c >= 50)
        print(f"    Low cloud (<20%):         {low:,} ({low/len(all_cloudy)*100:.1f}%)")
        print(f"    Medium cloud (20-50%):    {mid:,} ({mid/len(all_cloudy)*100:.1f}%)")
        print(f"    High cloud (>50%):        {high:,} ({high/len(all_cloudy)*100:.1f}%)")


if __name__ == "__main__":
    main()
