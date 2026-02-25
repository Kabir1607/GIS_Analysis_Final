"""
Extract multi-source features (Sentinel-2, Sentinel-1, DEM) for binned points.
Uses cloud masking logic adapted from Landsat 8 script for Sentinel-2 via S2 Cloud Probability.

This script is on STANDBY and will not be run during the initial baseline modeling phase.
"""

import csv
import os
import time
import ee

# ── Configuration ──────────────────────────────────────────────────────────
EE_PROJECT = "gis-hub-464402"
BASE = r"c:\Users\Kdixter\Desktop\GIS_Analysis_Final\raw_data\dataset_1"
INPUT_PATH = os.path.join(BASE, "dataset_1_binned.csv")
OUTPUT_PATH = os.path.join(BASE, "dataset_1_multisource.csv")

BATCH_SIZE = 2000

# Sentinel-2 with cloud probability
S2_COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"
S2_CLOUD_PROB = "COPERNICUS/S2_CLOUD_PROBABILITY"
MAX_CLOUD_PROB = 30  # Threshold for masking clouds

# Sentinel-1 SAR
S1_COLLECTION = "COPERNICUS/S1_GRD"

# DEM (NASADEM)
DEM_COLLECTION = "NASA/NASADEM_HGT/001"


def init_ee():
    """Initialize Earth Engine."""
    ee.Initialize(project=EE_PROJECT)
    print("Earth Engine initialized.")


def get_s2_cloud_masked(start_date, end_date):
    """Get Cloud-masked Sentinel-2 composite over given date range."""
    # 1. Join S2 SR with Cloud Probability
    s2_sr = ee.ImageCollection(S2_COLLECTION).filterDate(start_date, end_date)
    s2_clouds = ee.ImageCollection(S2_CLOUD_PROB).filterDate(start_date, end_date)

    # Join based on system:time_start
    inner_join = ee.Join.inner()
    join_filter = ee.Filter.equals(
        leftField="system:time_start", rightField="system:time_start"
    )
    joined = inner_join.apply(s2_sr, s2_clouds, join_filter)

    def mask_clouds(feature):
        img = ee.Image(feature.get('primary'))
        cld = ee.Image(feature.get('secondary')).select('probability')
        # Mask out pixels where cloud probability is higher than MAX_CLOUD_PROB
        mask = cld.lt(MAX_CLOUD_PROB)
        
        # Scale the optical bands (B2-B8, B11, B12)
        optical = img.select(['B2', 'B3', 'B4', 'B8', 'B11', 'B12']).multiply(0.0001)
        
        # Quality band: favor low cloud probability (100 - probability)
        quality = ee.Image.constant(100).subtract(cld).rename('quality_score')
        
        return optical.updateMask(mask).addBands(quality)
        
    s2_clean = ee.ImageCollection(joined.map(mask_clouds))
    
    # 2. Quality Mosaic - favor clearest pixels
    s2_mosaic = s2_clean.qualityMosaic('quality_score')

    # 3. Add Indices
    # B2=Blue, B3=Green, B4=Red, B8=NIR, B11=SWIR1, B12=SWIR2
    s2_indices = s2_mosaic.addBands([
        s2_mosaic.normalizedDifference(['B8', 'B4']).rename('NDVI'),
        s2_mosaic.normalizedDifference(['B8', 'B11']).rename('NDWI'),
        s2_mosaic.normalizedDifference(['B3', 'B11']).rename('MNDWI'),
        s2_mosaic.expression(
            '2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))',
            {
                'NIR': s2_mosaic.select('B8'),
                'RED': s2_mosaic.select('B4'),
                'BLUE': s2_mosaic.select('B2')
            }
        ).rename('EVI'),
        s2_mosaic.expression(
            '1.5 * (NIR - RED) / (0.5 + NIR + RED)',
            {
                'NIR': s2_mosaic.select('B8'),
                'RED': s2_mosaic.select('B4')
            }
        ).rename('SAVI')
    ])
    
    return s2_indices


def get_s1_composite(start_date, end_date):
    """Get Sentinel-1 SAR composite (Ascending & Descending)."""
    s1 = (
        ee.ImageCollection(S1_COLLECTION)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VH'))
        .filter(ee.Filter.eq('instrumentMode', 'IW'))
    )
    
    # Take median to smooth speckle noise
    vv = s1.select('VV').median()
    vh = s1.select('VH').median()
    
    # Add ratio
    ratio = vv.subtract(vh).rename('VV_minus_VH')  # in dB, subtraction is division
    
    return vv.addBands(vh).addBands(ratio)


def get_dem_features():
    """Get Elevation, Slope, and Aspect from NASADEM."""
    dem = ee.Image(DEM_COLLECTION).select('elevation')
    slope = ee.Terrain.slope(dem).rename('slope')
    aspect = ee.Terrain.aspect(dem).rename('aspect')
    return dem.addBands([slope, aspect])


def load_points(path):
    """Load binned CSV points."""
    points = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                points.append({
                    "SNo": row["SNo"],
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"])
                })
            except (ValueError, KeyError):
                pass
    return points


def main():
    init_ee()

    points = load_points(INPUT_PATH)
    print(f"Loaded {len(points):,} points.")

    # 1. Build Multi-Source Image Stack (2024)
    print("Building Multi-Source Stack...")
    start, end = "2024-01-01", "2025-01-01"
    
    s2_feat = get_s2_cloud_masked(start, end)
    s1_feat = get_s1_composite(start, end)
    dem_feat = get_dem_features()
    
    multi_stack = s2_feat.addBands([s1_feat, dem_feat])
    
    band_names = multi_stack.bandNames().getInfo()
    print(f"Bands ({len(band_names)}): {band_names}")

    # Process in batches via mapped reduceRegion
    total_batches = (len(points) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Processing {total_batches} batches...")

    feature_results = {}

    for batch_idx in range(total_batches):
        start_idx = batch_idx * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(points))
        batch = points[start_idx:end_idx]
        print(f"  Batch {batch_idx + 1}/{total_batches}...", end=" ", flush=True)

        t0 = time.time()

        features = []
        for p in batch:
            geom = ee.Geometry.Point([p["lon"], p["lat"]])
            feat = ee.Feature(geom, {"SNo": p["SNo"]})
            features.append(feat)

        fc = ee.FeatureCollection(features)

        def sample_stack(feature):
            vals = multi_stack.reduceRegion(
                reducer=ee.Reducer.first(),
                geometry=feature.geometry(),
                scale=10,
            )
            return feature.set(vals)

        try:
            sampled_info = fc.map(sample_stack).getInfo()
            for feat in sampled_info.get("features", []):
                props = feat["properties"]
                sno = str(props.get("SNo", ""))
                feature_results[sno] = {b: props.get(b) for b in band_names}
        except Exception as e:
            print(f"\n    WARNING: Extraction failed: {e}")

        elapsed = time.time() - t0
        print(f"done ({elapsed:.1f}s)")

        if batch_idx < total_batches - 1:
            time.sleep(1)

    # 2. Append to original CSV
    print(f"\nAppending results to {OUTPUT_PATH}...")
    
    # Read original
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        all_rows = list(reader)

    out_header = header + band_names
    
    with open(OUTPUT_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_header, extrasaction='ignore')
        writer.writeheader()
        
        for row in all_rows:
            sno = row["SNo"]
            feats = feature_results.get(sno, {})
            row.update(feats)
            writer.writerow(row)

    print("DONE.")


if __name__ == "__main__":
    main()
