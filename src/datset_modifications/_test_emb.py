"""Test different approaches to fix embedding extraction."""
import ee
ee.Initialize(project="gis-hub-464402")

col = ee.ImageCollection("GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL")

# Approach 1: .first().unmask()
img1 = col.filterDate("2024-01-01", "2025-01-01").first().unmask()

# Approach 2: .mosaic() (merges all images in collection)
img2 = col.filterDate("2024-01-01", "2025-01-01").mosaic()

# Approach 3: .mean()
img3 = col.filterDate("2024-01-01", "2025-01-01").mean()

pt = ee.Geometry.Point([95.1724194, 28.0330728])

for i, (label, img) in enumerate([
    ("first().unmask()", img1),
    ("mosaic()", img2),
    ("mean()", img3),
]):
    # Direct reduceRegion
    direct = img.reduceRegion(reducer=ee.Reducer.first(), geometry=pt, scale=10).getInfo()
    
    # Mapped reduceRegion
    fc = ee.FeatureCollection([ee.Feature(pt, {"SNo": "test"})])
    def sample(f, _img=img):
        vals = _img.reduceRegion(reducer=ee.Reducer.first(), geometry=f.geometry(), scale=10)
        return f.set(vals)
    mapped = fc.map(sample).first().getInfo()["properties"]
    
    print(f"\n{label}:")
    print(f"  Direct  A00={direct.get('A00')}")
    print(f"  Mapped  A00={mapped.get('A00')}")
