import ee
ee.Initialize()

lon, lat = 47.0772162354938, 39.996104389160976 
aoi = ee.Geometry.Point(lon, lat).buffer(1000)  

collection = (
    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
    .filterBounds(aoi)
    .filterDate("2025-07-01", "2025-07-31")
    .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10))
)

img = collection.sort("CLOUDY_PIXEL_PERCENTAGE").first()

rgb_vis = img.visualize(bands=["B4", "B3", "B2"], min=0, max=3000)

task = ee.batch.Export.image.toDrive(
    image=rgb_vis,
    description="Sentinel2_RGB_CottonTest",
    folder="Sentinel_Images",
    region=aoi,
    scale=10
)
task.start()
