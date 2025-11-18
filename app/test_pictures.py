import ee

# If you're using a service account:
# ee.Initialize(credentials=creds, project='kamal-vuln-scanner')
ee.Initialize(project='kamal-vuln-scanner')

img = ee.Image('LANDSAT/LC08/C01/T1/LC08_044034_20140318')

try:
    info = img.getInfo()
    print("Got image info OK:")
    print(info.keys())
except Exception as e:
    import traceback
    traceback.print_exc()
