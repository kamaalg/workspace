import requests
import os
import json


PLANET_API_KEY = os.getenv("PLANET_API_KEY", "PLAK7d35a3319258450c8e8af864d2d4dbb3")
session = requests.Session()
session.auth = (PLANET_API_KEY, "")

AOI_POLYGON = {
    "type": "Polygon",
    "coordinates": [[
        [48.52436681523979, 40.08474951759373],
        [48.473539895003455, 39.90389210443336],
        [48.866156023630104, 39.838108306593284],
        [48.9178844320667, 40.01833704818192],
        [48.52436681523979, 40.08474951759373]
    ]]
}


START_DATE = "2025-08-31T00:00:00Z"
END_DATE = "2025-11-07T23:59:59Z"
ITEM_TYPE = "PSScene"

print(" DIAGNOSTIC MODE - Testing different search criteria\n")
print(f" Area: Baku region")
print(f" Date range: {START_DATE} to {END_DATE}\n")


search_url = "https://api.planet.com/data/v1/quick-search"

print("=" * 70)
print("TEST 1: Searching WITHOUT cloud cover filter...")
print("=" * 70)

query1 = {
    "item_types": [ITEM_TYPE],
    "filter": {
        "type": "AndFilter",
        "config": [
            {
                "type": "GeometryFilter",
                "field_name": "geometry",
                "config": AOI_POLYGON
            },
            {
                "type": "DateRangeFilter",
                "field_name": "acquired",
                "config": {"gte": START_DATE, "lte": END_DATE}
            }
        ]
    }
}

response1 = session.post(search_url, json=query1)

if response1.status_code != 200:
    print(f"❌ API Error - Status {response1.status_code}")
    print(f"Response: {response1.text}")
else:
    results1 = response1.json()
    count1 = len(results1.get("features", []))
    print(f"✅ Found {count1} images (any cloud cover)")
    
    if count1 > 0:
        print(f"\nShowing first 3 images:")
        for i, feature in enumerate(results1["features"][:3], 1):
            props = feature["properties"]
            print(f"  {i}. Acquired: {props.get('acquired', 'N/A')[:10]} | "
                  f"Cloud: {props.get('cloud_cover', 'N/A'):.1%}" if props.get('cloud_cover') is not None else f"Cloud: N/A")


print("\n" + "=" * 70)
print("TEST 2: Searching with ≤50% cloud cover...")
print("=" * 70)

query2 = {
    "item_types": [ITEM_TYPE],
    "filter": {
        "type": "AndFilter",
        "config": [
            {
                "type": "GeometryFilter",
                "field_name": "geometry",
                "config": AOI_POLYGON
            },
            {
                "type": "DateRangeFilter",
                "field_name": "acquired",
                "config": {"gte": START_DATE, "lte": END_DATE}
            },
            {
                "type": "RangeFilter",
                "field_name": "cloud_cover",
                "config": {"lte": 0.5}
            }
        ]
    }
}

response2 = session.post(search_url, json=query2)
if response2.status_code == 200:
    count2 = len(response2.json().get("features", []))
    print(response2.json().keys())
    print(response2.json().get("features", [])[100])
    print(f"✅ Found {count2} images (≤50% cloud)")


# print("\n" + "=" * 70)
# print("TEST 3: Searching with ≤30% cloud cover (your original filter)...")
# print("=" * 70)

# query3 = {
#     "item_types": [ITEM_TYPE],
#     "filter": {
#         "type": "AndFilter",
#         "config": [
#             {
#                 "type": "GeometryFilter",
#                 "field_name": "geometry",
#                 "config": AOI_POLYGON
#             },
#             {
#                 "type": "DateRangeFilter",
#                 "field_name": "acquired",
#                 "config": {"gte": START_DATE, "lte": END_DATE}
#             },
#             {
#                 "type": "RangeFilter",
#                 "field_name": "cloud_cover",
#                 "config": {"lte": 0.3}
#             }
#         ]
#     }
# }

# response3 = session.post(search_url, json=query3)
# if response3.status_code == 200:
#     results3 = response3.json()
#     count3 = len(results3.get("features", []))
#     print(f"✅ Found {count3} images (≤30% cloud)")
    
#     if count3 > 0:
#         print(f"\n🎉 SUCCESS! Here are your images:")
#         for i, feature in enumerate(results3["features"][:5], 1):
#             item_id = feature["id"]
#             props = feature["properties"]
            
#             print(f"\n{'─' * 60}")
#             print(f"🛰️  Image #{i}")
#             print(f"ID: {item_id}")
#             print(f"Acquired: {props.get('acquired', 'N/A')}")
#             print(f"Cloud Cover: {props.get('cloud_cover', 0):.1%}")
            
#             # Get assets
#             assets_url = f"https://api.planet.com/data/v1/item-types/{ITEM_TYPE}/items/{item_id}/assets/"
#             asset_response = session.get(assets_url)
            
#             if asset_response.status_code == 200:
#                 asset_data = asset_response.json()
#                 print(f"\n📦 Available Assets:")
#                 for asset_type, asset_info in asset_data.items():
#                     status = asset_info.get("status", "unknown")
#                     print(f"   • {asset_type:25s} [{status}]")
        
#         if count3 > 5:
#             print(f"\n... and {count3 - 5} more images")

# # Summary
# print("\n" + "=" * 70)
# print("📊 SUMMARY")
# print("=" * 70)
# print(f"No cloud filter:  {count1 if 'count1' in locals() else 'Error'} images")
# print(f"≤50% cloud:       {count2 if 'count2' in locals() else 'Error'} images")
# print(f"≤30% cloud:       {count3 if 'count3' in locals() else 'Error'} images")

# if count1 == 0:
#     print("\n💡 RECOMMENDATIONS:")
#     print("   • Try a wider date range (e.g., last 6 months)")
#     print("   • Verify your API key has access to PSScene data")
#     print("   • Check if your coordinates are correct (looks like Baku area)")
#     print("   • Try different item types: 'PSOrthoTile', 'REOrthoTile', 'SkySatCollect'")
# elif count3 == 0 and count1 > 0:
#     print("\n💡 RECOMMENDATION: All images have >30% cloud cover. Use higher threshold!")

# print("\n" + "=" * 70)
# print("📥 DOWNLOADING BEST IMAGE")
# print("=" * 70)

# if count3 > 0:

#     best_image = results3["features"][0]
#     item_id = best_image["id"]
#     acquired_date = best_image["properties"].get("acquired", "unknown")[:10]
    
#     print(f"\n🎯 Selected image: {item_id}")
#     print(f"📅 Acquired: {acquired_date}")
    
#     # Get assets
#     assets_url = f"https://api.planet.com/data/v1/item-types/{ITEM_TYPE}/items/{item_id}/assets/"
#     asset_response = session.get(assets_url)
    
#     if asset_response.status_code == 200:
#         assets = asset_response.json()
        
#         # Try to get visual asset (RGB image)
#         asset_type = None
#         if "ortho_visual" in assets:
#             asset_type = "ortho_visual"
#         elif "basic_analytic_4b" in assets:
#             asset_type = "basic_analytic_4b"
#         elif "ortho_analytic_4b" in assets:
#             asset_type = "ortho_analytic_4b"
#         else:
#             # Use first available asset
#             if(len(assets.keys())>0):
#                 print(assets)
#                 asset_type = list(assets.keys())[0]
        
#                 print(f" Using asset type: {asset_type}")
        
#         asset_info = assets[asset_type]
#         asset_status = asset_info.get("status")
        
#         if asset_status != "active":
#             print(f" Asset status: {asset_status} - Activating...")
#             activate_url = asset_info["_links"]["activate"]
#             session.post(activate_url)

#             import time
#             print(" Waiting for activation (this may take 30-60 seconds)...")
#             for i in range(30):
#                 time.sleep(2)
#                 check_response = session.get(assets_url)
#                 if check_response.status_code == 200:
#                     updated_assets = check_response.json()
#                     if updated_assets[asset_type]["status"] == "active":
#                         print("✅ Asset activated!")
#                         asset_info = updated_assets[asset_type]
#                         break
#                 print(f"   Checking... ({i+1}/30)")
#             else:
#                 print("  Activation taking longer than expected. Check back in a few minutes.")
        
#         # Download if active
#         if asset_info.get("status") == "active":
#             download_url = asset_info["location"]
#             filename = f"planet_{item_id}_{acquired_date}_{asset_type}.tif"
            
#             print(f"\n⬇️  Downloading: {filename}")
#             print("   This may take a few minutes depending on file size...")
            
#             download_response = session.get(download_url, stream=True)
            
#             if download_response.status_code == 200:
#                 total_size = int(download_response.headers.get('content-length', 0))
#                 downloaded = 0
                
#                 with open(filename, 'wb') as f:
#                     for chunk in download_response.iter_content(chunk_size=8192):
#                         if chunk:
#                             f.write(chunk)
#                             downloaded += len(chunk)
#                             if total_size > 0:
#                                 percent = (downloaded / total_size) * 100
#                                 print(f"\r   Progress: {percent:.1f}%", end='', flush=True)
                
#                 print(f"\n\n✅ SUCCESS! Image saved as: {filename}")
#                 print(f"📊 File size: {downloaded / (1024*1024):.2f} MB")
#             else:
#                 print(f"❌ Download failed: {download_response.status_code}")
#         else:
#             print(f"⚠️  Asset not ready. Current status: {asset_info.get('status')}")
#     else:
#         print(f"❌ Failed to get assets: {asset_response.status_code}")
# elif count1 > 0:
#     print("\n💡 No images meet the 30% cloud threshold.")
#     print("   Modify the script to use a higher cloud_cover value to download.")
# else:
#     print("\n❌ No images available to download.")