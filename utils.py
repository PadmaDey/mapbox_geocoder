import os
import asyncio
from fuzzywuzzy import fuzz
from dotenv import load_dotenv
from config import output_dir

load_dotenv()
MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY")

semaphore = asyncio.Semaphore(10)

def standardize_permit_type(value):
    val = str(value).lower()
    if any(fuzz.partial_ratio(val, kw.lower()) > 80 for kw in ["Residential", "Family", "Home"]):
        return "Residential"
    elif any(fuzz.partial_ratio(val, kw.lower()) > 80 for kw in ["Commercial", "Com"]):
        return "Commercial"
    elif fuzz.partial_ratio(val, "Building") > 80:
        return "Commercial"
    else:
        return value

async def fetch_lat_lon(session, address):
    async with semaphore:
        try:
            url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json"
            params = {"access_token": MAPBOX_API_KEY}
            async with session.get(url, params=params) as response:
                data = await response.json()
                coords = data['features'][0]['center']
                return coords[1], coords[0]
        except Exception:
            return None, None

async def fetch_lat_lon_batch(session, addresses, batch_size=100, delay=1):
    results = []
    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i + batch_size]
        tasks = [fetch_lat_lon(session, addr.replace(" ", "%20")) for addr in batch]
        batch_results = await asyncio.gather(*tasks)
        results.extend(batch_results)
        await asyncio.sleep(delay)
    return results

async def process_sheet(sheet_name, df, session):
    df['permit_type'] = df['permit_type'].apply(standardize_permit_type)
    df['full_address'] = df[['street_address', 'county', 'state', 'country']].astype(str).agg(', '.join, axis=1)

    addresses = df['full_address'].tolist()
    lat_lon_results = await fetch_lat_lon_batch(session, addresses)

    df['latitude'], df['longitude'] = zip(*lat_lon_results)
    df['lat-lon'] = df[['latitude', 'longitude']].astype(str).agg(','.join, axis=1)

    csv_path = os.path.join(output_dir, f"{sheet_name.replace('/', '_')}.csv")
    df.to_csv(csv_path, index=False)
    return csv_path
