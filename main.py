import sqlite3
import logging
import asyncio
import os
from database import DB_PATH, init_db
from scraper import ScraperRegistry
from utils import build_url
from datetime import datetime
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def persist_listing(listing):
    """
    Inserts or updates a listing in the UNIFIED rentals table.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        # UPSERT into the unified rentals table
        c.execute('''
            INSERT INTO rentals (
                source, source_id, canonical_url, raw_address, 
                city, state, zip, beds, baths, sqft, property_type, 
                price, description, extra_metadata, 
                first_seen, last_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE, CURRENT_DATE)
            ON CONFLICT(source, source_id) DO UPDATE SET
                last_seen = EXCLUDED.last_seen,
                raw_address = EXCLUDED.raw_address,
                beds = COALESCE(EXCLUDED.beds, rentals.beds),
                baths = COALESCE(EXCLUDED.baths, rentals.baths),
                sqft = COALESCE(EXCLUDED.sqft, rentals.sqft),
                property_type = EXCLUDED.property_type,
                price = EXCLUDED.price,
                description = COALESCE(EXCLUDED.description, rentals.description),
                extra_metadata = EXCLUDED.extra_metadata
        ''', (
            listing['source'], listing['source_id'], listing['canonical_url'], 
            listing['raw_address'] or "Unknown Address",
            listing.get('city'), listing.get('state'), listing.get('zip'),
            listing.get('beds'), listing.get('baths'), listing.get('sqft'),
            listing.get('property_type', 'house'),
            listing.get('price'),
            listing.get('description', ''), 
            json.dumps(listing.get('extra_metadata', {}))
        ))
        
        # 2. Store Raw Snapshot
        c.execute('''
            INSERT INTO raw_snapshots (source, source_id, raw_data, date)
            VALUES (?, ?, ?, CURRENT_DATE)
        ''', (listing['source'], listing['source_id'], json.dumps(listing)))
        
        conn.commit()
        logging.info(f"Persisted {listing['source']} listing {listing['source_id']}")
        
    except Exception as e:
        logging.error(f"Error persisting {listing['source']} listing {listing['source_id']}: {e}")
        conn.rollback()
    finally:
        conn.close()

async def run_pipeline(zip_codes, platforms):
    """
    Runs the multi-page scraping pipeline for each zipcode and platform.
    """
    init_db()
    max_pages = 2 # Change to scan more pages
    
    for zip_code in zip_codes:
        for platform in platforms:
            page = 1
            while page <= max_pages:
                url = build_url(platform, zipcode=zip_code, page=page)
                try:
                    logging.info(f"Page {page} - Starting {platform} scrape for {url}")
                    scraper = ScraperRegistry.get_scraper(platform)
                    response = await scraper.fetch(url)
                    
                    if not response:
                        logging.warning(f"No response for {platform} page {page}. Moving to next source.")
                        break # Go to next platform/zip
                        
                    # Handle some errors that might be in the text instead of status
                    if "401 Unauthorized" in str(response) or "403 Forbidden" in str(response):
                        logging.warning(f"Likely blocked on page {page}. Retrying in 10s...")
                        await asyncio.sleep(10)
                        # Maybe it will work next time? Try once.
                        response = await scraper.fetch(url)
                        if not response: break

                    listings = scraper.parse(response)
                    if not listings:
                        logging.info(f"No more listings found for {platform} on page {page}.")
                        break # End pagination for this search
                    
                    logging.info(f"Page {page} - Found {len(listings)} listings from {platform}")
                    for listing in listings:
                        # Fallback zip if not parsed from address
                        if not listing.get('zip'):
                            listing['zip'] = zip_code
                        persist_listing(listing)
                    
                    # If we found only a few listings, might be the last page
                    if len(listings) < 10:
                        logging.debug("Fewer than 10 listings found, likely last page.")
                        break

                    page += 1
                    # Respectful crawl
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logging.error(f"Error on page {page} for {platform}: {e}")
                    break

async def main():
    zip_codes_env = os.getenv("RENTAL_ZIPCODES", "95051")
    zip_codes = [z.strip() for z in zip_codes_env.split(",") if z.strip()]
    
    platforms_env = os.getenv("PLATFORMS", "zillow,redfin")
    platforms = [p.strip() for p in platforms_env.split(",") if p.strip()]
    
    logging.info(f"Targeting platforms: {platforms} for zipcodes: {zip_codes}")
    await run_pipeline(zip_codes, platforms)

if __name__ == "__main__":
    asyncio.run(main())
