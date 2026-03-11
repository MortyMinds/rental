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

# Semaphore to limit concurrent network requests across the entire application
# Adjust this based on proxy limits and server respect
MAX_CONCURRENT_REQUESTS = 3
network_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

def persist_listing(listing, update_only=False):
    """
    Inserts or updates a listing in the UNIFIED rentals table.
    If update_only is True, it will only update fields if they were previously NULL.
    """
    # Filter out obvious garbage/navigation listings
    garbage_addresses = ['renter dashboard', 'buy menu', 'unknown address', 'redfin', 'zillow']
    raw_addr = str(listing.get('raw_address', '')).lower()
    if any(g in raw_addr for g in garbage_addresses) and not listing.get('beds'):
        logging.debug(f"Skipping garbage listing: {listing.get('source_id')}")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        if update_only:
            # Targeted update for enrichment: only fill in NULLs
            # Also allow updating canonical_url if it was previously a base domain
            c.execute('''
                UPDATE rentals SET
                    beds = COALESCE(rentals.beds, ?),
                    baths = COALESCE(rentals.baths, ?),
                    sqft = COALESCE(rentals.sqft, ?),
                    canonical_url = CASE 
                        WHEN (
                            rentals.canonical_url = 'https://www.redfin.com' OR 
                            rentals.canonical_url = 'https://www.redfin.com/rentals/renter-dashboard' OR
                            rentals.canonical_url = 'https://www.zillow.com'
                        ) THEN ? 
                        ELSE rentals.canonical_url 
                    END,
                    description = CASE 
                        WHEN (rentals.description IS NULL OR rentals.description = '') THEN ? 
                        ELSE rentals.description 
                    END,
                    last_seen = CURRENT_DATE
                WHERE source = ? AND source_id = ?
            ''', (
                listing.get('beds'), listing.get('baths'), listing.get('sqft'),
                listing['canonical_url'],
                listing.get('description', ''),
                listing['source'], listing['source_id']
            ))
            if c.rowcount > 0:
                logging.info(f"Enriched {listing['source']} listing {listing['source_id']}")
        else:
            # Standard UPSERT
            # SQLite does not support multiple ON CONFLICT clauses in one INSERT.
            # We must handle this by attempting the primary UPSERT and catching failures,
            # or by using a two-step approach. Here we'll use a safer two-step logic.
            
            # Step 1: Try to insert. If it fails due to UNIQUE(source, source_id), it will UPSERT.
            # If it fails due to UNIQUE(canonical_url), we'll handle that manually.
            try:
                c.execute('''
                    INSERT INTO rentals (
                        source, source_id, canonical_url, raw_address, 
                        city, state, zip, beds, baths, sqft, property_type, 
                        price, description, extra_metadata, 
                        first_seen, last_seen
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE, CURRENT_DATE)
                    ON CONFLICT(source, source_id) DO UPDATE SET
                        canonical_url = EXCLUDED.canonical_url,
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
            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed: rentals.canonical_url" in str(e):
                    # Step 2: Handle canonical_url conflict manually if the first attempt failed
                    c.execute('''
                        UPDATE rentals SET
                            source = ?,
                            source_id = ?,
                            last_seen = CURRENT_DATE,
                            beds = COALESCE(?, rentals.beds),
                            baths = COALESCE(?, rentals.baths),
                            sqft = COALESCE(?, rentals.sqft),
                            price = ?
                        WHERE canonical_url = ?
                    ''', (
                        listing['source'], listing['source_id'],
                        listing.get('beds'), listing.get('baths'), listing.get('sqft'),
                        listing.get('price'),
                        listing['canonical_url']
                    ))
                else:
                    raise e
            
            logging.info(f"Persisted {listing['source']} listing {listing['source_id']}")
        
        # 2. Store Raw Snapshot
        c.execute('''
            INSERT INTO raw_snapshots (source, source_id, raw_data, date)
            VALUES (?, ?, ?, CURRENT_DATE)
        ''', (listing['source'], listing['source_id'], json.dumps(listing)))
        
        conn.commit()
        
    except Exception as e:
        logging.error(f"Error persisting {listing['source']} listing {listing['source_id']}: {e}")
        conn.rollback()
    finally:
        conn.close()

async def scrape_platform_for_zip(platform, zip_code, max_pages=5):
    """
    Scrapes a single platform for a specific zip code across multiple pages.
    """
    page = 1
    scraper = ScraperRegistry.get_scraper(platform)
    
    while page <= max_pages:
        url = build_url(platform, zipcode=zip_code, page=page)
        try:
            logging.info(f"Zip {zip_code} | {platform} | Page {page} - Starting scrape for {url}")
            
            # Use the global semaphore for fetching
            async with network_semaphore:
                response = await scraper.fetch(url)
            
            if not response:
                logging.warning(f"Zip {zip_code} | {platform} | Page {page} - No response. Moving to next source.")
                break
            
            # Check for next page link in response (platform-specific logic)
            # For Zillow, we log it in the scraper, but we use page increment here.
            
            listings = scraper.parse(response)
            if not listings:
                logging.info(f"Zip {zip_code} | {platform} | Page {page} - No more listings found.")
                break
            
            logging.info(f"Zip {zip_code} | {platform} | Page {page} - Found {len(listings)} listings")
            for listing in listings:
                # Fallback zip if not parsed from address
                if not listing.get('zip'):
                    listing['zip'] = zip_code
                persist_listing(listing)
            
            # If the platform returned very few listings, it might be the last page
            if len(listings) < 5:
                logging.info(f"Zip {zip_code} | {platform} | Page {page} - Fewer than 5 listings found, assuming last page.")
                break

            page += 1
            # Respectful crawl
            await asyncio.sleep(3) # Increased sleep between pages
            
        except Exception as e:
            logging.error(f"Error for {platform} on zip {zip_code}, page {page}: {e}")
            break

async def run_pipeline(zip_codes, platforms):
    """
    Runs the multi-page scraping pipeline for each zipcode and platform.
    """
    init_db()
    
    for zip_code in zip_codes:
        tasks = []
        for platform in platforms:
            tasks.append(scrape_platform_for_zip(platform, zip_code))
        
        if tasks:
            logging.info(f"Scraping platforms for zip {zip_code}...")
            await asyncio.gather(*tasks, return_exceptions=True)
    
    # After searching all platforms/zips, enrich listings that need more detail
    await enrich_listings()

# Simple container to reuse parsing logic for snapshots
class SnapshotResponse:
    def __init__(self, text): self.text = text
    def get_all_text(self, separator=" "): return self.text

async def enrich_single_listing(listing):
    """
    Enriches a single listing by:
    1. Checking raw snapshots first.
    2. Fetching its detail page if info is still missing.
    """
    platform = listing['source']
    url = listing['canonical_url']
    source_id = listing['source_id']
    
    try:
        scraper = ScraperRegistry.get_scraper(platform)
        
        # --- Phase 1: Try enrichment from existing raw snapshot ---
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('SELECT raw_data FROM raw_snapshots WHERE source_id = ? AND source = ? ORDER BY date DESC LIMIT 1', (source_id, platform))
        row = c.fetchone()
        conn.close()
        
        if row:
            try:
                raw_data = json.loads(row['raw_data'])
                # Some snapshots are adaptors or full text strings
                raw_text = ""
                if isinstance(raw_data, dict):
                    raw_text = raw_data.get('description', '') + " " + str(raw_data.get('extra_metadata', ''))
                else:
                    raw_text = str(raw_data)
                
                # Use SnapshotResponse to reuse the existing scraper parsing logic
                snapshot_details = scraper.parse_detail(SnapshotResponse(raw_text))
                
                # Merge details from snapshot
                updated = False
                for key in ['beds', 'baths', 'sqft']:
                    if listing.get(key) is None and snapshot_details.get(key) is not None:
                        listing[key] = snapshot_details[key]
                        updated = True
                
                if updated:
                    logging.info(f"Enriched {platform} listing {source_id} from raw snapshot")
                    persist_listing(listing, update_only=True)
                    # If we filled all missing info, we can return early
                    if listing.get('beds') is not None and listing.get('baths') is not None and listing.get('sqft') is not None:
                        return
            except Exception as e:
                logging.debug(f"Failed to enrich {source_id} from snapshot: {e}")

        # --- Phase 2: Fetch from URL if info is still missing ---
        logging.info(f"Enriching {platform} listing {source_id} via {url}")
        
        async with network_semaphore:
            response = await scraper.fetch(url)
        
        if not response:
            return
            
        details = scraper.parse_detail(response)
        
        # If the original listing had an incorrect canonical_url (like base domain),
        # and we can find a better one in the description or details, use it.
        # We explicitly ignore generic URLs during this correction.
        generic_urls = [
            'https://www.redfin.com', 'http://www.redfin.com',
            'https://www.redfin.com/rentals/renter-dashboard',
            'http://www.redfin.com/rentals/renter-dashboard',
            'https://www.redfin.com/houses-near-me',
            'http://www.redfin.com/houses-near-me',
            'https://www.zillow.com', 'http://www.zillow.com'
        ]
        
        if listing['canonical_url'].strip('/') in [u.strip('/') for u in generic_urls]:
            import re
            # Try to find a specific property link in the description that isn't generic
            all_links = re.findall(r'\((https?://(?:www\.)?(?:redfin|zillow)\.com/[^\s)]+)\)', listing.get('description', ''))
            for candidate in all_links:
                if candidate.strip('/') not in [u.strip('/') for u in generic_urls]:
                    if any(path in candidate for path in ['/home/', '/apartment/', '/condo/', '/rentals/', '/homedetails/']):
                        listing['canonical_url'] = candidate
                        logging.info(f"Corrected canonical_url for {listing['source_id']} to {listing['canonical_url']}")
                        break

        # Merge details into listing
        # Only update if the detail field is not None
        for key, value in details.items():
            if value is not None:
                listing[key] = value
        
        # Re-parse extra metadata if it was a JSON string from the DB
        if isinstance(listing.get('extra_metadata'), str):
            try:
                listing['extra_metadata'] = json.loads(listing['extra_metadata'])
            except:
                listing['extra_metadata'] = {}
        
        # Persist updated listing with update_only=True
        persist_listing(listing, update_only=True)
        
        # Shorter delay since we have a global semaphore
        await asyncio.sleep(1)
        
    except Exception as e:
        logging.error(f"Error enriching listing {listing['source_id']}: {e}")

async def enrich_listings():
    """
    Fetches detail pages for listings that have missing specs (baths, sqft) concurrently.
    """
    logging.info("Starting enrichment phase for listings with missing details...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Target listings with missing baths or sqft that were seen in db
    # Also include listings with likely incorrect/base canonical URLs
    c.execute('''
        SELECT * FROM rentals 
        WHERE (
            (baths IS NULL OR sqft IS NULL OR beds IS NULL)
            OR canonical_url = 'https://www.redfin.com'
            OR canonical_url = 'https://www.redfin.com/rentals/renter-dashboard'
            OR canonical_url = 'https://www.zillow.com'
        )
    ''')
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        logging.info("No listings found needing enrichment.")
        return

    logging.info(f"Found {len(rows)} listings to enrich concurrently...")
    
    tasks = [enrich_single_listing(dict(row)) for row in rows]
    await asyncio.gather(*tasks, return_exceptions=True)

async def main():
    zip_codes_env = os.getenv("RENTAL_ZIPCODES")
    if not zip_codes_env:
        logging.error("RENTAL_ZIPCODES environment variable is not set.")
        return
    zip_codes = [z.strip() for z in zip_codes_env.split(",") if z.strip()]
    
    platforms_env = os.getenv("PLATFORMS", "zillow,redfin")
    platforms = [p.strip() for p in platforms_env.split(",") if p.strip()]
    
    logging.info(f"Targeting platforms: {platforms} for zipcodes: {zip_codes}")
    await run_pipeline(zip_codes, platforms)

if __name__ == "__main__":
    asyncio.run(main())
