# Rental Scraper (Zillow & Redfin)

This project extracts rental information from Zillow and Redfin, including location, price, and fees, and stores them in a SQLite database with daily snapshots.

## Features
- **Scrapling-based extraction**: Uses adaptive selectors and stealthy fetchers to bypass anti-bot systems.
- **Address Normalization**: Normalizes addresses for deduplication across sources.
- **Fee Extraction**: Regex-based fee extraction from listing descriptions.
- **SQLite Persistence**: Daily snapshots of prices and fees for historical tracking.
- **Cron-ready**: Designed to be run as a daily cron job.

## Configuration
The scraper is managed via a `.env` file. All platforms (Zillow and Redfin) are searched by default.

- `RENTAL_LOCATIONS`: Comma-separated list of zipcodes (e.g., `95051, 95050`).
- `MIN_PRICE` / `MAX_PRICE`: Numeric price limits.
- `MIN_BEDS` / `MIN_BATHS`: Numeric minimums for rooms.

## Installation
1. Install dependencies:
   ```bash
   pip3 install -r requirements.txt
   ```
2. Initialize the database (optional, will be initialized on first run):
   ```bash
   python database.py
   ```

## Usage
Everything is managed via the `.env` file. Once configured, simply run:
```bash
python3 main.py
```

## Daily Cron Job Setup
To run the scraper daily at 2:00 AM, add the following to your crontab (`crontab -e`):
```cron
0 2 * * * /usr/bin/python3 /Users/bytedance/Documents/rental/main.py >> /Users/bytedance/Documents/rental/scraper.log 2>&1
```

## Database Schema
- `listings`: Base property details. Includes `extra_metadata` (JSON) for platform-specific fields (e.g., Zillow-owned badge, Redfin remarks).
- `prices`: Daily price snapshots.
- `fees`: Extracted fees (pet, application, etc.).
- `raw_snapshots`: Original listing data for re-parsing.

## Improvements and Extensibility
- **Extra Metadata**: All scrapers now populate an `extra_metadata` JSON field, allowing for platform-specific data without schema changes.
- **Scraper Registry**: New platforms (e.g., Apartments.com) can be added by simply creating a new class decorated with `@ScraperRegistry.register`.
- **Flexible Endpoints**: While the current scrapers target HTML list pages, they are designed to be easily swapped for internal APIs (e.g., Zillow's `async-create-search-page-state`) if higher bandwidth is needed.
- **Improved Parsers**: Enhanced extraction for beds, baths, and square footage.
