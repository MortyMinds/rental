from scrapling.fetchers import StealthyFetcher
import logging
import asyncio
import re
from utils import parse_address, extract_property_type

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ScraperRegistry:
    """Registry to manage different scrapers for different platforms."""
    _scrapers = {}

    @classmethod
    def register(cls, platform_name):
        def decorator(scraper_class):
            cls._scrapers[platform_name.lower()] = scraper_class
            return scraper_class
        return decorator

    @classmethod
    def get_scraper(cls, platform_name):
        scraper_class = cls._scrapers.get(platform_name.lower())
        if not scraper_class:
            raise ValueError(f"No scraper registered for platform: {platform_name}")
        return scraper_class()

class BaseScraper:
    def __init__(self, source: str):
        self.source = source
        
    async def fetch(self, url: str):
        """
        Fetch the HTML content of the page with multiple fallback prefixes.
        Sequence: markdown.new -> r.jina.ai -> defuddle.md -> raw url
        """
        import httpx
        from scrapling import Selector
        
        prefixes = [
            "https://markdown.new/",
            "https://r.jina.ai/",
            "https://defuddle.md/",
            "" # Raw URL fallback
        ]
        
        # Browser-imitation headers based on debug success
        browser_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate", # Removed 'br' to avoid brotli decoding issues
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Referer": "https://www.google.com/"
        }
        
        last_error = None
        for prefix in prefixes:
            current_url = f"{prefix}{url}" if prefix else url
            logging.info(f"Trying to fetch {self.source} via: {current_url}")
            
            try:
                # Use a more realistic browser-like user agent
                async with httpx.AsyncClient(timeout=60.0, follow_redirects=True, headers=browser_headers) as client:
                    resp = await client.get(current_url)
                    text = resp.text
                    status = resp.status_code
                    
                    # Log snippet if it's very short
                    if text and len(text) < 100:
                        logging.debug(f"Tiny response from {current_url}: '{text}'")

                # Check if we got a valid response
                # Relaxed length check to 300 to capture shorter but valid pages
                is_valid = status == 200 and text and text != "None" and len(text) > 300
                # Check for block markers in proxy responses
                # Case-insensitive check
                block_markers = ["forbidden", "access denied", "captcha", "press & hold", "human verification", "robot", "confirm you are human", "blocked"]
                text_lower = text.lower() if text else ""
                
                # If we find actual listing markers, ignore the captcha markers (Zillow often hides data behind overlay)
                has_listings = any(m in text_lower for m in ['property-card', 'v2-home-card', 'data-test="property-card"', 'list-card_for-rent'])
                
                if any(marker in text_lower for marker in block_markers) and not has_listings:
                    logging.warning(f"Fetch via '{prefix}' returned a block/captcha page. Skipping.")
                    continue
                        
                if is_valid:
                    logging.info(f"Successfully fetched {self.source} using prefix: '{prefix}' (Found Listings: {has_listings})")
                    # Wrap in Selector to maintain compatibility with .css() calls
                    return Selector(text)
                else:
                    logging.warning(f"Fetch via '{prefix}' failed. Status: {status}, Length: {len(text) if text else 0}")
            except Exception as e:
                logging.warning(f"Error fetching via '{prefix}': {e}")
                last_error = e
                
        logging.error(f"All fetch attempts failed for {url}. Last error: {last_error}")
        return None

    def parse(self, response):
        """
        Should return a list of listing dictionaries from a search result page.
        """
        raise NotImplementedError("Subclasses must implement parse()")

    def parse_detail(self, response) -> dict:
        """
        Should return a dictionary of fields from a detail page.
        """
        raise NotImplementedError("Subclasses must implement parse_detail()")

    def _clean_price(self, price_str: str) -> int:
        if not price_str:
            return 0
        cleaned = "".join(filter(str.isdigit, price_str))
        return int(cleaned) if cleaned else 0

    def _extract_sqft(self, text: str) -> int | None:
        """
        Enhanced sqft extraction with multiple strategies, ordered by reliability.
        Handles various formats found in Redfin/Zillow detail pages.
        """
        import re
        text_lower = text.lower() if text else ""
        
        # Strategy 1: JSON-LD / structured data (most reliable)
        # Look for "livingArea":1200 or "floorSize":{"value":"1200"}
        json_patterns = [
            r'"livingarea"\s*:\s*(\d+)',
            r'"floorsize"\s*:\s*\{[^}]*"value"\s*:\s*"?(\d+)',
            r'"living_area"\s*:\s*(\d+)',
            r'"sqft"\s*:\s*(\d+)',
        ]
        for pat in json_patterns:
            m = re.search(pat, text_lower)
            if m:
                try:
                    val = int(m.group(1))
                    if 100 <= val <= 50000:  # Sanity check
                        logging.debug(f"sqft extracted via JSON pattern: {val}")
                        return val
                except: pass
        
        # Strategy 2: Structured line pattern "N bed(s) N bath(s) N,NNN sq ft"
        # This is the most common format on detail pages and avoids nearby listing pollution
        structured = re.search(
            r'(\d+)\s*(?:beds?|bd)\s+(\d[\d.]*)\s*(?:baths?|ba)\s+([\d,]+)\s*(?:sq\s*ft|sqft)',
            text_lower, re.I
        )
        if structured:
            try:
                val = int(structured.group(3).replace(',', ''))
                if 100 <= val <= 50000:
                    logging.debug(f"sqft extracted via structured bed/bath/sqft pattern: {val}")
                    return val
            except: pass
        
        # Strategy 3: Labeled patterns ("Size: 1,200 sq ft", "Living Area: 950 Sq. Ft.")
        labeled_patterns = [
            r'(?:size|area|living\s*(?:area|space)|floor\s*(?:size|area))\s*[:\-|]\s*([\d,]+)\s*(?:sq|sf)',
            r'(?:size|area|living\s*(?:area|space)|floor\s*(?:size|area))\s*[:\-|]\s*([\d,]+)',
        ]
        for pat in labeled_patterns:
            m = re.search(pat, text_lower)
            if m:
                try:
                    val = int(m.group(1).replace(',', ''))
                    if 100 <= val <= 50000:
                        logging.debug(f"sqft extracted via labeled pattern: {val}")
                        return val
                except: pass
        
        # Strategy 4: Standard "N sqft" / "N sq ft" pattern (first match, not findall)
        # Use word boundary to avoid matching partial numbers
        # For ranges like "758 - 1,378 sqft", capture the first number
        standard_patterns = [
            r'([\d,]+)\s*[-–—]\s*[\d,]+\s*(?:sqft|sq\.?\s*ft\.?|square\s*feet)\b',  # Range: pick first
            r'([\d,]+)\s*(?:sqft|sq\.?\s*ft\.?|square\s*feet)\b',  # Single value
            r'([\d,]+)\s*sf\b',
        ]
        for pat in standard_patterns:
            m = re.search(pat, text_lower)
            if m:
                try:
                    val = int(m.group(1).replace(',', ''))
                    if 100 <= val <= 50000:
                        logging.debug(f"sqft extracted via standard pattern: {val}")
                        return val
                except: pass
        
        # Strategy 5: Collect all sqft mentions and pick the most likely one
        # For apartment complexes, prefer the first reasonable value
        all_sqft = re.findall(r'([\d,]+)\s*(?:sqft|sq\.?\s*ft\.?|square\s*feet|sf)\b', text_lower)
        if all_sqft:
            try:
                vals = [int(v.replace(',', '')) for v in all_sqft if v.replace(',', '').isdigit()]
                # Filter to reasonable range
                vals = [v for v in vals if 100 <= v <= 50000]
                if vals:
                    # For detail pages, the first reasonable value is usually the property's own sqft
                    logging.debug(f"sqft extracted via findall (first reasonable): {vals[0]}")
                    return vals[0]
            except: pass
        
        return None

@ScraperRegistry.register("zillow")
class ZillowScraper(BaseScraper):
    """
    Scraper for Zillow rental listings.
    Note: For higher volume, consider using the internal search API:
    https://www.zillow.com/async-create-search-page-state
    """
    def __init__(self):
        super().__init__("zillow")

    def parse(self, response):
        listings = []
        try:
            # Check for pagination / next page
            next_page_link = response.css('a[rel="next"]::attr(href)').get()
            if next_page_link:
                logging.info(f"Zillow: Found next page link: {next_page_link}")
            # Try different selectors for listing cards
            cards = response.css('[data-test="property-card"]')
            if not cards:
                cards = response.css('.property-card')
            if not cards:
                cards = response.css('.list-card_for-rent')
            if not cards:
                cards = response.css('article')
            if not cards:
                cards = response.css('.list-card')

            if cards:
                logging.info(f"Zillow: Found {len(cards)} cards with primary selectors")
            else:
                # FALLBACK: Try parsing from Markdown/Raw Text
                import re
                try:
                    full_text = response.get_all_text(separator="\n")
                except:
                    # If it's a raw Markdown string, .get_all_text() might fail
                    full_text = str(response)
                
                # Split by likely separators: price patterns, horizontal lines, or multiple newlines
                potential_listings = re.split(r'\n(?=\$\d{1,3}(?:,\d{3})*)|\n---\n', full_text)
                if len(potential_listings) > 1:
                    logging.info(f"Zillow: Using text-fallback parsing for {len(potential_listings)} items")
                    for text_item in potential_listings:
                        if ('$' not in text_item and 'sqft' not in text_item) or len(text_item) < 100: continue
                        listings.append(self._parse_zillow_text_item(text_item))
                    return listings

            for card in cards:
                # Link
                url = (card.css('a[data-test="property-card-link"]::attr(href)').get() or 
                       card.css('a::attr(href)').get())
                if not url:
                    logging.debug(f"Zillow: Card skipped, no URL found. Text: {card.get_all_text()[:50]}")
                    continue
                
                if url.startswith('/'):
                    url = "https://www.zillow.com" + url
 
                # Aggregate card text for robust parsing
                card_text = card.get_all_text(separator=" | ")
                
                # Robust Source ID extraction
                import re
                source_id = ""
                zpid_match = re.search(r'(\d+)_zpid', url)
                if zpid_match:
                    source_id = zpid_match.group(1)
                else:
                    parts = [p for p in url.split('/') if p]
                    if parts:
                        source_id = parts[-1]
                
                if not source_id or source_id == "homedetails":
                    import hashlib
                    source_id = hashlib.md5(url.encode()).hexdigest()[:10]

                # Regex Fallbacks for Price, Beds, Baths, Sqft
                # Search in the full card text
                full_text = card.get_all_text(separator=" ").lower()
                
                price_match = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:\+)?(?:\/mo)?)', full_text)
                price_str = price_match.group(1) if price_match else None
                
                # Beds: look for "1 bd", "Studio", "2 beds", "1bd"
                # More specific than \d+ to avoid capturing years or addresses
                beds_match = re.search(r'(\d+|\b(?:studio)\b)\s*(?:bd|beds?|bedroom)(?:\+)?', full_text, re.I)
                if not beds_match:
                    # Try studio without bd suffix
                    beds_match = re.search(r'\b(studio)\b', full_text, re.I)
                
                beds_val = beds_match.group(1) if beds_match else None
                if beds_val and beds_val.lower() == 'studio':
                    beds = 0.0
                else:
                    try:
                        beds = float(beds_val) if beds_val else None
                    except:
                        beds = None
                
                # Baths: look for "1 ba", "1.5 baths", "2 bathrooms", "1ba", "1 bath"
                baths_match = re.search(r'([\d.]+)\s*(?:ba|baths?|bathrooms?)', full_text, re.I)
                if not baths_match:
                    # Try a broader search in all text if the primary separator split it
                    baths_match = re.search(r'([\d.]+)\s*(?:ba|baths?|bathrooms?)', card.get_all_text(), re.I)
                
                try:
                    baths = float(baths_match.group(1)) if baths_match else None
                except:
                    baths = None
                
                # Sqft: look for "1,200 sqft", "500 sq ft", "1200sgft", "1,200 sf", "square feet"
                sqft_match = re.search(r'([\d,.]+)\s*(?:sqft|sq\s*ft|square\s*feet|sf)', full_text, re.I)
                if not sqft_match:
                    sqft_match = re.search(r'([\d,.]+)\s*(?:sqft|sq\s*ft|square\s*feet|sf)', card.get_all_text(), re.I)

                try:
                    if sqft_match:
                        sqft_val = sqft_match.group(1).replace(',', '')
                        # Handle case like "1.2k sqft" if it ever appears, but mostly just float/int
                        sqft = int(float(sqft_val))
                    else:
                        sqft = None
                except:
                    sqft = None

                # Double extraction for sqft if still None
                if sqft is None:
                    sqft_text = card.attrib.get('title') or ""
                    sqft_match = re.search(r'([\d,]{3,})\s*(?:sqft|sq\s*ft)', sqft_text, re.I)
                    if not sqft_match:
                        # Try all text again with a more aggressive regex
                        all_text = card.get_all_text()
                        sqft_match = re.search(r'([\d,]+)\s*(?:sqft|sq\s*ft|square\s*feet)', all_text, re.I)
                    
                    if sqft_match:
                        try:
                            sqft = int(sqft_match.group(1).replace(',', ''))
                        except:
                            pass

                # Address Fallback
                raw_address = (card.css('address::text').get() or 
                               card.css('[data-test="property-card-addr"]::text').get() or
                               card.css('.property-card-address::text').get())
                if not raw_address:
                    addr_match = re.search(r'\|\s*([^|]{10,100})', card_text)
                    raw_address = addr_match.group(1).strip() if addr_match else "Unknown Address"

                # Description Fallback
                description = (card.css('.property-card-subtitle::text').get() or 
                               card.css('[data-test="property-card-subtitle"]::text').get() or
                               "")
                if not description:
                    # Take a longer snippet of the card text as description
                    description = card_text.replace("|", " ").strip()[:2000]

                listing = {
                    'source': 'zillow',
                    'source_id': source_id,
                    'canonical_url': url,
                    'raw_address': raw_address,
                    'price': price_str,
                    'beds': beds,
                    'baths': baths,
                    'sqft': sqft,
                    'description': description,
                    'extra_metadata': {
                        'is_zillow_owned': card.css('.zillow-owned-badge').get() is not None,
                        'badge_text': card.css('.property-card-badge::text').get()
                    }
                }
                
                listing['price'] = self._clean_price(listing['price'])
                
                # Extract city, state, zip
                listing['city'], listing['state'], listing['zip'] = parse_address(listing['raw_address'], url=listing.get('canonical_url'))
                
                # Extract property type
                listing['property_type'] = extract_property_type(listing['canonical_url'], listing['description'], listing.get('raw_address', ''))
                
                # Final refinement: if beds/baths/sqft are missing, try regex on description
                desc_text = listing['description'].lower()
                if listing['beds'] is None:
                    m = re.search(r'(\d+|studio)\s*(?:bd|beds?|bedroom)', desc_text)
                    if m: listing['beds'] = 0.0 if m.group(1) == 'studio' else float(m.group(1))
                if listing['baths'] is None:
                    m = re.search(r'([\d.]+)\s*(?:ba|baths?|bathrooms?)', desc_text)
                    if m: listing['baths'] = float(m.group(1))
                if listing['sqft'] is None:
                    m = re.search(r'([\d,]+)\s*(?:sqft|sq\s*ft)', desc_text)
                    if m: listing['sqft'] = int(m.group(1).replace(',', ''))

                # Final Address Refinement for State/Zip
                if not listing['state'] or not listing['zip']:
                    # Try parsing from both raw_address and description
                    full_addr_test = f"{listing['raw_address']} {listing['description']}"
                    _, state, zip_c = parse_address(full_addr_test)
                    if not listing['state']: listing['state'] = state
                    if not listing['zip']: listing['zip'] = zip_c

                logging.info(f"Parsed Zillow: ID={listing['source_id']} | Price={listing['price']} | Beds={listing['beds']} | Baths={listing['baths']} | Addr={listing['raw_address'][:30]}...")
                listings.append(listing)
        except Exception as e:
            logging.error(f"Error parsing Zillow response: {e}")
            
        return listings

    def parse_detail(self, response) -> dict:
        """Parses a Zillow detail page for missing specs and address."""
        try:
            text = response.get_all_text(separator=" | ")
        except:
            text = str(response)
            
        full_text = text.lower()
        
        # Reject too-short responses (anti-bot empty shells)
        if len(text) < 500:
            logging.warning(f"Zillow detail page too short ({len(text)} chars). Likely blocked.")
            return {}
        
        # Check for CAPTCHA/Block
        if "confirm you are human" in full_text or "captcha" in full_text or "security check" in full_text:
            logging.warning("Zillow detail page returned CAPTCHA/Block. Skipping.")
            return {}

        # 1. Address Extraction from detail page
        raw_address = None
        patterns = [
            r'([^|]+)\s*\|\s*([^,]+),\s*([A-Z]{2})\s*(\d{5})',
            r'([^,]+),\s*([^,]+),\s*([A-Z]{2})\s*(\d{5})',
            r'address\s*:\s*([^|,\n]+(?:,\s*[^|,\n]+){2,3})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                if len(match.groups()) >= 4:
                    raw_address = f"{match.group(1).strip()}, {match.group(2).strip()}, {match.group(3).strip()} {match.group(4).strip()}"
                else:
                    raw_address = match.group(1).strip()
                break

        # 2. Specs Extraction
        beds_match = re.search(r'(\d+|\b(?:studio)\b)\s*(?:bd|beds?|bedroom)', full_text, re.I)
        baths_match = re.search(r'([\d.]+)\s*(?:ba|baths?|bathrooms?)', full_text, re.I)
        
        beds = None
        if beds_match:
            beds = 0.0 if beds_match.group(1).lower() == 'studio' else float(beds_match.group(1))
        
        # Aggressive baths search for apartment complexes
        baths = None
        if baths_match:
            try:
                baths = float(baths_match.group(1))
            except: pass
        
        if baths is None:
            m = re.search(r'([\d.]+)\s*(?:-\s*[\d.]+\s*)?ba', full_text)
            if m: 
                try: baths = float(m.group(1))
                except: pass
        
        if baths is None:
            all_baths = re.findall(r'([\d.]+)\s*(?:ba|baths?|bathrooms?)', full_text)
            if all_baths:
                try:
                    vals = [float(v) for v in all_baths if v.replace('.','',1).isdigit()]
                    if vals: baths = min(vals)
                except: pass
  
        # --- Enhanced sqft extraction (ordered by reliability) ---
        sqft = self._extract_sqft(full_text)
  
        details = {
            'beds': beds,
            'baths': baths,
            'sqft': sqft,
            'description': text[:2000]
        }
        if raw_address:
            details['raw_address'] = raw_address
            from utils import parse_address
            details['city'], details['state'], details['zip'] = parse_address(raw_address)
            
        return details

    def _parse_zillow_text_item(self, text: str):
        """Helper to parse a single listing from a text/markdown chunk."""
        import re
        import hashlib
        
        # Link extraction from markdown [text](url)
        link_match = re.search(r'\[.*?\]\((https://www\.zillow\.com/.*?)\)', text)
        url = link_match.group(1) if link_match else ""
        
        # Address extraction FIRST
        raw_address = ""
        # 1. Try markdown link text
        link_text_match = re.search(r'\[(.*?)\]\(https://www\.zillow\.com', text)
        if link_text_match:
            raw_address = link_text_match.group(1).strip()

        # 2. If empty or looks like a price/junk, try lines
        if not raw_address or '$' in raw_address or len(raw_address) < 5 or 'Zillow' in raw_address:
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            for line in lines:
                # Basic address-like check
                if '$' not in line and not line.startswith('http') and len(line) > 5 and 'Title:' not in line:
                    raw_address = line.replace('###', '').strip()
                    break
        
        if not raw_address: raw_address = "Unknown Address"

        source_id = ""
        if url and 'homedetails' in url:
            zpid_match = re.search(r'(\d+)_zpid', url)
            source_id = zpid_match.group(1) if zpid_match else url.split('/')[-1]
        
        if not source_id:
            # Use hash of address to deduplicate if URL is missing or generic
            source_id = hashlib.md5(f"zillow_{raw_address.lower()}".encode()).hexdigest()[:10]

        # Price
        price_match = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:\+)?(?:\/mo)?)', text)
        price_str = price_match.group(1) if price_match else None
        
        # Specs
        beds_match = re.search(r'(\b\d+|\b(?:studio)\b)\s*(?:bd|beds?|bedroom)(?:\+)?', text, re.I)
        if not beds_match:
            beds_match = re.search(r'\b(studio)\b', text, re.I)
        
        beds_val = beds_match.group(1) if beds_match else None
        if beds_val and str(beds_val).lower() == 'studio':
            beds = 0.0
        else:
            try:
                beds = float(beds_val) if beds_val else None
            except:
                beds = None
        
        baths_match = re.search(r'([\d.]+)\s*(?:ba|baths?|bathrooms?)', text, re.I)
        try:
            baths = float(baths_match.group(1)) if baths_match else None
        except:
            baths = None
        
        sqft_match = re.search(r'([\d,.]+)\s*(?:sqft|sq\s*ft|square\s*feet|sf)', text, re.I)
        try:
            if sqft_match:
                sqft_val = sqft_match.group(1).replace(',', '')
                sqft = int(float(sqft_val))
            else:
                sqft = None
        except:
            sqft = None
        
        listing = {
            'source': 'zillow',
            'source_id': source_id,
            'canonical_url': url or "https://www.zillow.com",
            'raw_address': raw_address,
            'price': self._clean_price(price_str),
            'beds': beds,
            'baths': baths,
            'sqft': sqft,
            'description': text[:200].replace('\n', ' '),
            'extra_metadata': {'parsed_from': 'text_fallback'}
        }
        
        # Extract city, state, zip
        listing['city'], listing['state'], listing['zip'] = parse_address(listing['raw_address'], url=listing.get('canonical_url'))
        
        # Address Refinement
        if not listing['state'] or not listing['zip']:
            full_addr_test = f"{listing['raw_address']} {text}"
            _, state, zip_c = parse_address(full_addr_test)
            if not listing['state']: listing['state'] = state
            if not listing['zip']: listing['zip'] = zip_c
            
        # Extract property type
        listing['property_type'] = extract_property_type(listing['canonical_url'], text, listing.get('raw_address', ''))
        
        logging.info(f"Text-Parsed Zillow: ID={listing['source_id']} | Price={listing['price']} | Beds={listing['beds']} | Addr={listing['raw_address'][:30]}...")
        return listing

@ScraperRegistry.register("redfin")
class RedfinScraper(BaseScraper):
    """
    Scraper for Redfin rental listings.
    Note: Redfin's search results can also be fetched via their Map API for bulk data.
    """
    def __init__(self):
        super().__init__("redfin")

    def parse(self, response):
        listings = []
        try:
            card_selectors = ['.HomeCardContainer', '.bottomV2', 'article', '.v2-home-card', '[data-test="property-card"]']
            cards = []
            for s in card_selectors:
                cards = response.css(s)
                if cards:
                    logging.info(f"Redfin: Found {len(cards)} cards with selector '{s}'")
                    break

            if not cards:
                # FALLBACK: Try parsing from Markdown/Raw Text
                import re
                try:
                    full_text = response.get_all_text(separator="\n")
                except:
                    full_text = str(response)
                
                # Redfin listings in Markdown often split by price, horizontal lines, or address-looking headers
                potential_listings = re.split(r'\n(?=\$\d{1,3}(?:,\d{3})*)|\n---\n', full_text)
                if len(potential_listings) > 1:
                    logging.info(f"Redfin: Using text-fallback parsing for {len(potential_listings)} items")
                    for text_item in potential_listings:
                        if ('$' not in text_item and 'sqft' not in text_item) or len(text_item) < 100: continue
                        listings.append(self._parse_redfin_text_item(text_item))
                    return listings

            for card in cards:
                url_path = card.css('a::attr(href)').get()
                if not url_path: continue
                
                url = "https://www.redfin.com" + url_path
                card_text = card.get_all_text(separator=" | ")
                
                # Redfin Source ID
                import re
                source_id = ""
                id_match = re.search(r'/home/(\d+)$', url_path)
                if id_match:
                    source_id = id_match.group(1)
                else:
                    parts = [p for p in url_path.split('/') if p]
                    source_id = parts[-1] if parts else ""
                
                if not source_id:
                    import hashlib
                    source_id = hashlib.md5(url.encode()).hexdigest()[:10]

                # Regex Fallbacks
                price_str = card.css('[data-test="property-card-price"]::text').get() or card.css('.property-card-price::text').get()
                beds = self._parse_numeric(card.css('.property-card-common-info::text').re_first(r'(\d+)\s*bd'))
                baths = self._parse_numeric(card.css('.property-card-common-info::text').re_first(r'(\d+(?:\.\d+)?)\s*ba'))
                sqft = self._parse_numeric(card.css('.property-card-common-info::text').re_first(r'(\d+(?:,\d+)?)\s*sqft'))

                # Fallback to regex if data-test selectors didn't work
                if not price_str:
                    price_match = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:\+)?(?:\/mo)?)', card_text)
                    price_str = price_match.group(1) if price_match else None
                
                if beds is None:
                    beds_match = re.search(r'(\d+|Studio)\s*(?:bd|beds?|bedroom)', card_text, re.I)
                    beds_val = beds_match.group(1) if beds_match else "0"
                    beds = 0 if str(beds_val).lower() == 'studio' else float(beds_val) if beds_val.replace('.','',1).isdigit() else 0
                
                if baths is None:
                    baths_match = re.search(r'([\d.]+)\s*(?:ba|baths?|bathrooms?)', card_text, re.I)
                    baths = float(baths_match.group(1)) if (baths_match and baths_match.group(1).replace('.','',1).isdigit()) else 0
                
                if sqft is None:
                    sqft_match = re.search(r'([\d,]+)\s*(?:sqft|sq\s*ft)', card_text, re.I)
                    sqft_val = sqft_match.group(1).replace(',', '') if sqft_match else None
                    sqft = int(sqft_val) if sqft_val and sqft_val.isdigit() else None

                # Double extraction for sqft
                if sqft is None:
                    all_text = card.get_all_text()
                    sqft_match = re.search(r'([\d,]+)\s*(?:sqft|sq\s*ft|square\s*feet)', all_text, re.I)
                    if sqft_match:
                        try:
                            sqft = int(sqft_match.group(1).replace(',', ''))
                        except: pass

                description = card.css('.remarks::text').get() or ""
                if not description:
                    description = card_text.replace("|", " ").strip()[:100]

                listing = {
                    'source': 'redfin',
                    'source_id': source_id,
                    'canonical_url': url,
                    'raw_address': (card.css('[data-test="property-card-addr"]::text').get() or 
                                   card.css('.property-card-addr::text').get() or
                                   card.css('.homeAddress::text').get() or 
                                   card.css('[data-test-name="address"]::text').get() or
                                   card.css('.address::text').get() or
                                   card.css('.home-address::text').get() or "Unknown Address"),
                    'price': price_str,
                    'beds': beds,
                    'baths': baths,
                    'sqft': sqft,
                    'description': description,
                    'extra_metadata': {
                        'listing_remarks': card.css('.remarks::text').get()
                    }
                }
                
                listing['price'] = self._clean_price(listing['price'])
                
                # Extract city, state, zip
                listing['city'], listing['state'], listing['zip'] = parse_address(listing['raw_address'], url=listing.get('canonical_url'))
                
                # Extract property type
                listing['property_type'] = extract_property_type(listing['canonical_url'], listing['description'], listing.get('raw_address', ''))
                
                logging.info(f"Parsed Redfin: ID={listing['source_id']} | Price={listing['price']} | Beds={listing['beds']} | Baths={listing['baths']} | Addr={listing['raw_address'][:30]}...")
                listings.append(listing)
        except Exception as e:
            logging.error(f"Error parsing Redfin response: {e}")
            
        return listings

    def parse_detail(self, response) -> dict:
        """Parses a Redfin detail page for missing specs and address."""
        try:
            text = response.get_all_text(separator=" | ")
        except:
            text = str(response)
            
        full_text = text.lower()
        
        # Reject too-short responses (anti-bot empty shells)
        if len(text) < 500:
            logging.warning(f"Redfin detail page too short ({len(text)} chars). Likely blocked.")
            return {}
        
        # Check for CAPTCHA/Block
        if "confirm you are human" in full_text or "captcha" in full_text or "security check" in full_text or "human verification" in full_text:
            logging.warning("Redfin detail page returned CAPTCHA/Block. Skipping.")
            return {}

        # 1. Address Extraction
        raw_address = None
        patterns = [
            r'([^|]+)\s*\|\s*([^,]+),\s*([A-Z]{2})\s*(\d{5})',
            r'([^,]+),\s*([^,]+),\s*([A-Z]{2})\s*(\d{5})',
            r'address\s*:\s*([^|,\n]+(?:,\s*[^|,\n]+){2,3})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                if len(match.groups()) >= 4:
                    raw_address = f"{match.group(1).strip()}, {match.group(2).strip()}, {match.group(3).strip()} {match.group(4).strip()}"
                else:
                    raw_address = match.group(1).strip()
                break

        # 2. Specs Extraction
        beds_match = re.search(r'(\d+|Studio)\s*(?:bd|beds?|bedroom)', full_text, re.I)
        baths_match = re.search(r'([\d.]+)\s*(?:ba|baths?|bathrooms?)', full_text, re.I)
        
        beds = None
        if beds_match:
            beds = 0.0 if beds_match.group(1).lower() == 'studio' else float(beds_match.group(1))
            
        baths = float(baths_match.group(1)) if (baths_match and baths_match.group(1).replace('.','',1).isdigit()) else None
        
        # --- Enhanced sqft extraction (ordered by reliability) ---
        sqft = self._extract_sqft(full_text)

        details = {
            'beds': beds,
            'baths': baths,
            'sqft': sqft,
            'description': text[:2000]
        }
        if raw_address:
            details['raw_address'] = raw_address
            from utils import parse_address
            details['city'], details['state'], details['zip'] = parse_address(raw_address)
            
        return details

    def _parse_redfin_text_item(self, text: str):
        """Helper to parse a single Redfin listing from a text/markdown chunk."""
        import re
        import hashlib
        
        # Link extraction from markdown [text](url) - More robust regex
        # Prioritize finding the URL that contains /home/, /apartment/, /condo/, etc.
        # Markdown might contain multiple links, we want the most specific property link.
        links = re.findall(r'\[.*?\]\((https?://(?:www\.)?redfin\.com[^\s)]+)\)', text)
        url = ""
        
        # Filter out generic Redfin URLs
        def is_generic(u):
            u = u.strip('/').lower()
            generic = [
                'https://www.redfin.com', 'http://www.redfin.com',
                'https://www.redfin.com/rentals/renter-dashboard',
                'http://www.redfin.com/rentals/renter-dashboard',
                'https://www.redfin.com/houses-near-me',
                'http://www.redfin.com/houses-near-me'
            ]
            return any(u == g.strip('/').lower() for g in generic)

        for link in links:
            if any(path in link for path in ['/home/', '/apartment/', '/condo/', '/rentals/']) and not is_generic(link):
                url = link
                break
        
        # If no specific property link found, pick the first one that isn't generic
        if not url and links:
            for link in links:
                if not is_generic(link):
                    url = link
                    break

        # Address extraction FIRST to help with source_id if URL is generic
        raw_address = ""
        # 1. Try markdown link text
        # If we found a specific URL, try to get the label for THAT URL
        if url:
            escaped_url = re.escape(url)
            link_text_match = re.search(rf'\[(.*?)\]\({escaped_url}\)', text)
            if link_text_match:
                raw_address = link_text_match.group(1).strip()

        # 2. Fallback to generic link text search
        if not raw_address:
            link_text_match = re.search(r'\[(.*?)\]\(https?://(?:www\.)?redfin\.com', text)
            if link_text_match:
                raw_address = link_text_match.group(1).strip()
        
        # Clean up potential separators in address
        if ' | ' in raw_address:
            raw_address = raw_address.split(' | ')[-1].strip()

        # 3. If empty or looks like a price/junk, try lines
        if not raw_address or '$' in raw_address or len(raw_address) < 5 or 'Redfin' in raw_address:
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            for line in lines:
                if '$' not in line and not line.startswith('http') and len(line) > 5 and 'Title:' not in line:
                    raw_address = line.replace('###', '').strip()
                    break
        
        if not raw_address: raw_address = "Unknown Address"

        source_id = ""
        if url and not is_generic(url):
            # Redfin IDs can be in /home/(\d+) or /apartment/(\d+) or /condo/(\d+) etc.
            id_match = re.search(r'/(?:home|apartment|condo|rentals)/(\d+)', url)
            if id_match:
                source_id = id_match.group(1)
            else:
                # Fallback to the last part of the URL path
                parts = [p for p in url.split('/') if p]
                if parts:
                    source_id = parts[-1]
            
        if not source_id:
            # If URL is generic or missing, use a hash of the address + source to deduplicate
            import hashlib
            source_id = hashlib.md5(f"redfin_{raw_address.lower()}".encode()).hexdigest()[:10]

        # Price
        price_match = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:\+)?(?:\/mo)?)', text)
        price_str = price_match.group(1) if price_match else None
        
        # Specs
        beds_match = re.search(r'(\d+|Studio)\s*(?:bd|beds?|bedroom)', text, re.I)
        beds_val = beds_match.group(1) if beds_match else None
        if beds_val and str(beds_val).lower() == 'studio':
            beds = 0
        else:
            try:
                beds = float(beds_val) if beds_val else None
            except:
                beds = None
        
        baths_match = re.search(r'([\d.]+)\s*(?:ba|baths?|bathrooms?)', text, re.I)
        try:
            baths = float(baths_match.group(1)) if (baths_match and baths_match.group(1).replace('.','',1).isdigit()) else None
        except:
            baths = None
        
        # --- Use the shared robust sqft extraction ---
        sqft = self._extract_sqft(text)
        
        listing = {
            'source': 'redfin',
            'source_id': source_id,
            'canonical_url': url or "https://www.redfin.com",
            'raw_address': raw_address,
            'price': self._clean_price(price_str),
            'beds': beds,
            'baths': baths,
            'sqft': sqft,
            'description': text[:4000].replace('\n', ' '), # Increased to 4000 to preserve sqft data
            'extra_metadata': {'parsed_from': 'text_fallback'}
        }
        
        # Extract city, state, zip
        listing['city'], listing['state'], listing['zip'] = parse_address(listing['raw_address'], url=listing.get('canonical_url'))
        
        # Extract property type
        listing['property_type'] = extract_property_type(listing['canonical_url'], text, listing.get('raw_address', ''))
        
        logging.info(f"Text-Parsed Redfin: ID={listing['source_id']} | Price={listing['price']} | Beds={listing['beds']} | Addr={listing['raw_address'][:30]}...")
        return listing
