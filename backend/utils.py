import re

def extract_city_from_url(url):
    """
    Extracts city and state from Redfin/Zillow canonical URLs.
    Redfin: /CA/Santa-Clara/...
    Zillow: /apartments/santa-clara-ca/...
    Zillow: /homedetails/...-Santa-Clara-CA-95050/...
    """
    if not url:
        return None, None
    
    # Redfin: /CA/Santa-Clara/...
    redfin_match = re.search(r'redfin\.com/([A-Z]{2})/([A-Za-z-]+)/', url)
    if redfin_match:
        state = redfin_match.group(1).upper()
        city = redfin_match.group(2).replace('-', ' ').title()
        return city, state
    
    # Zillow: /apartments/santa-clara-ca/...
    zillow_match = re.search(r'zillow\.com/apartments/([a-z-]+)-([a-z]{2})/', url, re.I)
    if zillow_match:
        city = zillow_match.group(1).replace('-', ' ').title()
        state = zillow_match.group(2).upper()
        return city, state
    
    # Zillow homedetails: /homedetails/...-Santa-Clara-CA-95050/...
    zillow_detail = re.search(r'zillow\.com/homedetails/.*?-([A-Za-z-]+)-([A-Z]{2})-(\d{5})/', url)
    if zillow_detail:
        city = zillow_detail.group(1).replace('-', ' ').title()
        state = zillow_detail.group(2).upper()
        return city, state
    
    return None, None

def parse_address(address: str, zip_code: str = None, url: str = None):
    """
    Parses a raw address string into (city, state, zip).
    Assumes standard US format: "Street, City, ST 12345"
    Falls back to URL extraction when city/state can't be parsed.
    """
    if not address:
        city, state = None, None
        if url:
            city, state = extract_city_from_url(url)
        return city, state, zip_code
        
    parts = [p.strip() for p in address.split(',')]
    if len(parts) < 2:
        # Not comma-separated. Try regex on whole string.
        state_match = re.search(r'\b([A-Z]{2})\b', address)
        zip_match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', address)
        state = state_match.group(1).upper() if state_match else None
        found_zip = zip_match.group(1) if zip_match else zip_code
        city = None
        # Fallback: extract from URL
        if url:
            city, url_state = extract_city_from_url(url)
            if not state:
                state = url_state
        return city, state, found_zip
        
    # Last part usually contains State and Zip
    last_part = parts[-1]
    state_zip_match = re.search(r'([A-Z]{2})\s*(\d{5}(?:-\d{4})?)', last_part, re.I)
    
    if state_zip_match:
        state = state_zip_match.group(1).upper()
        found_zip = state_zip_match.group(2)
    else:
        state_match = re.search(r'([A-Z]{2})', last_part, re.I)
        state = state_match.group(1).upper() if state_match else None
        zip_match = re.search(r'\b(\d{5})\b', address)
        found_zip = zip_match.group(1) if zip_match else zip_code
    
    # City is usually the second to last part
    city = parts[-2] if len(parts) >= 2 else None
    
    if city:
        city = re.sub(r'#.*', '', city).strip()
    
    # Fallback: extract from URL if city/state still missing
    if (not city or not state) and url:
        url_city, url_state = extract_city_from_url(url)
        if not city and url_city:
            city = url_city
        if not state and url_state:
            state = url_state
    
    return city, state, found_zip

def extract_fees_from_description(description: str) -> list:
    """
    Simple regex-based fee extraction from listing descriptions.
    """
    fees = []
    
    # Pet fee pattern: e.g., "Pet fee: $50/month", "pet deposit of $300"
    pet_fee_pattern = re.compile(r'pet\s+(?:fee|deposit|rent)\s+(?:of\s+)?\$(\d+(?:\.\d{2})?)', re.IGNORECASE)
    matches = pet_fee_pattern.findall(description)
    for amount in matches:
        fees.append({'fee_type': 'pet_fee', 'amount': float(amount), 'is_recurring': 'month' in description.lower() or 'rent' in description.lower()})
        
    # Application fee pattern: e.g., "Application fee: $45"
    app_fee_pattern = re.compile(r'application\s+fee\s+(?:of\s+)?\$(\d+(?:\.\d{2})?)', re.IGNORECASE)
    matches = app_fee_pattern.findall(description)
    for amount in matches:
        fees.append({'fee_type': 'application_fee', 'amount': float(amount), 'is_recurring': False})

    # Cleaning fee pattern
    cleaning_fee_pattern = re.compile(r'cleaning\s+fee\s+(?:of\s+)?\$(\d+(?:\.\d{2})?)', re.IGNORECASE)
    matches = cleaning_fee_pattern.findall(description)
    for amount in matches:
        fees.append({'fee_type': 'cleaning_fee', 'amount': float(amount), 'is_recurring': False})
        
    return fees

def extract_property_type(url: str, description: str, address: str = "") -> str:
    """
    Determines if a listing is an apartment, condo, or house.
    """
    addr_lower = address.lower()
    url_lower = url.lower()
    
    # 1. Check address specifically for unit identifiers
    if re.search(r'\b(unit|apt|apt\.|suite|ste)\b\s*[\w\d]+', addr_lower) or re.search(r'#\s*[\w\d]+', addr_lower) or 'apartment' in addr_lower:
        return 'apartment'
        
    # 2. Check canonical URL identifiers (strong indicators)
    if '/homedetails/' in url_lower or '/home/' in url_lower:
        return 'house'
    if '/b/' in url_lower or '/apartments/' in url_lower or '/apartment/' in url_lower:
        return 'apartment'
        
    # 3. Fallback to broad text search
    text = f"{url} {description}".lower()
    if 'apartment' in text:
        return 'apartment'
    if 'condo' in text:
        return 'condo'
    
    return 'house'

def build_url(platform: str, zipcode: str = None, page: int = 1, base_url: str = None, 
              min_price: int = None, max_price: int = None, 
              min_beds: int = None, min_baths: float = None) -> str:
    """
    Builds a search URL with normalized filter parameters.
    """
    platform = platform.lower()
    final_url = ""
    
    if platform == 'zillow':
        if not base_url:
            if zipcode:
                if page > 1:
                    base_url = f"https://www.zillow.com/homes/for_rent/{zipcode}_rb/{page}_p/"
                else:
                    base_url = f"https://www.zillow.com/homes/for_rent/{zipcode}_rb/"
            else:
                base_url = "https://www.zillow.com/homes/for_rent/"
        final_url = base_url

    elif platform == 'redfin':
        if not base_url:
            if zipcode:
                if page > 1:
                    base_url = f"https://www.redfin.com/zipcode/{zipcode}/rentals/page-{page}"
                else:
                    base_url = f"https://www.redfin.com/zipcode/{zipcode}/rentals"
            else:
                base_url = "https://www.redfin.com/rentals"
        final_url = base_url
            
    return final_url or base_url or ""

if __name__ == "__main__":
    test_desc = "Nice rental. Application fee: $45. Pet fee: $50/month."
    print(f"Extracted fees: {extract_fees_from_description(test_desc)}")
