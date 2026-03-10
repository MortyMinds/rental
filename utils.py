import re

def parse_address(address: str):
    """
    Parses a raw address string into (city, state, zip).
    Assumes standard US format: "Street, City, ST 12345"
    Also handles variations like "Street, City, ST"
    """
    if not address:
        return None, None, None
        
    parts = [p.strip() for p in address.split(',')]
    if len(parts) < 2:
        # Maybe it's not comma separated. Try regex on whole string.
        state_match = re.search(r'\b([A-Z]{2})\b', address)
        zip_match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', address)
        state = state_match.group(1).upper() if state_match else None
        zip_code = zip_match.group(1) if zip_match else None
        return None, state, zip_code
        
    # Last part usually contains State and Zip
    last_part = parts[-1]
    state_zip_match = re.search(r'([A-Z]{2})\s*(\d{5}(?:-\d{4})?)', last_part, re.I)
    
    if state_zip_match:
        state = state_zip_match.group(1).upper()
        zip_code = state_zip_match.group(2)
    else:
        # Try state without zip in the last part
        state_match = re.search(r'([A-Z]{2})', last_part, re.I)
        state = state_match.group(1).upper() if state_match else None
        # Look for zip anywhere else?
        zip_match = re.search(r'\b(\d{5})\b', address)
        zip_code = zip_match.group(1) if zip_match else None
    
    # City is usually the second to last part
    city = parts[-2] if len(parts) >= 2 else None
    
    # Clean up city if it has extra info
    if city:
        city = re.sub(r'#.*', '', city).strip()
        
    return city, state, zip_code

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

def extract_property_type(url: str, description: str) -> str:
    """
    Determines if a listing is an apartment, condo, or house.
    """
    text = f"{url} {description}".lower()
    if 'apartment' in text:
        return 'apartment'
    if 'condo' in text:
        return 'condo'
    if 'homedetails' in url:
        return 'house'
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
