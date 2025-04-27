import csv
import requests
import time
import datetime
import os

# Google Places API key
API_KEY = "AIzaSyAo9nxP1rpIdP5UBPKftAxhjsiZCLGcdyM"

# Input and output file paths
input_csv_path = "/home/ubuntu/ramq_data/ramq_establishments_final.csv"
output_csv_path = "/home/ubuntu/ramq_data/ramq_establishments_enriched.csv"
temp_output_path = "/home/ubuntu/ramq_data/ramq_establishments_enriched_temp.csv"

# Define the output CSV structure
output_fieldnames = [
    "id", "admin_user_id", "name", "address", "locality", "country",
    "administrative_area_level_1", "administrative_area_level_2",
    "international_phone_number", "fax_number", "type", "website",
    "latitude", "longitude", "added_time", "place_type", "is_fax_enabled"
]

# Function to search Google Places API
def search_place(name, address, region):
    # Combine name and address for better search results
    query = f"{name}, {address}, {region}, Quebec, Canada"
    
    # Prepare the API request
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": query,
        "key": API_KEY
    }
    
    try:
        # Make the API request
        response = requests.get(url, params=params)
        data = response.json()
        
        # Check if we got results
        if data.get("status") == "OK" and data.get("results"):
            return data["results"][0]
        else:
            print(f"No results found for: {query}")
            return None
    except Exception as e:
        print(f"Error searching for place: {str(e)}")
        return None

# Function to get place details
def get_place_details(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,international_phone_number,website,geometry,address_components,type",
        "key": API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        
        if data.get("status") == "OK" and data.get("result"):
            return data["result"]
        else:
            print(f"No details found for place ID: {place_id}")
            return None
    except Exception as e:
        print(f"Error getting place details: {str(e)}")
        return None

# Function to determine place type
def determine_place_type(google_types, ramq_name):
    # Check for hospital
    if any(t in ["hospital", "health"] for t in google_types) or "HOPITAL" in ramq_name.upper():
        return "hospital"
    # Check for clinic
    elif any(t in ["doctor", "medical_clinic", "health"] for t in google_types) or any(term in ramq_name.upper() for term in ["CLINIQUE", "CLSC", "CENTRE", "MEDICAL"]):
        return "clinic"
    # Default to pharmacy
    else:
        return "pharmacy"

# Function to extract address components
def extract_address_components(components):
    result = {
        "locality": "",
        "country": "",
        "administrative_area_level_1": "",
        "administrative_area_level_2": ""
    }
    
    for component in components:
        types = component.get("types", [])
        
        if "locality" in types:
            result["locality"] = component.get("long_name", "")
        elif "country" in types:
            result["country"] = component.get("short_name", "")
        elif "administrative_area_level_1" in types:
            result["administrative_area_level_1"] = component.get("short_name", "")
        elif "postal_code" in types:
            result["administrative_area_level_2"] = component.get("long_name", "")
    
    return result

# Function to process a small batch of establishments
def process_batch(establishments, start_idx, batch_size, current_timestamp):
    results = []
    
    for i in range(start_idx, min(start_idx + batch_size, len(establishments))):
        establishment = establishments[i]
        
        # Extract establishment data
        name = establishment["name"]
        address = establishment["address"]
        region = establishment["region"]
        
        # Skip if no address
        if not address:
            print(f"Skipping {name} - No address available")
            continue
        
        # Search for place
        place = search_place(name, address, region)
        
        if place:
            place_id = place.get("place_id")
            
            # Get place details
            details = get_place_details(place_id)
            
            if details:
                # Extract address components
                address_components = extract_address_components(details.get("address_components", []))
                
                # Determine place type
                place_type = determine_place_type(details.get("types", []), name)
                
                # Extract phone and fax
                phone = details.get("international_phone_number", "")
                fax = ""  # Fax is not available in Google Places API
                
                # Check if fax is enabled
                is_fax_enabled = "1" if fax else "0"
                
                # Extract coordinates
                location = details.get("geometry", {}).get("location", {})
                latitude = location.get("lat", "")
                longitude = location.get("lng", "")
                
                # Create output row
                output_row = {
                    "id": place_id,
                    "admin_user_id": "",
                    "name": details.get("name", name),
                    "address": details.get("formatted_address", address),
                    "locality": address_components["locality"],
                    "country": address_components["country"],
                    "administrative_area_level_1": address_components["administrative_area_level_1"],
                    "administrative_area_level_2": address_components["administrative_area_level_2"],
                    "international_phone_number": phone,
                    "fax_number": fax,
                    "type": place_type,
                    "website": details.get("website", ""),
                    "latitude": latitude,
                    "longitude": longitude,
                    "added_time": current_timestamp,
                    "place_type": place_type,
                    "is_fax_enabled": is_fax_enabled
                }
                
                results.append(output_row)
                
                # Add a small delay to avoid hitting API rate limits
                time.sleep(0.2)
            else:
                print(f"Could not get details for {name}")
        else:
            print(f"Could not find place for {name}")
    
    return results

# Function to process establishments with resume capability
def process_establishments(batch_size=10, max_batches=None):
    # Get current timestamp
    current_timestamp = int(datetime.datetime.now().timestamp())
    
    # Read input CSV
    with open(input_csv_path, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        establishments = list(reader)
    
    # Check if temp file exists to resume
    start_idx = 0
    if os.path.exists(temp_output_path):
        with open(temp_output_path, 'r', encoding='utf-8') as temp_file:
            temp_reader = csv.DictReader(temp_file)
            processed_count = sum(1 for _ in temp_reader)
            start_idx = processed_count
            print(f"Resuming from index {start_idx}")
    
    # Create or append to temp output CSV
    mode = 'a' if start_idx > 0 else 'w'
    with open(temp_output_path, mode, newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=output_fieldnames)
        
        # Write header if new file
        if mode == 'w':
            writer.writeheader()
        
        # Process establishments in batches
        total = len(establishments)
        batch_count = 0
        
        for batch_start in range(start_idx, total, batch_size):
            batch_count += 1
            if max_batches and batch_count > max_batches:
                print(f"Reached maximum batch limit of {max_batches}")
                break
                
            print(f"Processing batch {batch_count} (establishments {batch_start}-{min(batch_start+batch_size-1, total-1)}/{total})")
            
            # Process batch
            batch_results = process_batch(establishments, batch_start, batch_size, current_timestamp)
            
            # Write batch results
            for row in batch_results:
                writer.writerow(row)
            
            # Flush to ensure data is written
            outfile.flush()
    
    # If processing is complete, rename temp file to final output
    if start_idx + (batch_count * batch_size) >= total or (max_batches and batch_count >= max_batches):
        if os.path.exists(output_csv_path):
            os.remove(output_csv_path)
        os.rename(temp_output_path, output_csv_path)
        print(f"Processing complete. Enriched data saved to {output_csv_path}")
    else:
        print(f"Partial processing complete. Temporary data saved to {temp_output_path}")

# Main execution
if __name__ == "__main__":
    print("Starting to enrich RAMQ establishments with Google Places data...")
    # Process in small batches and limit to 50 batches for testing
    process_establishments(batch_size=5, max_batches=50)
