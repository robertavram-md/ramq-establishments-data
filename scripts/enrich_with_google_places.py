import csv
import requests
import time
import datetime
import os
import random

# Google Places API key
API_KEY = "AIzaSyAo9nxP1rpIdP5UBPKftAxhjsiZCLGcdyM"

# Input and output file paths
input_csv_path = "data/ramq_establishments_final.csv"
output_csv_path = "data/ramq_establishments_enriched_complete.csv"
temp_output_path = "data/ramq_establishments_enriched_temp.csv"
progress_log_path = "data/enrichment_progress.log"

# Define the output CSV structure
output_fieldnames = [
    "ramq_id", "id", "google_place_name", "address", "locality", "country",
    "administrative_area_level_1", "administrative_area_level_2",
    "international_phone_number", "fax_number", "type", "website",
    "latitude", "longitude", "added_time", "place_type", "is_fax_enabled"
]

# Function to log progress
def log_progress(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(progress_log_path, 'a') as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(message)

# Function to search Google Places API with exponential backoff for rate limiting
def search_place(name, address, region, max_retries=5):
    # Combine name and address for better search results
    query = f"{name}, {address}, {region}, Quebec, Canada"
    
    # Prepare the API request
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": query,
        "key": API_KEY
    }
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Make the API request
            response = requests.get(url, params=params)
            data = response.json()
            
            # Check for rate limiting
            if data.get("status") == "OVER_QUERY_LIMIT":
                retry_count += 1
                wait_time = 2 ** retry_count + random.uniform(0, 1)  # Exponential backoff with jitter
                log_progress(f"Rate limit hit, waiting {wait_time:.2f} seconds before retry {retry_count}/{max_retries}")
                time.sleep(wait_time)
                continue
                
            # Check if we got results
            if data.get("status") == "OK" and data.get("results"):
                return data["results"][0]
            else:
                log_progress(f"No results found for: {query}")
                return None
                
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count + random.uniform(0, 1)
            log_progress(f"Error searching for place: {str(e)}. Retrying in {wait_time:.2f} seconds ({retry_count}/{max_retries})")
            time.sleep(wait_time)
    
    log_progress(f"Failed to get results after {max_retries} retries for: {query}")
    return None

# Function to get place details with exponential backoff for rate limiting
def get_place_details(place_id, max_retries=5):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,international_phone_number,website,geometry,address_components,type",
        "key": API_KEY
    }
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            # Check for rate limiting
            if data.get("status") == "OVER_QUERY_LIMIT":
                retry_count += 1
                wait_time = 2 ** retry_count + random.uniform(0, 1)  # Exponential backoff with jitter
                log_progress(f"Rate limit hit, waiting {wait_time:.2f} seconds before retry {retry_count}/{max_retries}")
                time.sleep(wait_time)
                continue
            
            if data.get("status") == "OK" and data.get("result"):
                return data["result"]
            else:
                log_progress(f"No details found for place ID: {place_id}")
                return None
                
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count + random.uniform(0, 1)
            log_progress(f"Error getting place details: {str(e)}. Retrying in {wait_time:.2f} seconds ({retry_count}/{max_retries})")
            time.sleep(wait_time)
    
    log_progress(f"Failed to get details after {max_retries} retries for place ID: {place_id}")
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
    batch_results = []
    processed_count = 0
    
    for i in range(start_idx, min(start_idx + batch_size, len(establishments))):
        establishment = establishments[i]
        ramq_id = establishment['code']  # Get the RAMQ ID from 'code' field
        name = establishment["name"]
        address = establishment["address"]
        region = establishment["region"]
        
        if not address:
            log_progress(f"Skipping {name} - No address available")
            # Create empty row for establishments without address
            output_row = {
                "ramq_id": ramq_id,
                "id": "",
                "google_place_name": "",
                "address": "",
                "locality": "",
                "country": "",
                "administrative_area_level_1": "",
                "administrative_area_level_2": "",
                "international_phone_number": "",
                "fax_number": "",
                "type": "",
                "website": "",
                "latitude": "",
                "longitude": "",
                "added_time": current_timestamp,
                "place_type": "",
                "is_fax_enabled": "0"
            }
            batch_results.append(output_row)
            log_progress(f"Added empty data for: {name} ({i+1}/{len(establishments)})")
            continue
        
        # Search for place
        place = search_place(name, address, region)
        
        if place and place.get("place_id"):
            place_id = place.get("place_id")
            details = get_place_details(place_id)
            
            if details:
                # Extract address components
                address_components = extract_address_components(details.get("address_components", []))
                
                # Determine place type
                place_type = determine_place_type(details.get("types", []), name)
                
                # Create output row
                output_row = {
                    "ramq_id": ramq_id,
                    "id": place_id,
                    "google_place_name": details.get('name', ''),
                    "address": details.get("formatted_address", ""),
                    "locality": address_components.get("locality", ""),
                    "country": address_components.get("country", ""),
                    "administrative_area_level_1": address_components.get("administrative_area_level_1", ""),
                    "administrative_area_level_2": address_components.get("administrative_area_level_2", ""),
                    "international_phone_number": details.get("international_phone_number", ""),
                    "fax_number": "",
                    "type": place_type,
                    "website": details.get("website", ""),
                    "latitude": details.get("geometry", {}).get("location", {}).get("lat", ""),
                    "longitude": details.get("geometry", {}).get("location", {}).get("lng", ""),
                    "added_time": current_timestamp,
                    "place_type": place_type,
                    "is_fax_enabled": "0"
                }
                batch_results.append(output_row)
                log_progress(f"Successfully processed: {name} ({i+1}/{len(establishments)})")
            else:
                # Create empty row if no details found
                output_row = {
                    "ramq_id": ramq_id,
                    "id": "",
                    "google_place_name": "",
                    "address": "",
                    "locality": "",
                    "country": "",
                    "administrative_area_level_1": "",
                    "administrative_area_level_2": "",
                    "international_phone_number": "",
                    "fax_number": "",
                    "type": "",
                    "website": "",
                    "latitude": "",
                    "longitude": "",
                    "added_time": current_timestamp,
                    "place_type": "",
                    "is_fax_enabled": "0"
                }
                batch_results.append(output_row)
                log_progress(f"No details found for: {name} ({i+1}/{len(establishments)})")
        else:
            # Create empty row if place not found
            output_row = {
                "ramq_id": ramq_id,
                "id": "",
                "google_place_name": "",
                "address": "",
                "locality": "",
                "country": "",
                "administrative_area_level_1": "",
                "administrative_area_level_2": "",
                "international_phone_number": "",
                "fax_number": "",
                "type": "",
                "website": "",
                "latitude": "",
                "longitude": "",
                "added_time": current_timestamp,
                "place_type": "",
                "is_fax_enabled": "0"
            }
            batch_results.append(output_row)
            log_progress(f"Place not found for: {name} ({i+1}/{len(establishments)})")
        
        # Add a variable delay to avoid hitting API rate limits
        delay = random.uniform(1.0, 2.0)
        time.sleep(delay)
    
    return batch_results

# Function to process establishments with resume capability
def process_establishments(batch_size=3, max_batches=None, start_from=None):
    # Get current timestamp
    current_timestamp = int(datetime.datetime.now().timestamp())
    
    # Initialize progress log
    if not os.path.exists(progress_log_path):
        with open(progress_log_path, 'w') as log_file:
            log_file.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting enrichment process\n")
    
    # Read input CSV
    with open(input_csv_path, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        establishments = list(reader)
    
    # Check if temp file exists to resume
    start_idx = 0
    if os.path.exists(temp_output_path) and not start_from:
        with open(temp_output_path, 'r', encoding='utf-8') as temp_file:
            temp_reader = csv.DictReader(temp_file)
            processed_count = sum(1 for _ in temp_reader)
            start_idx = processed_count
            log_progress(f"Resuming from index {start_idx}")
    elif start_from is not None:
        start_idx = start_from
        log_progress(f"Starting from specified index {start_idx}")
    
    # Create or append to temp output CSV
    mode = 'a' if start_idx > 0 and not start_from else 'w'
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
                log_progress(f"Reached maximum batch limit of {max_batches}")
                break
                
            log_progress(f"Processing batch {batch_count} (establishments {batch_start}-{min(batch_start+batch_size-1, total-1)}/{total})")
            
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
        log_progress(f"Processing complete. Enriched data saved to {output_csv_path}")
    else:
        log_progress(f"Partial processing complete. Temporary data saved to {temp_output_path}")

# Function to merge the original and enriched CSV files
def merge_csv_files(original_csv_path, enriched_csv_path, merged_csv_path):
    log_progress(f"Starting to merge CSV files...")
    
    # Read the original RAMQ data
    original_data = {}
    with open(original_csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        original_fieldnames = reader.fieldnames
        
        # Store original data by code (unique identifier in original data)
        for row in reader:
            code = row['code']
            original_data[code] = row
    
    log_progress(f"Read {len(original_data)} establishments from original CSV")
    
    # Read the enriched data
    enriched_data = {}
    with open(enriched_csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        enriched_fieldnames = reader.fieldnames
        
        # Create a mapping from Google Places name to enriched data
        # We'll use this to match with original data
        for row in reader:
            name = row['google_place_name']
            enriched_data[name] = row
    
    log_progress(f"Read {len(enriched_data)} establishments from enriched CSV")
    
    # Define the merged CSV structure
    merged_fieldnames = original_fieldnames + [
        field for field in enriched_fieldnames 
        if field not in original_fieldnames and field != 'google_place_name'  # Skip duplicate name field
    ]
    
    # Create the merged CSV
    with open(merged_csv_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=merged_fieldnames)
        writer.writeheader()
        
        # Process each establishment in the original data
        merged_count = 0
        for code, original_row in original_data.items():
            merged_row = original_row.copy()
            
            # Try to find a match in the enriched data
            original_name = original_row['name']
            
            # Look for exact match first
            if original_name in enriched_data:
                enriched_row = enriched_data[original_name]
                
                # Add enriched fields to merged row
                for field in enriched_fieldnames:
                    if field not in original_fieldnames and field != 'google_place_name':
                        merged_row[field] = enriched_row[field]
                
                merged_count += 1
            else:
                # If no exact match, try to find a partial match
                best_match = None
                best_score = 0
                
                for enriched_name, enriched_row in enriched_data.items():
                    # Simple matching score based on common words
                    original_words = set(original_name.upper().split())
                    enriched_words = set(enriched_name.upper().split())
                    common_words = original_words.intersection(enriched_words)
                    
                    if len(common_words) > best_score and len(common_words) >= 2:
                        best_score = len(common_words)
                        best_match = enriched_row
                
                if best_match:
                    # Add enriched fields to merged row
                    for field in enriched_fieldnames:
                        if field not in original_fieldnames and field != 'google_place_name':
                            merged_row[field] = best_match[field]
                    
                    merged_count += 1
                else:
                    # No match found, add empty values for enriched fields
                    for field in enriched_fieldnames:
                        if field not in original_fieldnames and field != 'google_place_name':
                            merged_row[field] = ""
            
            # Write the merged row
            writer.writerow(merged_row)
    
    log_progress(f"Merged CSV created at {merged_csv_path}")
    log_progress(f"Successfully merged data for {merged_count} establishments")
    log_progress(f"Total establishments in merged file: {len(original_data)}")
    
    return merged_csv_path

# Main execution
if __name__ == "__main__":
    # Process more establishments with improved rate limiting
    log_progress("Starting to enrich RAMQ establishments with Google Places data (improved version)...")
    
    # Process in small batches with better rate limiting
    # Adjust these parameters based on API limits
    process_establishments(batch_size=5, max_batches=500)
    
    # Merge the original and enriched CSV files
    merge_csv_files(
        original_csv_path=input_csv_path,
        enriched_csv_path=output_csv_path,
        merged_csv_path="data/ramq_establishments_merged_complete.csv"
    )
