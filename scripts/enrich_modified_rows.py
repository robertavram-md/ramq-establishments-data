import csv
import requests
import time
import datetime
import os
import random
import argparse
import sys

# Google Places API key
API_KEY = "AIzaSyAo9nxP1rpIdP5UBPKftAxhjsiZCLGcdyM"

# Input and output file paths
input_csv_path = "data/ramq_establishments_to_enrich.csv"
output_csv_path = "data/ramq_establishments_enriched_modified.csv"
temp_output_path = "data/ramq_establishments_enriched_modified_temp.csv"
progress_log_path = "data/enrichment_progress_modified.log"

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
    
    log_progress(f"Failed to get place details after {max_retries} retries for place ID: {place_id}")
    return None

# Function to determine place type
def determine_place_type(google_types, ramq_name):
    if "hospital" in google_types:
        return "hospital"
    elif "pharmacy" in google_types:
        return "pharmacy"
    else:
        return "hospital"  # Default to hospital

# Function to extract address components
def extract_address_components(components):
    result = {
        "locality": "",
        "country": "",
        "administrative_area_level_1": "",
        "administrative_area_level_2": "",
        "postal_code": ""
    }
    
    for component in components:
        for type in component["types"]:
            if type in result:
                result[type] = component["long_name"]
                break
    
    return result

# Function to process a small batch of establishments
def process_batch(establishments, start_idx, batch_size, current_timestamp):
    results = []
    end_idx = min(start_idx + batch_size, len(establishments))
    
    for i in range(start_idx, end_idx):
        establishment = establishments[i]
        
        ramq_id = establishment.get("code", "")
        ramq_name = establishment.get("name", "")
        ramq_address = establishment.get("address", "")
        ramq_region = establishment.get("region", "")
        
        log_progress(f"Processing ({i+1}/{len(establishments)}): {ramq_id} - {ramq_name}")
        
        # Initialize output row with original data
        output_row = {
            "ramq_id": ramq_id,
            "id": "",  # Will be filled with Google Place ID if found
            "google_place_name": "",
            "address": ramq_address,  # Default to original address
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
            "is_fax_enabled": "0"  # Default value
        }
        
        # Skip if missing essential data
        if not ramq_name or not ramq_address:
            log_progress(f"Skipping {ramq_id}: Missing name or address")
            results.append(output_row)
            continue
        
        try:
            # Step 1: Search for place
            search_result = search_place(ramq_name, ramq_address, ramq_region)
            if not search_result:
                log_progress(f"No search results for {ramq_id}: {ramq_name}")
                results.append(output_row)
                continue
            
            # Step 2: Get place details
            place_id = search_result.get("place_id")
            if not place_id:
                log_progress(f"No place ID for {ramq_id}: {ramq_name}")
                results.append(output_row)
                continue
                
            place_details = get_place_details(place_id)
            if not place_details:
                log_progress(f"No place details for {ramq_id}: {ramq_name}")
                # Still save the place ID
                output_row["id"] = place_id
                results.append(output_row)
                continue
            
            # Step 3: Extract and map relevant data
            output_row["id"] = place_id
            output_row["google_place_name"] = place_details.get("name", "")
            output_row["address"] = place_details.get("formatted_address", ramq_address)
            
            # Extract address components
            if "address_components" in place_details:
                address_components = extract_address_components(place_details["address_components"])
                output_row["locality"] = address_components["locality"]
                output_row["country"] = address_components["country"]
                output_row["administrative_area_level_1"] = address_components["administrative_area_level_1"]
                output_row["administrative_area_level_2"] = address_components["administrative_area_level_2"]
            
            # Phone number
            output_row["international_phone_number"] = place_details.get("international_phone_number", "")
            
            # Types
            if "types" in place_details:
                output_row["type"] = ",".join(place_details["types"])
                output_row["place_type"] = determine_place_type(place_details["types"], ramq_name)
                
            # Website
            output_row["website"] = place_details.get("website", "")
            
            # Coordinates
            if "geometry" in place_details and "location" in place_details["geometry"]:
                output_row["latitude"] = place_details["geometry"]["location"]["lat"]
                output_row["longitude"] = place_details["geometry"]["location"]["lng"]
            
            log_progress(f"Successfully processed {ramq_id}: {ramq_name}")
            
        except Exception as e:
            log_progress(f"Error processing {ramq_id}: {str(e)}")
        
        # Add the result
        results.append(output_row)
        
        # Small delay between each establishment in the batch for rate limiting
        time.sleep(random.uniform(0.5, 1.5))
    
    return results

# Function to process establishments with resume capability
def process_establishments(batch_size=3, max_batches=None, start_from=None):
    # Get the current time for all records in this run
    current_timestamp = int(time.time())
    
    # Read the input CSV
    try:
        with open(input_csv_path, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            establishments = list(reader)
            log_progress(f"Read {len(establishments)} establishments from {input_csv_path}")
    except Exception as e:
        log_progress(f"Error reading input file: {str(e)}")
        return
    
    # Check if temp file exists to resume
    start_idx = 0
    if os.path.exists(temp_output_path) and not start_from:
        try:
            with open(temp_output_path, 'r', encoding='utf-8') as temp_file:
                temp_reader = csv.DictReader(temp_file)
                processed_count = sum(1 for _ in temp_reader)
                start_idx = processed_count
                log_progress(f"Resuming from index {start_idx}")
        except Exception as e:
            log_progress(f"Error reading temp file: {str(e)}")
            start_idx = 0
    elif start_from is not None:
        start_idx = start_from
        log_progress(f"Starting from specified index {start_idx}")
    
    # Create or append to temp output CSV
    mode = 'a' if start_idx > 0 and not start_from else 'w'
    try:
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
                os.fsync(outfile.fileno())  # Force write to disk
        
        # If processing is complete, rename temp file to final output
        if start_idx + (batch_count * batch_size) >= total or (max_batches and batch_count >= max_batches):
            log_progress("Processing complete. Preparing to save final output...")
            if os.path.exists(output_csv_path):
                try:
                    os.remove(output_csv_path)
                    log_progress(f"Removed existing output file: {output_csv_path}")
                except Exception as e:
                    log_progress(f"Warning: Could not remove existing output file: {str(e)}")
            
            try:
                if os.path.exists(temp_output_path):  # Double check temp file exists
                    os.rename(temp_output_path, output_csv_path)
                    log_progress(f"Processing complete. Enriched data saved to {output_csv_path}")
                else:
                    log_progress(f"Error: Temporary file {temp_output_path} not found")
            except Exception as e:
                log_progress(f"Error saving final output: {str(e)}")
        else:
            log_progress(f"Partial processing complete. Temporary data saved to {temp_output_path}")
    except Exception as e:
        log_progress(f"Error during processing: {str(e)}")

# Function to merge with existing enriched data
def merge_with_complete_data():
    enriched_complete_path = "data/ramq_establishments_enriched_complete_full.csv"
    modified_enriched_path = output_csv_path
    merged_output_path = "data/ramq_establishments_enriched_complete_merged.csv"
    
    # Read the modified enriched data
    log_progress(f"Reading modified enriched data from {modified_enriched_path}")
    modified_rows = {}
    try:
        with open(modified_enriched_path, 'r', encoding='utf-8') as modified_file:
            reader = csv.DictReader(modified_file)
            for row in reader:
                ramq_id = row.get("ramq_id", "")
                if ramq_id:
                    modified_rows[ramq_id] = row
        log_progress(f"Read {len(modified_rows)} modified enriched rows")
    except Exception as e:
        log_progress(f"Error reading modified enriched data: {str(e)}")
        return
    
    # Read the complete enriched data
    log_progress(f"Reading complete enriched data from {enriched_complete_path}")
    complete_rows = []
    complete_fieldnames = []
    try:
        with open(enriched_complete_path, 'r', encoding='utf-8') as complete_file:
            reader = csv.DictReader(complete_file)
            complete_fieldnames = reader.fieldnames
            for row in reader:
                ramq_id = row.get("ramq_id", "")
                if ramq_id in modified_rows:
                    # Replace with modified row
                    complete_rows.append(modified_rows[ramq_id])
                    # Mark as processed
                    modified_rows.pop(ramq_id)
                else:
                    # Keep original row
                    complete_rows.append(row)
        log_progress(f"Read {len(complete_rows)} total rows from complete data")
    except Exception as e:
        log_progress(f"Error reading complete enriched data: {str(e)}")
        return
    
    # Add any remaining modified rows (if they didn't exist in complete data)
    for ramq_id, row in modified_rows.items():
        complete_rows.append(row)
        log_progress(f"Adding new modified row for RAMQ ID: {ramq_id}")
    
    # Sort by ramq_id
    complete_rows.sort(key=lambda x: x.get("ramq_id", ""))
    
    # Write merged data
    log_progress(f"Writing merged data to {merged_output_path}")
    try:
        with open(merged_output_path, 'w', newline='', encoding='utf-8') as merged_file:
            writer = csv.DictWriter(merged_file, fieldnames=complete_fieldnames or output_fieldnames)
            writer.writeheader()
            for row in complete_rows:
                writer.writerow(row)
        log_progress(f"Successfully wrote {len(complete_rows)} merged rows to {merged_output_path}")
    except Exception as e:
        log_progress(f"Error writing merged data: {str(e)}")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Enrich RAMQ establishments with Google Places data')
    parser.add_argument('--start-from', type=int, help='Index to start processing from', default=0)
    parser.add_argument('--merge', action='store_true', help='Merge with complete data after processing')
    args = parser.parse_args()
    
    # Process establishments with improved rate limiting
    log_progress("Starting to enrich modified RAMQ establishments with Google Places data...")
    
    # Process in small batches with better rate limiting
    # Adjust these parameters based on API limits
    process_establishments(batch_size=10, max_batches=None, start_from=args.start_from)
    
    # Merge with complete data if requested
    if args.merge:
        log_progress("Merging modified data with complete data...")
        merge_with_complete_data()
