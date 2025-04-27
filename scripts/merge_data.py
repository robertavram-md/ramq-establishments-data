import csv
import os

# Input and output file paths
original_csv_path = "data/ramq_establishments_final.csv"
enriched_csv_path = "data/ramq_establishments_enriched_temp.csv"
merged_csv_path = "data/ramq_establishments_merged_improved.csv"

def merge_csv_files():
    print(f"Starting to merge CSV files...")
    
    # Read the original RAMQ data
    original_data = {}
    with open(original_csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        original_fieldnames = reader.fieldnames
        
        # Store original data by id (unique identifier in original data)
        for row in reader:
            ramq_id = row['code']  # Using code as ramq_id
            original_data[ramq_id] = row
    
    print(f"Read {len(original_data)} establishments from original CSV")
    
    # Read the enriched data
    enriched_data = {}
    with open(enriched_csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        enriched_fieldnames = reader.fieldnames
        
        # Create a mapping from ramq_id to enriched data
        for row in reader:
            ramq_id = row['ramq_id']
            enriched_data[ramq_id] = row
    
    print(f"Read {len(enriched_data)} establishments from enriched CSV")
    
    # Define the merged CSV structure
    merged_fieldnames = original_fieldnames + [
        field for field in enriched_fieldnames 
        if field not in original_fieldnames and field not in ['ramq_id', 'google_place_name', 'id']  # Skip duplicate fields
    ]
    
    # Create the merged CSV
    with open(merged_csv_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=merged_fieldnames)
        writer.writeheader()
        
        # Process each establishment in the original data
        merged_count = 0
        for ramq_id, original_row in original_data.items():
            merged_row = original_row.copy()
            
            # Try to find a match in the enriched data using ramq_id
            if ramq_id in enriched_data:
                enriched_row = enriched_data[ramq_id]
                
                # Add enriched fields to merged row
                for field in enriched_fieldnames:
                    if field not in original_fieldnames and field not in ['ramq_id', 'google_place_name', 'id']:
                        merged_row[field] = enriched_row[field]
                
                # Add google_place_name and place_id as new fields
                merged_row['google_place_name'] = enriched_row['google_place_name']
                merged_row['google_place_id'] = enriched_row['id']  # Store Google place_id as google_place_id
                merged_count += 1
            else:
                # No match found, add empty values for enriched fields
                for field in enriched_fieldnames:
                    if field not in original_fieldnames and field not in ['ramq_id', 'id']:
                        merged_row[field] = ""
            
            # Write the merged row
            writer.writerow(merged_row)
    
    print(f"Merged CSV created at {merged_csv_path}")
    print(f"Successfully merged data for {merged_count} establishments")
    print(f"Total establishments in merged file: {len(original_data)}")
    
    return merged_csv_path

if __name__ == "__main__":
    merge_csv_files()
