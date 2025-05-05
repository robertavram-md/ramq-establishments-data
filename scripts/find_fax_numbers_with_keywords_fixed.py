import csv
import json
import re
import time
import os
import argparse
from openai import OpenAI
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# OpenAI API key should be set as an environment variable
# Do not hardcode API keys in the code

# Input and output file paths
INPUT_CSV_PATH = "data/ramq_establishments_merged_final.csv"
OUTPUT_CSV_PATH = "data/ramq_establishments_final_with_keywords.csv"

# Regular expression pattern for fax numbers
# This pattern looks for common fax number formats in North America
FAX_PATTERNS = [
    r'(?i)(?:fax|télécopieur|telecopieur)(?:\s*:|\s*number|\s*#|\s*is|\s*at)?\s*(\+?1?[-\.\s]?(?:\(?\d{3}\)?[-\.\s]?)?(?:\d{3})[-\.\s]?(?:\d{4}))',
    r'(?i)(?:fax|télécopieur|telecopieur)(?:\s*:|\s*number|\s*#|\s*is|\s*at)?\s*(\d{3}[-\.\s]?\d{3}[-\.\s]?\d{4})',
    r'(?i)(?:fax|télécopieur|telecopieur)(?:\s*:|\s*number|\s*#|\s*is|\s*at)?\s*(\(\d{3}\)\s*\d{3}[-\.\s]?\d{4})',
    r'(?i)(?:fax|télécopieur|telecopieur)(?:\s*:|\s*number|\s*#|\s*is|\s*at)?\s*(\d{10})',
    r'(?i)(?:fax|télécopieur|telecopieur)(?:\s*:|\s*number|\s*#|\s*is|\s*at)?\s*(\d{3}\s\d{3}\s\d{4})'
]

def search_with_openai(query: str, client: OpenAI) -> str:
    """
    Use OpenAI API to search for information about an establishment.
    
    Args:
        query: The search query string
        client: OpenAI client instance
        
    Returns:
        String containing the search results
    """
    try:
        # Using the working API format provided by the user
        response = client.responses.create(
            model="gpt-4.1",
            tools=[{
                "type": "web_search_preview",
                "search_context_size": "low",
            }],
            input=query
        )
        
        # Extract the text content from the response
        result = response.output_text
        if result:
            print(f"Search result preview: {result[:200]}...")  # Print first 200 chars
            return result
        
        return ""
    except Exception as e:
        print(f"Error during OpenAI API request: {e}")
        return ""

def extract_fax_numbers_with_regex(text: str) -> List[str]:
    """
    Extract fax numbers from text using regex patterns.
    
    Args:
        text: The text to search for fax numbers
        
    Returns:
        List of unique fax numbers found
    """
    fax_numbers = []
    
    for pattern in FAX_PATTERNS:
        matches = re.findall(pattern, text)
        for match in matches:
            # Clean up the fax number
            fax = re.sub(r'[^\d]', '', match)
            if len(fax) >= 10:  # Ensure it's a valid length
                # Format consistently as XXX-XXX-XXXX
                if len(fax) == 10:
                    formatted_fax = f"{fax[:3]}-{fax[3:6]}-{fax[6:]}"
                    fax_numbers.append(formatted_fax)
                elif len(fax) == 11 and fax[0] == '1':  # Handle country code
                    formatted_fax = f"{fax[1:4]}-{fax[4:7]}-{fax[7:]}"
                    fax_numbers.append(formatted_fax)
    
    # Remove duplicates while preserving order
    unique_fax_numbers = []
    for fax in fax_numbers:
        if fax not in unique_fax_numbers:
            unique_fax_numbers.append(fax)
    
    return unique_fax_numbers

def extract_fax_numbers_with_second_llm(text: str, client: OpenAI) -> List[str]:
    """
    Extract fax numbers from text using a second LLM call.
    
    Args:
        text: The text to search for fax numbers
        client: OpenAI client instance
        
    Returns:
        List of unique fax numbers found
    """
    try:
        # Prepare a structured prompt to extract fax numbers
        prompt = f"""
        Extract all fax numbers from the following text. Return ONLY the fax numbers in a numbered list format.
        If there are no fax numbers, return an empty list.
        
        TEXT:
        {text}
        """
        
        # Using the working API format provided by the user
        response = client.responses.create(
            model="gpt-4.1",
            tools=[{
                "type": "web_search_preview",
                "search_context_size": "low",
            }],
            input=prompt
        )
        
        # Extract the response text
        result = response.output_text.strip()
        print(f"LLM extraction result: {result[:200]}...")
        
        # Parse the numbered list to extract fax numbers
        fax_numbers = []
        for line in result.split('\n'):
            # Remove numbers, dots, and other non-fax content
            line = re.sub(r'^\s*\d+[\.\)\-]?\s*', '', line).strip()
            if line:
                # Apply regex patterns to verify this is a fax number
                for pattern in FAX_PATTERNS:
                    match = re.search(pattern, line)
                    if match:
                        fax = match.group(1).strip()
                        fax_numbers.append(fax)
                
                # If the line doesn't match the pattern but looks like a phone number, add it
                if not any(re.search(pattern, line) for pattern in FAX_PATTERNS):
                    # Clean up and standardize the potential fax number
                    cleaned_line = re.sub(r'[^\d]', '', line)
                    if len(cleaned_line) == 10:  # Standard North American phone number length
                        fax_numbers.append(cleaned_line)
        
        # Deduplicate and return
        unique_fax_numbers = list(set(fax_numbers))
        return unique_fax_numbers
        
    except Exception as e:
        print(f"Error during second LLM extraction: {e}")
        return []

def extract_fax_keywords(text: str, fax_numbers: List[str], client: OpenAI = None) -> Dict[str, str]:
    """
    Extract keywords for each fax number.
    
    Args:
        text: The text to analyze
        fax_numbers: List of fax numbers to find keywords for
        client: OpenAI client instance
        
    Returns:
        Dictionary mapping fax numbers to keywords
    """
    if not fax_numbers or not client:
        return {fax: "general" for fax in fax_numbers}
    
    try:
        # Create a prompt for keyword extraction
        fax_str = ", ".join(fax_numbers)
        prompt = f"""
        Analyze the following text about a healthcare establishment and determine the purpose of each fax number listed.
        
        Text: {text}
        
        Fax numbers to analyze: {fax_str}
        
        For each fax number, determine if it's for any specific purpose such as:
        - general inquiries
        - appointments
        - referrals
        - medical records
        - patient care
        - admin
        - billing
        - etc.
        
        Return your answer as a simple mapping of fax number to purpose, one per line like this:
        XXX-XXX-XXXX: purpose
        """
        
        # Using the working API format provided by the user
        response = client.responses.create(
            model="gpt-4.1",
            tools=[{
                "type": "web_search_preview",
                "search_context_size": "low",
            }],
            input=prompt
        )
        
        # Extract the response text
        result = response.output_text.strip()
        print(f"Keyword extraction result: {result[:200]}...")
        
        # Parse the response to extract keywords
        keywords = {}
        for line in result.split('\n'):
            line = line.strip()
            if ':' in line:
                parts = line.split(':', 1)
                if len(parts) == 2:
                    fax_candidate = parts[0].strip()
                    keyword = parts[1].strip().lower()
                    
                    # Clean the fax number for matching
                    cleaned_fax = re.sub(r'[^\d]', '', fax_candidate)
                    
                    # Find the matching fax number from our list
                    for fax in fax_numbers:
                        cleaned_original = re.sub(r'[^\d]', '', fax)
                        if cleaned_original in cleaned_fax or cleaned_fax in cleaned_original:
                            keywords[fax] = keyword
        
        # Ensure all fax numbers have keywords
        for fax in fax_numbers:
            if fax not in keywords:
                keywords[fax] = "general"
        
        return keywords
    except Exception as e:
        print(f"Error during keyword extraction: {e}")
        return {fax: "general" for fax in fax_numbers}

def search_establishment_fax(name: str, address: str, website: str = None, client: OpenAI = None) -> Dict[str, Any]:
    """
    Search for fax numbers for a specific establishment using OpenAI API.
    
    Args:
        name: The name of the establishment
        address: The address of the establishment
        website: The website of the establishment (if available)
        client: OpenAI client instance
        
    Returns:
        Dictionary with fax numbers and related information
    """
    # Construct search queries
    queries = []
    
    # If website is available, search for fax on the website
    if website and website.strip():
        queries.append(f"Find the fax number for {name} located at {address}. Their website is {website}. Please search the website and any other sources to find all fax numbers for this healthcare establishment. For each fax number, specify which department it belongs to and what it's used for.")
    
    # Search for the establishment with address
    queries.append(f"Find the fax number for {name} located at {address} in Quebec, Canada. Please search for all fax numbers for this healthcare establishment. For each fax number, specify which department it belongs to and what it's used for.")
    
    # Search for the establishment name only
    queries.append(f"Find the fax number for {name} healthcare establishment in Quebec, Canada. Please search for all fax numbers. For each fax number, specify which department it belongs to and what it's used for.")
    
    all_fax_numbers = []
    search_result = ""
    
    # Try each query until we find fax numbers
    for query in queries:
        print(f"Searching: {query}")
        
        search_result = search_with_openai(query, client)
        
        # Check if there was an error
        if not search_result:
            print("No search results returned")
            continue
        
        print(f"Search result length: {len(search_result)} characters")
        
        # First layer: Extract fax numbers using regex
        regex_fax_numbers = extract_fax_numbers_with_regex(search_result)
        if regex_fax_numbers:
            print(f"Found fax numbers with regex: {regex_fax_numbers}")
            all_fax_numbers.extend(regex_fax_numbers)
        
        # Second layer: Extract fax numbers using a second LLM call
        llm_fax_numbers = extract_fax_numbers_with_second_llm(search_result, client)
        if llm_fax_numbers:
            print(f"Found fax numbers with second LLM: {llm_fax_numbers}")
            all_fax_numbers.extend(llm_fax_numbers)
        
        # If we found any fax numbers, stop searching
        if all_fax_numbers:
            break
        
        # Add a delay to avoid rate limiting
        time.sleep(2)
    
    # Remove duplicates while preserving order
    unique_fax_numbers = []
    for fax in all_fax_numbers:
        if fax not in unique_fax_numbers:
            unique_fax_numbers.append(fax)
    
    # Extract keywords for each fax number
    fax_keywords = {}
    if unique_fax_numbers:
        fax_keywords = extract_fax_keywords(search_result, unique_fax_numbers, client)
    
    # Ensure all fax numbers have keywords
    for fax in unique_fax_numbers:
        if fax not in fax_keywords:
            fax_keywords[fax] = "general"
    
    if unique_fax_numbers:
        print(f"Found fax numbers: {', '.join(unique_fax_numbers)}")
        print(f"Keywords: {fax_keywords}")
    else:
        print("No fax numbers found")
    
    return {
        "fax_numbers": unique_fax_numbers,
        "fax_keywords": fax_keywords,
        "raw_search_result": search_result if unique_fax_numbers else ""
    }

def standardize_fax_number(fax: str) -> str:
    """
    Standardize fax number to format 1XXXYYYZZZZ
    
    Args:
        fax: The fax number to standardize
        
    Returns:
        Standardized fax number
    """
    # Remove all non-digit characters
    digits = re.sub(r'[^\d]', '', fax)
    
    # Handle different formats
    if len(digits) == 10:  # XXXYYYZZZZ
        return f"1{digits}"
    elif len(digits) == 11 and digits[0] == '1':  # 1XXXYYYZZZZ
        return digits
    elif len(digits) == 7:  # YYYZZZZ (assuming area code is 819)
        return f"1819{digits}"
    else:
        return None  # Invalid format

def process_csv(input_path: str, output_path: str, limit: int = None, client: OpenAI = None):
    """
    Process the CSV file to find fax numbers and their keywords.
    
    Args:
        input_path: Path to input CSV file
        output_path: Path to output CSV file
        limit: Maximum number of rows to process (None for all)
        client: OpenAI client instance
    """
    # Read existing codes from the output file if it exists
    existing_codes = set()
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', newline='', encoding='utf-8') as f:
                # Handle potential empty file or file with only header
                try:
                    reader = csv.DictReader(f)
                    if reader.fieldnames: # Check if header exists
                        existing_codes = {row['code'] for row in reader if 'code' in row}
                    else:
                        print(f"Warning: Output file '{output_path}' exists but has no header. Treating as empty.")
                except (EOFError, csv.Error) as e:
                     print(f"Warning: Could not read existing codes from '{output_path}'. Error: {e}. Treating as empty.")
        except Exception as e:
            print(f"Warning: Error opening existing output file '{output_path}': {e}. Proceeding as if it's empty.")

    # --- Get total rows for progress calculation ---
    total_rows = 0
    try:
        with open(input_path, 'r', newline='', encoding='utf-8') as f_count:
             # Count rows more robustly
             reader_count = csv.reader(f_count)
             header = next(reader_count, None) # Skip header
             if header:
                  total_rows = sum(1 for row in reader_count)
             else:
                  print(f"Warning: Input file '{input_path}' seems empty or has no header.")
        print(f"Total data rows to check in '{input_path}': {total_rows}")
    except FileNotFoundError:
         print(f"Error: Input file '{input_path}' not found for counting rows.")
         return
    except Exception as e:
         print(f"Error reading input file '{input_path}' for counting rows: {e}")
         return

    if total_rows == 0:
        print("No rows found in input file. Exiting.")
        return

    # Calculate progress checkpoints (every 10%)
    checkpoints = {int(total_rows * p / 100.0) for p in range(10, 101, 10)}
    reported_checkpoints = set()

    # --- Process CSV --- 
    rows_processed_count = 0
    rows_skipped_count = 0

    try:
        with open(input_path, 'r', newline='', encoding='utf-8') as infile, \
             open(output_path, 'a', newline='', encoding='utf-8') as outfile:
            
            reader = csv.DictReader(infile)
            if not reader.fieldnames:
                 print(f"Error: Could not read header from '{input_path}'. Exiting.")
                 return
                 
            # Define fieldnames including the new ones
            # Ensure new fields are added correctly even if output file was empty/new
            output_fieldnames = list(reader.fieldnames)
            if 'fax_numbers' not in output_fieldnames: output_fieldnames.append('fax_numbers')
            if 'fax_keywords' not in output_fieldnames: output_fieldnames.append('fax_keywords')
            
            writer = csv.DictWriter(outfile, fieldnames=output_fieldnames)
            
            # Write header only if the file is newly created or was empty (existing_codes is empty implies this)
            file_is_new_or_empty = not existing_codes and os.path.getsize(output_path) == 0
            if file_is_new_or_empty:
                print(f"Writing header to new file: {output_path}")
                writer.writeheader()
            
            # Process each row
            for i, row in enumerate(reader):
                current_row_index = i + 1 # 1-based index for reporting
                if limit and rows_processed_count >= limit:
                    print(f"Reached processing limit of {limit} rows.")
                    break
                    
                # Skip if code already exists
                if row.get('code') in existing_codes:
                    # print(f"Skipping row {current_row_index} with code {row.get('code')} as it already exists")
                    rows_skipped_count += 1
                    continue
                    
                print(f"\nProcessing row {current_row_index} (Actual processed: {rows_processed_count + 1}): {row.get('name', 'N/A')}")
                
                # --- Search and Standardize --- 
                try:
                    result = search_establishment_fax(
                        row.get('name', ''),
                        row.get('address', ''),
                        row.get('website'),
                        client
                    )
                    
                    standardized_fax_numbers = []
                    standardized_keywords = {}

                    if result.get('fax_numbers'):
                        for fax in result['fax_numbers']:
                            standardized = standardize_fax_number(fax)
                            if standardized:
                                standardized_fax_numbers.append(standardized)
                                # Map keywords using the *standardized* number
                                original_keyword = result['fax_keywords'].get(fax, "general") # Get keyword for original fax format
                                standardized_keywords[standardized] = original_keyword # Store with standardized key
                    
                    # Prepare row for writing (ensure all expected fields are present)
                    output_row = {fieldname: row.get(fieldname) for fieldname in reader.fieldnames}
                    output_row['fax_numbers'] = json.dumps(standardized_fax_numbers)
                    output_row['fax_keywords'] = json.dumps(standardized_keywords)
                    
                    # Write the row
                    writer.writerow(output_row)
                    rows_processed_count += 1

                except Exception as e_inner:
                     print(f"Error processing data for row {current_row_index} ({row.get('name', 'N/A')}): {e_inner}")
                     # Optionally write row with empty fax data or skip
                     # Skipping for now to avoid partial data

                # --- Progress Reporting --- 
                if total_rows > 0:
                     progress_percent = int((current_row_index / total_rows) * 100)
                     # Check if we crossed a 10% checkpoint that hasn't been reported
                     for p_checkpoint in range(10, 101, 10):
                          row_checkpoint = int(total_rows * p_checkpoint / 100.0)
                          if current_row_index >= row_checkpoint and p_checkpoint not in reported_checkpoints:
                               print(f"\n--- Progress: Approx. {p_checkpoint}% complete (Checked row {current_row_index}/{total_rows}). Output file up-to-date. ---")
                               reported_checkpoints.add(p_checkpoint)
                               break # Report only the first checkpoint crossed

                # Add a small delay to avoid rate limiting
                time.sleep(1)
                
    except FileNotFoundError:
         print(f"Error: Input file '{input_path}' not found during processing.")
    except Exception as e:
         print(f"An unexpected error occurred during CSV processing: {e}")
    finally:
         print(f"\nFinished processing loop. Total rows checked: {current_row_index}. Rows actually processed and added: {rows_processed_count}. Rows skipped: {rows_skipped_count}.")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Search for fax numbers with keywords using OpenAI API')
    parser.add_argument('--api-key', type=str, help='OpenAI API key')
    parser.add_argument('--input', type=str, help='Input CSV file path', default=INPUT_CSV_PATH)
    parser.add_argument('--output', type=str, help='Output CSV file path', default=OUTPUT_CSV_PATH)
    parser.add_argument('--limit', type=int, help='Maximum number of rows to process', default=10)
    args = parser.parse_args()
    
    # Use API key from arguments or from environment
    api_key = args.api_key or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("No OpenAI API key provided. Please provide it via --api-key argument or set OPENAI_API_KEY environment variable.")
    
    # Initialize OpenAI client with provided API key
    client = OpenAI(api_key=api_key)
    
    print("Starting to search for fax numbers with keywords using OpenAI API with gpt-4.1 and web_search_preview...")
    print(f"Input file: {args.input}")
    print(f"Output file: {args.output}")
    
    # Set limit to None if user provides 0 or negative, to process all rows
    effective_limit = args.limit if args.limit and args.limit > 0 else None
    
    print(f"Processing limit: {effective_limit if effective_limit else 'All remaining rows'}")
    
    process_csv(args.input, args.output, limit=effective_limit, client=client)
    print(f"Processing complete. Results saved to {args.output}")
