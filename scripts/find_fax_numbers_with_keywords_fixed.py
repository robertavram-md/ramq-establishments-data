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

def process_csv(input_path: str, output_path: str, limit: int = None, client: OpenAI = None):
    """
    Process the CSV file to find fax numbers for establishments.
    
    Args:
        input_path: Path to the input CSV file
        output_path: Path to the output CSV file
        limit: Maximum number of establishments to process (None for all)
        client: OpenAI client instance
    """
    # Read the input CSV
    with open(input_path, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        data = list(reader)
    
    print(f"Total establishments in the input file: {len(data)}")
    
    # Initialize counters and result storage
    processed_count = 0
    establishments_with_fax = 0
    total_fax_numbers = 0
    
    # Check if output file exists and get existing processed establishments
    processed_ids = set()
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as existing_file:
                existing_reader = csv.DictReader(existing_file)
                existing_data = list(existing_reader)
                
                # Store already processed establishment IDs
                for row in existing_data:
                    if row.get('code') or row.get('ramq_id'):
                        processed_ids.add(row.get('code', '') + row.get('ramq_id', ''))
                
                print(f"Found {len(processed_ids)} already processed establishments in output file.")
        except Exception as e:
            print(f"Error reading existing output file: {e}")
            existing_data = []
    else:
        existing_data = []
    
    # Process establishments
    for establishment in data:
        establishment_id = establishment.get('code', '') + establishment.get('ramq_id', '')
        
        # Skip already processed establishments
        if establishment_id in processed_ids:
            print(f"Skipping already processed establishment: {establishment.get('name')}")
            continue
        
        # Apply limit if specified
        if limit is not None and processed_count >= limit:
            print(f"Reached limit of {limit} establishments. Stopping.")
            break
        
        processed_count += 1
        
        print(f"\nProcessing establishment {processed_count}/{limit if limit else len(data)}: {establishment.get('name', 'Unknown')}")
        
        # Search for fax numbers
        fax_result = search_establishment_fax(
            establishment['name'],
            establishment['address'],
            establishment.get('website'),
            client
        )
        
        fax_numbers = fax_result["fax_numbers"]
        fax_keywords = fax_result["fax_keywords"]
        search_result = fax_result.get("raw_search_result", "")
        
        # Add fax numbers to the establishment data
        if fax_numbers:
            establishments_with_fax += 1
            total_fax_numbers += len(fax_numbers)
            
            # Format fax numbers and keywords for output
            establishment['fax_numbers'] = json.dumps(fax_numbers)
            establishment['fax_keywords'] = json.dumps(fax_keywords)
            establishment['raw_search_result'] = search_result[:1000] if search_result else ""  # Limit length
            
            print(f"Found {len(fax_numbers)} fax numbers: {', '.join(fax_numbers)}")
        else:
            establishment['fax_numbers'] = "[]"
            establishment['fax_keywords'] = "{}"
            establishment['raw_search_result'] = ""
            
            print("No fax numbers found")
        
        # Write to output file
        write_mode = 'a' if os.path.exists(output_path) else 'w'
        with open(output_path, write_mode, encoding='utf-8', newline='') as outfile:
            # Get all fieldnames (original + new ones)
            fieldnames = list(reader.fieldnames) + ['fax_numbers', 'fax_keywords']
            
            # Remove duplicates while preserving order
            seen = set()
            fieldnames = [x for x in fieldnames if not (x in seen or seen.add(x))]
            
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            
            # Write header if file is new
            if write_mode == 'w':
                writer.writeheader()
            
            # Remove the 'raw_search_result' key before writing
            establishment.pop('raw_search_result', None)
            writer.writerow(establishment)
        
        # Add to processed IDs
        processed_ids.add(establishment_id)
        
        # Sleep to avoid rate limiting
        time.sleep(3)
    
    return {
        "processed_count": processed_count,
        "establishments_with_fax": establishments_with_fax,
        "total_fax_numbers": total_fax_numbers
    }

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Search for fax numbers with keywords using OpenAI API')
    parser.add_argument('--api-key', type=str, help='OpenAI API key')
    parser.add_argument('--input', type=str, help='Input CSV file path', default=INPUT_CSV_PATH)
    parser.add_argument('--output', type=str, help='Output CSV file path', default=OUTPUT_CSV_PATH)
    parser.add_argument('--limit', type=int, help='Maximum number of establishments to process', default=10)
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
    print(f"Processing limit: {args.limit if args.limit else 'All establishments'}")
    
    # Set limit to None to process all establishments, or a number to process a subset
    results = process_csv(args.input, args.output, limit=args.limit, client=client)
    print(f"Processing complete. Results saved to {args.output}")
    print(f"Found {results['total_fax_numbers']} fax numbers across {results['establishments_with_fax']} establishments.")
