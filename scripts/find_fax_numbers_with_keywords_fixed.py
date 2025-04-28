import csv
import json
import re
import time
import os
from openai import OpenAI
from typing import List, Dict, Any, Optional

# OpenAI API key should be set as an environment variable
# Do not hardcode API keys in the code
client = OpenAI()  # This will use the OPENAI_API_KEY environment variable

# Input and output file paths
INPUT_CSV_PATH = "/home/ubuntu/ramq_github/ramq-establishments-data/data/ramq_establishments_merged_improved.csv"
OUTPUT_CSV_PATH = "/home/ubuntu/ramq_github/ramq-establishments-data/data/ramq_establishments_final_with_keywords.csv"

# Regular expression pattern for fax numbers
# This pattern looks for common fax number formats in North America
FAX_PATTERNS = [
    r'(?i)(?:fax|télécopieur|telecopieur)(?:\s*:|\s*number|\s*#|\s*is|\s*at)?\s*(\+?1?[-\.\s]?(?:\(?\d{3}\)?[-\.\s]?)?(?:\d{3})[-\.\s]?(?:\d{4}))',
    r'(?i)(?:fax|télécopieur|telecopieur)(?:\s*:|\s*number|\s*#|\s*is|\s*at)?\s*(\d{3}[-\.\s]?\d{3}[-\.\s]?\d{4})',
    r'(?i)(?:fax|télécopieur|telecopieur)(?:\s*:|\s*number|\s*#|\s*is|\s*at)?\s*(\(\d{3}\)\s*\d{3}[-\.\s]?\d{4})',
    r'(?i)(?:fax|télécopieur|telecopieur)(?:\s*:|\s*number|\s*#|\s*is|\s*at)?\s*(\d{10})',
    r'(?i)(?:fax|télécopieur|telecopieur)(?:\s*:|\s*number|\s*#|\s*is|\s*at)?\s*(\d{3}\s\d{3}\s\d{4})'
]

def search_with_openai(query: str) -> str:
    """
    Use OpenAI API to search for information about an establishment.
    
    Args:
        query: The search query string
        
    Returns:
        String containing the search results
    """
    try:
        # Using the working example provided by the user
        response = client.responses.create(
            model="gpt-4.1",  # Use gpt-4.1 model as specified
            tools=[{
                "type": "web_search_preview",  # Use web_search_preview tool type
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

def extract_fax_numbers_with_second_llm(text: str) -> List[str]:
    """
    Extract fax numbers from text using a second LLM call.
    
    Args:
        text: The text to search for fax numbers
        
    Returns:
        List of unique fax numbers found
    """
    try:
        # Create a prompt for the second LLM call
        prompt = """
        You are an expert at identifying fax numbers in text. 
        Please analyze the following text and extract all fax numbers.
        Focus specifically on identifying fax numbers (not phone numbers) for healthcare establishments.

        Look for patterns like:
        - "Fax: XXX-XXX-XXXX"
        - "Télécopieur: XXX-XXX-XXXX"
        - "Fax number is XXX-XXX-XXXX"
        - Any other format that clearly indicates a fax number

        Return ONLY a JSON array of fax numbers in the format ["XXX-XXX-XXXX", "XXX-XXX-XXXX"].
        If no fax numbers are found, return an empty array [].

        Text to analyze:
        {text}
        """
        
        # Make a call to the OpenAI API
        response = client.chat.completions.create(
            model="gpt-4.1",  # Use gpt-4.1 model for consistency
            messages=[
                {"role": "system", "content": "You are an expert at identifying fax numbers in text."},
                {"role": "user", "content": prompt.format(text=text)}
            ],
            temperature=0.1  # Low temperature for more deterministic output
        )
        
        # Extract the content from the response
        content = response.choices[0].message.content
        
        # Try to parse the response as JSON
        try:
            fax_numbers = json.loads(content)
            if isinstance(fax_numbers, list):
                return fax_numbers
        except json.JSONDecodeError:
            # If JSON parsing fails, try to extract fax numbers using regex
            return extract_fax_numbers_with_regex(content)
        
        return []
    except Exception as e:
        print(f"Error during second LLM extraction: {e}")
        return []

def extract_fax_keywords(text: str, fax_numbers: List[str]) -> Dict[str, str]:
    """
    Extract keywords for each fax number.
    
    Args:
        text: The text to analyze
        fax_numbers: List of fax numbers to find keywords for
        
    Returns:
        Dictionary mapping fax numbers to keywords
    """
    try:
        # Create a prompt for extracting keywords for each fax number
        fax_list = ", ".join(fax_numbers)
        prompt = f"""
        You are an expert at analyzing healthcare information.
        
        I have found the following fax numbers for a healthcare establishment:
        {fax_list}
        
        Based on the following text, assign ONE keyword to each fax number that best describes its purpose or department.
        Choose from keywords like: general, emergency, radiology, cardiology, pediatrics, surgery, laboratory, billing, 
        records, appointments, referrals, pharmacy, administration, etc.
        
        If you can't determine a specific purpose, use "general" as the keyword.
        
        Return your answer as a simple JSON object where keys are fax numbers and values are single keyword strings.
        Example: {{"819-123-4567": "radiology", "819-234-5678": "billing"}}
        
        Text to analyze:
        {text}
        """
        
        # Make a call to the OpenAI API
        response = client.chat.completions.create(
            model="gpt-4.1",
            messages=[
                {"role": "system", "content": "You are an expert at analyzing healthcare information."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1
        )
        
        # Extract the content from the response
        content = response.choices[0].message.content
        
        # Try to parse the response as JSON
        try:
            keywords_dict = json.loads(content)
            if isinstance(keywords_dict, dict):
                return keywords_dict
        except json.JSONDecodeError:
            print(f"Failed to parse JSON response for keywords: {content}")
            # If JSON parsing fails, return a default dictionary
            return {fax: "general" for fax in fax_numbers}
        
        return {fax: "general" for fax in fax_numbers}
    except Exception as e:
        print(f"Error during keyword extraction: {e}")
        return {fax: "general" for fax in fax_numbers}

def search_establishment_fax(name: str, address: str, website: str = None) -> Dict[str, Any]:
    """
    Search for fax numbers for a specific establishment using OpenAI API.
    
    Args:
        name: The name of the establishment
        address: The address of the establishment
        website: The website of the establishment (if available)
        
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
        
        search_result = search_with_openai(query)
        
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
        llm_fax_numbers = extract_fax_numbers_with_second_llm(search_result)
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
        fax_keywords = extract_fax_keywords(search_result, unique_fax_numbers)
    
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

def process_csv(input_path: str, output_path: str, limit: int = None):
    """
    Process the CSV file to find fax numbers for establishments.
    
    Args:
        input_path: Path to the input CSV file
        output_path: Path to the output CSV file
        limit: Maximum number of establishments to process (None for all)
    """
    # Read the input CSV
    with open(input_path, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        establishments = list(reader)
    
    # Create a new CSV with additional columns for fax numbers and keywords
    fieldnames = list(establishments[0].keys()) + ['found_fax_numbers', 'fax_keywords']
    
    # Track total fax numbers found
    total_fax_numbers = 0
    establishments_with_fax = 0
    
    # Determine how many establishments to process
    if limit is None:
        limit = len(establishments)
    else:
        limit = min(limit, len(establishments))
    
    with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Process establishments
        for i, establishment in enumerate(establishments[:limit]):
            print(f"\nProcessing establishment {i+1}/{limit}: {establishment['name']}")
            
            # Skip if fax number is already present
            if establishment.get('fax_number') and establishment.get('fax_number').strip():
                print(f"Fax number already exists: {establishment['fax_number']}")
                establishment['found_fax_numbers'] = json.dumps([establishment['fax_number']])
                establishment['fax_keywords'] = json.dumps({establishment['fax_number']: "general"})
                writer.writerow(establishment)
                total_fax_numbers += 1
                establishments_with_fax += 1
                continue
            
            # Search for fax numbers
            fax_result = search_establishment_fax(
                establishment['name'],
                establishment['address'],
                establishment.get('website')
            )
            
            fax_numbers = fax_result["fax_numbers"]
            fax_keywords = fax_result["fax_keywords"]
            
            if fax_numbers:
                establishment['found_fax_numbers'] = json.dumps(fax_numbers)
                establishment['fax_keywords'] = json.dumps(fax_keywords)
                
                # Also update the fax_number field with the first found number
                if fax_numbers:
                    establishment['fax_number'] = fax_numbers[0]
                    establishment['is_fax_enabled'] = '1'
                    total_fax_numbers += len(fax_numbers)
                    establishments_with_fax += 1
            else:
                print("No fax numbers found")
                establishment['found_fax_numbers'] = json.dumps([])
                establishment['fax_keywords'] = json.dumps({})
            
            # Write the updated row
            writer.writerow(establishment)
            
            # Flush to ensure data is written immediately
            outfile.flush()
            
            # Add a delay to avoid rate limiting
            time.sleep(3)
    
    print(f"\nSummary:")
    print(f"Total establishments processed: {limit}")
    print(f"Establishments with fax numbers: {establishments_with_fax}")
    print(f"Total fax numbers found: {total_fax_numbers}")
    
    return {
        "total_processed": limit,
        "establishments_with_fax": establishments_with_fax,
        "total_fax_numbers": total_fax_numbers
    }

if __name__ == "__main__":
    print("Starting to search for fax numbers with keywords using OpenAI API with gpt-4.1 and web_search_preview...")
    # Set limit to None to process all establishments, or a number to process a subset
    results = process_csv(INPUT_CSV_PATH, OUTPUT_CSV_PATH, limit=10)
    print(f"Processing complete. Results saved to {OUTPUT_CSV_PATH}")
    print(f"Found {results['total_fax_numbers']} fax numbers across {results['establishments_with_fax']} establishments.")
