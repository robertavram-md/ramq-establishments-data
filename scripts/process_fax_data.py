import pandas as pd
import os
import json
import ast
import re
import sys
from groq import Groq
from tqdm import tqdm # For progress bar
import time # Added for potential future retries

# --- Configuration ---
INPUT_CSV = 'data/ramq_establishments_final_with_keywords.csv'
OUTPUT_CSV = 'data/ramq_establishments_processed.csv'
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "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") # Use provided key as fallback
MODEL = 'llama3-70b-8192'

# --- Initialize Groq Client ---
if not GROQ_API_KEY:
    print("Error: GROQ_API_KEY environment variable not set and no fallback provided.")
    sys.exit(1)
try:
    client = Groq(api_key=GROQ_API_KEY)
except Exception as e:
    print(f"Error initializing Groq client: {e}")
    sys.exit(1)

# --- Helper Functions ---

def format_fax_number(fax_num_raw):
    """Formats fax number to 1XXXYYYZZZZ (11 digits starting with 1)."""
    if not fax_num_raw or pd.isna(fax_num_raw):
        return None # Return None if input is invalid/missing

    cleaned = re.sub(r'\D', '', str(fax_num_raw))

    if len(cleaned) == 10:
        return '1' + cleaned
    elif len(cleaned) == 11 and cleaned.startswith('1'):
        return cleaned
    else:
        # Log warning but return the cleaned number anyway if format is unexpected
        # print(f"  Warning: Fax '{fax_num_raw}' cleaned to '{cleaned}' does not fit 1XXXXXXXXXX format.")
        return cleaned if cleaned else None # Return None if cleaning results in empty string

def format_fax_numbers_list(fax_list_str):
    """Parses string list, formats numbers, returns comma-separated string."""
    if pd.isna(fax_list_str) or not fax_list_str or fax_list_str in ['[]', '{}']:
        return '' # Return empty string for empty/invalid input

    try:
        fax_list = ast.literal_eval(fax_list_str)
        if not isinstance(fax_list, list):
            # print(f"Warning: Could not parse '{fax_list_str}' as a list. Returning raw.")
            return str(fax_list_str) # Return original if not a list

        formatted_numbers = [format_fax_number(num) for num in fax_list if format_fax_number(num)] # Format and filter out None results
        return ','.join(formatted_numbers)
    except (ValueError, SyntaxError):
        # print(f"Warning: Error parsing fax_numbers list string: '{fax_list_str}'. Returning raw.")
        return str(fax_list_str) # Return original string on parsing error
    except Exception as e:
        # print(f"Warning: Unexpected error parsing fax_numbers list string '{fax_list_str}': {e}. Returning raw.")
        return str(fax_list_str) # Return original string on other errors

def simplify_verbose_keyword(original_keyword, verbose_translation, target_language):
    """Simplifies a verbose translation based on original keyword context."""
    original_lower = str(original_keyword).lower() # Ensure string and lowercase

    # Check for specific terms in original keyword (add more as needed)
    if "record" in original_lower or "archive" in original_lower or "dossier" in original_lower:
        simplified = "medical records" if target_language == "English" else "dossiers médicaux"
        print(f"    Simplifying ({target_language}): '{verbose_translation}' -> '{simplified}' (based on original: '{original_keyword}')")
        return simplified
    if "appointment" in original_lower or "rendez-vous" in original_lower:
        simplified = "appointments" if target_language == "English" else "rendez-vous"
        print(f"    Simplifying ({target_language}): '{verbose_translation}' -> '{simplified}' (based on original: '{original_keyword}')")
        return simplified
    if "referral" in original_lower or "référence" in original_lower:
        simplified = "referrals" if target_language == "English" else "références"
        print(f"    Simplifying ({target_language}): '{verbose_translation}' -> '{simplified}' (based on original: '{original_keyword}')")
        return simplified
    if "billing" in original_lower or "facturation" in original_lower:
        simplified = "billing" if target_language == "English" else "facturation"
        print(f"    Simplifying ({target_language}): '{verbose_translation}' -> '{simplified}' (based on original: '{original_keyword}')")
        return simplified
    if "admin" in original_lower: # Catch 'admin', 'administration', etc.
        simplified = "administration" if target_language == "English" else "administration"
        print(f"    Simplifying ({target_language}): '{verbose_translation}' -> '{simplified}' (based on original: '{original_keyword}')")
        return simplified
    if "urgence" in original_lower or "emergency" in original_lower:
         simplified = "emergency" if target_language == "English" else "urgence"
         print(f"    Simplifying ({target_language}): '{verbose_translation}' -> '{simplified}' (based on original: '{original_keyword}')")
         return simplified
    if "reception" in original_lower or "accueil" in original_lower:
         simplified = "reception" if target_language == "English" else "accueil"
         print(f"    Simplifying ({target_language}): '{verbose_translation}' -> '{simplified}' (based on original: '{original_keyword}')")
         return simplified
    # Add more specific checks here...

    # Default simplification if no specific term found
    default_simplified = "general inquiries" if target_language == "English" else "renseignements généraux"
    print(f"    Default Simplifying ({target_language}): '{verbose_translation}' -> '{default_simplified}'")
    return default_simplified

def translate_text(text, target_language):
    """Translates text and simplifies if verbose."""
    if not text or pd.isna(text):
        return ""
    try:
        prompt = f"Translate the following keyword/phrase strictly into {target_language}. If the input looks like a phone number, return it as is. Output ONLY the translation or the original number, nothing else, no explanations, no quotes: '{text}'"
        # Basic retry mechanism
        for attempt in range(3): # Try up to 3 times
            try:
                chat_completion = client.chat.completions.create(
                    messages=[{"role": "user", "content": prompt}],
                    model=MODEL,
                    temperature=0.1,
                    max_tokens=150,
                    top_p=1,
                    stop=None,
                )
                translation = chat_completion.choices[0].message.content.strip()
                break # Success, exit retry loop
            except Exception as e_inner:
                 print(f"\nAttempt {attempt+1} failed translating '{text}' to {target_language}: {e_inner}")
                 if attempt >= 2: # If last attempt failed
                      raise e_inner # Re-raise the exception to be caught below
                 time.sleep(2 * (attempt + 1)) # Exponential backoff (2s, 4s)

        # Check verbosity
        translation_lower = translation.lower()
        # Simplified verbosity check: focus on length and parentheses as primary indicators
        # is_verbose = len(translation) > 45 or "(" in translation or ")" in translation
        # Refined check (keeps some previous terms, adjusts length)
        is_verbose = (len(translation) > 50 or
                      "(" in translation or ")" in translation or
                      "likely" in translation_lower or
                      "potentially" in translation_lower or
                      "purpose not specified" in translation_lower or
                      "used for" in translation_lower or
                      "non spécifié" in translation_lower or
                      "utilisé pour" in translation_lower or
                      "probablement" in translation_lower
                      )

        if is_verbose:
            translation = simplify_verbose_keyword(text, translation, target_language) # Pass original 'text'

        # Remove potential quotes
        if translation.startswith('"') and translation.endswith('"'):
            translation = translation[1:-1]
        if translation.startswith("'") and translation.endswith("'"):
            translation = translation[1:-1]
        return translation
    except Exception as e:
        print(f"\nError translating '{text}' to {target_language} after retries: {e}")
        return f"Error: Translation failed"

# --- Main Processing Logic ---
def main():
    print(f"Loading data from {INPUT_CSV}...")
    try:
        df = pd.read_csv(INPUT_CSV)
        print(f"Loaded {len(df)} rows.")
    except FileNotFoundError:
        print(f"Error: Input file not found at {INPUT_CSV}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        sys.exit(1)

    # --- Format 'fax_numbers' column ---
    if 'fax_numbers' in df.columns:
        print("Formatting 'fax_numbers' column...")
        tqdm.pandas(desc="Formatting fax_numbers")
        df['fax_numbers_formatted'] = df['fax_numbers'].progress_apply(format_fax_numbers_list)
        print("Finished formatting 'fax_numbers'.")
    else:
        print("Warning: 'fax_numbers' column not found. Skipping formatting.")
        df['fax_numbers_formatted'] = '' # Add empty column if original is missing


    # --- Process 'fax_keywords' column ---
    if 'fax_keywords' not in df.columns:
        print("Error: 'fax_keywords' column not found. Cannot proceed with translation.")
        # Optionally save the df with just formatted fax_numbers here if desired
        # df.to_csv(OUTPUT_CSV, index=False)
        # print(f"Saved DataFrame with formatted fax_numbers only to {OUTPUT_CSV}")
        sys.exit(1)

    print("Processing 'fax_keywords' column (formatting fax numbers and translating keywords)...")
    fax_keywords_en_results = []
    fax_keywords_fr_results = []

    # Use tqdm for progress bar over rows
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing fax_keywords"):
        fax_keywords_str = row['fax_keywords']
        row_en_data = []
        row_fr_data = []

        if pd.isna(fax_keywords_str) or not fax_keywords_str or fax_keywords_str in ['{}', '[]']:
            fax_keywords_en_results.append("[]")
            fax_keywords_fr_results.append("[]")
            continue

        try:
            fax_keywords_dict = ast.literal_eval(fax_keywords_str)

            if not isinstance(fax_keywords_dict, dict):
                fax_keywords_en_results.append("[]")
                fax_keywords_fr_results.append("[]")
                continue

            if not fax_keywords_dict:
                fax_keywords_en_results.append("[]")
                fax_keywords_fr_results.append("[]")
                continue

            # Use a temporary list to process entries for the current row
            processed_entries = []
            for fax_num_raw, keyword in fax_keywords_dict.items():
                 if pd.isna(keyword) or not str(keyword).strip():
                     continue # Skip empty keywords

                 formatted_fax = format_fax_number(fax_num_raw)
                 if formatted_fax: # Only proceed if fax number is valid after formatting
                      processed_entries.append({'formatted_fax': formatted_fax, 'keyword': keyword})

            # Translate the processed entries
            for entry in processed_entries:
                formatted_fax = entry['formatted_fax']
                keyword = entry['keyword']

                # Translate English
                keyword_en = translate_text(keyword, "English")
                if not keyword_en.startswith("Error:"):
                    row_en_data.append({"fax_number": formatted_fax, "keyword_en": keyword_en})

                # Translate French
                keyword_fr = translate_text(keyword, "French")
                if not keyword_fr.startswith("Error:"):
                     row_fr_data.append({"fax_number": formatted_fax, "keyword_fr": keyword_fr})

            fax_keywords_en_results.append(json.dumps(row_en_data))
            fax_keywords_fr_results.append(json.dumps(row_fr_data, ensure_ascii=False))

        except (ValueError, SyntaxError) as e:
            # print(f"\nWarning: Row {index+1} - Error parsing fax_keywords string: '{fax_keywords_str}'. Error: {e}")
            fax_keywords_en_results.append("[]")
            fax_keywords_fr_results.append("[]")
        except Exception as e:
            # print(f"\nWarning: Row {index+1} - Unexpected error processing fax_keywords: {e}")
            fax_keywords_en_results.append("[]")
            fax_keywords_fr_results.append("[]")

    # --- Add new columns to DataFrame ---
    df['fax_keywords_en'] = fax_keywords_en_results
    df['fax_keywords_fr'] = fax_keywords_fr_results

    print("Finished processing 'fax_keywords'.")

    # --- Save Processed Data ---
    print(f"Saving processed data to {OUTPUT_CSV}...")
    try:
        # Reorder columns to put new ones near originals
        cols = list(df.columns)
        if 'fax_numbers_formatted' in cols:
             fax_num_idx = cols.index('fax_numbers') + 1 if 'fax_numbers' in cols else len(cols) - 3
             cols.insert(fax_num_idx, cols.pop(cols.index('fax_numbers_formatted')))

        fax_key_idx = cols.index('fax_keywords') + 1 if 'fax_keywords' in cols else len(cols) - 2
        cols.insert(fax_key_idx, cols.pop(cols.index('fax_keywords_fr')))
        cols.insert(fax_key_idx, cols.pop(cols.index('fax_keywords_en')))

        df_output = df[cols]
        df_output.to_csv(OUTPUT_CSV, index=False)
        print("Save complete.")
    except Exception as e:
        print(f"Error saving final CSV: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 