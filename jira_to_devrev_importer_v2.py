#!/usr/bin/env python3
"""
Jira CSV to DevRev Ticket Importer (Improved Version)
- Reads a Jira export CSV file (semicolon-separated)
- Maps each row's status to a DevRev stage
- Creates a DevRev ticket for each valid row
- Uses environment variables for sensitive data (like the DevRev Token)
- Uses command-line arguments for file paths and other configurations
- Implements Python's logging module for better output control
- Includes more specific error handling for API requests
"""

import csv
import requests
import pathlib
import sys
import os
import argparse
import logging
from typing import Dict, Any, List

# --- Setup Logging ---
# Configure the logger to output messages to the console with a specific format
logging.basicConfig(
    level=logging.INFO,  # Set the minimum level of messages to display (INFO, WARNING, ERROR, DEBUG)
    format='%(asctime)s - %(levelname)s - %(message)s', # Include timestamp, level, and message
    handlers=[
        logging.StreamHandler(sys.stdout) # Direct logging output to standard output
    ]
)
logger = logging.getLogger(__name__) # Get a logger instance for this module

# --- DevRev API endpoint and headers ---
# Base URL for the DevRev API endpoint to create work items
DEVREV_API_ENDPOINT = "https://api.devrev.ai/works.create"

# --- STATUS_MAP: Match Jira statuses to DevRev stages ---
# This dictionary maps Jira status strings to their corresponding DevRev stage names.
# If a Jira status is not found in this map, the 'create_ticket' function
# will default to "open" for the DevRev stage.
STATUS_MAP: Dict[str, str] = {
    "Open": "prioritized",
    "Work In Progress": "work_in_progress",
    "Blocked": "wont_fix",
    "Reopened": "in_review",
    "Closed": "Done",
    "Awaiting implementation": "awaiting_product_assist",
    "Planning": "To Do",
    "Waiting for customer": "awaiting_customer_response",
    "Implementing": "in_deployment",
    "Declined": "wont_do"
}

# --- Validation Function ---
def is_valid(row: Dict[str, str]) -> bool:
    """
    Checks if a given CSV row contains valid 'Title', 'Description', and 'Status' fields.
    Fields are considered valid if they exist and are not empty after stripping whitespace.

    Args:
        row (Dict[str, str]): A dictionary representing a single row from the CSV.

    Returns:
        bool: True if all required fields are valid, False otherwise.
    """
    required_fields = ("Title", "Description", "Status")
    return all(row.get(field, "").strip() for field in required_fields)

# --- Ticket Creation Function ---
def create_ticket(
    title: str,
    description: str,
    status: str,
    devrev_token: str,
    applies_to_part: str,
    owned_by: List[str]
) -> None:
    """
    Creates a single ticket in DevRev using the provided details.

    Args:
        title (str): The title for the DevRev ticket.
        description (str): The body/description for the DevRev ticket.
        status (str): The Jira status string, used to map to a DevRev stage.
        devrev_token (str): The Personal Access Token for DevRev API authentication.
        applies_to_part (str): The DevRev Part DON to associate the ticket with.
        owned_by (List[str]): A list of DevRev User DONs who will own the ticket.

    Raises:
        requests.exceptions.RequestException: If the API request fails (e.g., connection error, HTTP error).
        RuntimeError: If the API response is not OK and not specifically a requests exception.
    """
    # Determine the DevRev stage name based on the Jira status.
    # Defaults to "open" if the status is not found in STATUS_MAP.
    stage_name = STATUS_MAP.get(status, "open")

    # Construct the payload for the DevRev API request.
    payload: Dict[str, Any] = {
        "type": "ticket",
        "applies_to_part": applies_to_part,
        "owned_by": owned_by,
        "title": title,
        "body": description,
        "stage": {"name": stage_name}
    }

    # Prepare HTTP headers, including Authorization with the DevRev token.
    headers = {
        "Authorization": f"Bearer {devrev_token}",
        "Content-Type": "application/json"
    }

    logger.debug(f"Attempting to create ticket with payload: {payload}")

    try:
        # Send the POST request to the DevRev API.
        # Set a timeout to prevent indefinite waiting.
        response = requests.post(DEVREV_API_ENDPOINT, headers=headers, json=payload, timeout=30)
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)

        # If the request was successful, parse the JSON response and log success.
        ticket = response.json().get("work", {})
        logger.info(f"✅ Ticket created: {ticket.get('display_id', 'Unknown ID')} — {title}")

    except requests.exceptions.Timeout as e:
        logger.error(f"❌ API request timed out for ticket '{title}': {e}")
        raise # Re-raise to be caught by the calling function's try-except

    except requests.exceptions.ConnectionError as e:
        logger.error(f"❌ Connection error during API request for ticket '{title}': {e}")
        raise # Re-raise

    except requests.exceptions.HTTPError as e:
        # Log HTTP errors with status code and response text for debugging.
        logger.error(f"❌ HTTP error {e.response.status_code} for ticket '{title}': {e.response.text}")
        raise # Re-raise

    except requests.exceptions.RequestException as e:
        # Catch any other requests-related exceptions.
        logger.error(f"❌ An unknown requests error occurred for ticket '{title}': {e}")
        raise # Re-raise

    except Exception as e:
        # Catch any other unexpected errors during the process.
        logger.error(f"❌ An unexpected error occurred while creating ticket '{title}': {e}")
        raise # Re-raise

# --- Main Processing Function ---
def process_csv(
    csv_file_path: pathlib.Path,
    devrev_token: str,
    applies_to_part: str,
    owned_by: List[str]
):
    """
    Loads the CSV file, processes each row, and attempts to create DevRev tickets.
    Provides a summary of successful and failed imports.

    Args:
        csv_file_path (pathlib.Path): The path to the Jira export CSV file.
        devrev_token (str): The DevRev PAT for API calls.
        applies_to_part (str): The DevRev Part DON.
        owned_by (List[str]): List of DevRev User DONs for ownership.
    """
    success = 0
    failed = 0

    # Ensure the CSV file exists before attempting to open it.
    if not csv_file_path.exists():
        logger.error(f"💥 Error: CSV file not found at '{csv_file_path}'")
        sys.exit(1)

    logger.info(f"🔗 Attempting to open CSV file: {csv_file_path}")
    try:
        with csv_file_path.open(newline="", encoding="utf-8") as file:
            # Use DictReader to read rows as dictionaries, with semicolon as delimiter.
            reader = csv.DictReader(file, delimiter=";")
            rows = list(reader) # Load all rows into memory

        logger.info(f"📊 Found {len(rows)} tickets to import...")

        if not rows:
            logger.warning("The CSV file is empty or contains only headers. No tickets to import.")
            return

        for i, row in enumerate(rows):
            logger.debug(f"Processing row {i+1}: {row}")
            if not is_valid(row):
                logger.warning(f"⚠️ Skipping row (missing required fields): {row}")
                failed += 1
                continue

            try:
                # Call create_ticket with all necessary arguments, including config from CLI/Env
                create_ticket(
                    title=row["Title"].strip(),
                    description=row["Description"].strip(),
                    status=row["Status"].strip(),
                    devrev_token=devrev_token,
                    applies_to_part=applies_to_part,
                    owned_by=owned_by
                )
                success += 1
            except Exception:
                # create_ticket already logs the specific error, just increment failed count
                failed += 1
                pass # The exception is already logged by create_ticket, no need to re-log here.

    except FileNotFoundError:
        logger.error(f"💥 Error: The CSV file was not found at '{csv_file_path}'. Please check the path.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"💥 An unrecoverable error occurred while reading or processing the CSV: {e}")
        sys.exit(1)

    # --- Summary ---
    logger.info("\n📈 Import Summary")
    logger.info(f"   ✅ Success: {success}")
    logger.info(f"   ❌ Failed : {failed}")

# --- Main Execution Block ---
if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(
        description="Jira CSV to DevRev Ticket Importer",
        formatter_class=argparse.RawTextHelpFormatter # Preserve newlines in description
    )
    parser.add_argument(
        "csv_path",
        type=pathlib.Path,
        help="Path to the Jira export CSV file (e.g., /path/to/Jira_Export.csv)"
    )
    parser.add_argument(
        "--applies-to-part",
        type=str,
        default=os.environ.get("DEVREV_APPLIES_TO_PART"), # Default from env var
        help="DevRev Part DON (e.g., don:core:dvrv-eu-1:devo/abc:product/123).\n"
             "Can also be set via DEVREV_APPLIES_TO_PART environment variable."
    )
    parser.add_argument(
        "--owned-by",
        nargs='+', # Allows multiple values to be provided (e.g., --owned-by don1 don2)
        type=str,
        default=[os.environ.get("DEVREV_OWNED_BY_DEFAULT")] if os.environ.get("DEVREV_OWNED_BY_DEFAULT") else [],
        help="Space-separated list of DevRev User DONs who will own the ticket.\n"
             "E.g., 'don:identity:...:devu/1 don:identity:...:devu/2'.\n"
             "Can also be set via DEVREV_OWNED_BY_DEFAULT environment variable (single DON only for env var)."
    )

    args = parser.parse_args()

    # --- Retrieve DevRev Token from Environment Variable ---
    devrev_token = os.environ.get("DEVREV_TOKEN")
    if not devrev_token:
        logger.critical(
            "💥 Error: DEVREV_TOKEN environment variable not set. "
            "Please set it (e.g., export DEVREV_TOKEN='your_pat_here') "
            "before running the script."
        )
        sys.exit(1)

    # --- Validate Applies To Part and Owned By ---
    if not args.applies_to_part:
        logger.critical(
            "💥 Error: --applies-to-part argument or DEVREV_APPLIES_TO_PART environment variable "
            "is required but not provided."
        )
        sys.exit(1)

    if not args.owned_by:
        logger.critical(
            "💥 Error: --owned-by argument or DEVREV_OWNED_BY_DEFAULT environment variable "
            "is required but not provided."
        )
        sys.exit(1)

    logger.info("🔧 Running Jira → DevRev import (improved version)...")
    logger.info(f"Using CSV path: {args.csv_path}")
    logger.info(f"Tickets will apply to part: {args.applies_to_part}")
    logger.info(f"Tickets will be owned by: {args.owned_by}")

    try:
        process_csv(
            csv_file_path=args.csv_path,
            devrev_token=devrev_token,
            applies_to_part=args.applies_to_part,
            owned_by=args.owned_by
        )
        logger.info("\n🎉 Done! All tickets processed.")
    except Exception as err:
        # Catch any unexpected errors from process_csv that weren't handled internally
        logger.critical(f"\n💥 Import stopped due to unhandled error: {err}")
        sys.exit(1)
