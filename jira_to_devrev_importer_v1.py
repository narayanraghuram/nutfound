#!/usr/bin/env python3
"""
Jira CSV to DevRev Ticket Importer
- Reads a Jira export CSV file (semicolon-separated)
- Maps each row's status to a DevRev stage
- Creates a DevRev ticket for each valid row
"""

import csv
import requests
import pathlib
import sys
from typing import Dict, Any

# Basic settings 
CSV_PATH       = pathlib.Path("/Users/nrn/Documents/Snappins/build-lab/Jira_Export.csv")  
DEVREV_TOKEN   = "My_PAT"                                                                 # <-- your PAT
APPLIES_TO_PART = "don:core:dvrv-eu-1:devo/31CcJe9Aoi:product/3"
OWNED_BY        = ["don:identity:dvrv-eu-1:devo/31CcJe9Aoi:devu/1"]

# Match Jira statuses to DevRev stages
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



    # If a status isn't in this list, it will default to "open"
}

# DevRev API endpoint and headers
ENDPOINT = "https://api.devrev.ai/works.create"
HEADERS = {
    "Authorization": f"Bearer {DEVREV_TOKEN}",
    "Content-Type": "application/json"
}

# Check that each CSV row has Title, Description, and Status
def is_valid(row: Dict[str, str]) -> bool:
    return all(row.get(field, "").strip() for field in ("Title", "Description", "Status"))

# Create a single ticket in DevRev using data from one CSV row
def create_ticket(title: str, description: str, status: str) -> None:
    stage_name = STATUS_MAP.get(status, "open")  # fallback to "open" if status isn't mapped

    payload: Dict[str, Any] = {
        "type": "ticket",
        "applies_to_part": APPLIES_TO_PART,
        "owned_by": OWNED_BY,
        "title": title,
        "body": description,
        "stage": { "name": stage_name }
    }

    # Send the request to DevRev
    response = requests.post(ENDPOINT, headers=HEADERS, json=payload, timeout=30)

    if response.ok:
        ticket = response.json().get("work", {})
        print(f"✅ Ticket created: {ticket.get('display_id', 'Unknown ID')} — {title}")
    else:
        raise RuntimeError(f"{response.status_code} — {response.text}")

# Load CSV and process each ticket
def process_csv(csv_file: pathlib.Path):
    success = error = 0

    with csv_file.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=";")
        rows = list(reader)

    print(f"📊 Found {len(rows)} tickets to import...\n")

    for row in rows:
        if not is_valid(row):
            print(f"⚠️ Skipping row (missing required fields): {row}")
            error += 1
            continue

        try:
            create_ticket(
                title=row["Title"].strip(),
                description=row["Description"].strip(),
                status=row["Status"].strip()
            )
            success += 1
        except Exception as e:
            print(f"❌ Failed to create ticket: {e}")
            error += 1

    # Summary
    print("\n📈 Import Summary")
    print(f"   ✅ Success: {success}")
    print(f"   ❌ Failed : {error}")

# 🚀 Run the script
if __name__ == "__main__":
    print("🔧 Running Jira → DevRev import...\n")
    try:
        process_csv(CSV_PATH)
        print("\n🎉 Done! All tickets processed.")
    except Exception as err:
        print(f"\n💥 Import stopped due to error: {err}", file=sys.stderr)
        sys.exit(1)