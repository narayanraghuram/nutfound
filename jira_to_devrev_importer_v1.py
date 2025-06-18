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
DEVREV_TOKEN   = "eyJhbGciOiJSUzI1NiIsImlzcyI6Imh0dHBzOi8vYXV0aC10b2tlbi5kZXZyZXYuYWkvIiwia2lkIjoic3RzX2tpZF9yc2EiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOlsiamFudXMiXSwiYXpwIjoiZG9uOmlkZW50aXR5OmR2cnYtZXUtMTpkZXZvLzMxQ2NKZTlBb2k6ZGV2dS8xIiwiZXhwIjoxNzgxMzAxNTc4LCJodHRwOi8vZGV2cmV2LmFpL2F1dGgwX3VpZCI6ImRvbjppZGVudGl0eTpkdnJ2LXVzLTE6ZGV2by9zdXBlcjphdXRoMF91c2VyL2xpbmtlZGlufFIzbGVYUk5CZFoiLCJodHRwOi8vZGV2cmV2LmFpL2F1dGgwX3VzZXJfaWQiOiJsaW5rZWRpbnxSM2xlWFJOQmRaIiwiaHR0cDovL2RldnJldi5haS9kZXZvX2RvbiI6ImRvbjppZGVudGl0eTpkdnJ2LWV1LTE6ZGV2by8zMUNjSmU5QW9pIiwiaHR0cDovL2RldnJldi5haS9kZXZvaWQiOiJERVYtMzFDY0plOUFvaSIsImh0dHA6Ly9kZXZyZXYuYWkvZGV2dWlkIjoiREVWVS0xIiwiaHR0cDovL2RldnJldi5haS9kaXNwbGF5bmFtZSI6Im5ybi1yYWdodXJhbSIsImh0dHA6Ly9kZXZyZXYuYWkvZW1haWwiOiJucm4ucmFnaHVyYW1AZ21haWwuY29tIiwiaHR0cDovL2RldnJldi5haS9mdWxsbmFtZSI6Ik5hcmF5YW4gUmFnaHVyYW0iLCJodHRwOi8vZGV2cmV2LmFpL2lzX3ZlcmlmaWVkIjp0cnVlLCJodHRwOi8vZGV2cmV2LmFpL3Rva2VudHlwZSI6InVybjpkZXZyZXY6cGFyYW1zOm9hdXRoOnRva2VuLXR5cGU6cGF0IiwiaWF0IjoxNzQ5NzY1NTc4LCJpc3MiOiJodHRwczovL2F1dGgtdG9rZW4uZGV2cmV2LmFpLyIsImp0aSI6ImRvbjppZGVudGl0eTpkdnJ2LWV1LTE6ZGV2by8zMUNjSmU5QW9pOnRva2VuL05zNnBmSmU0Iiwib3JnX2lkIjoib3JnX1pWdVhFZkZKWVNJeWZiWUwiLCJzdWIiOiJkb246aWRlbnRpdHk6ZHZydi1ldS0xOmRldm8vMzFDY0plOUFvaTpkZXZ1LzEifQ.BRESpxz9SJtfkwhBBWHZ_Y7q277Dolb9Uwdfw1v0gADU6kW15xx5dYxES79xkIO4ibw0aPD2jWFwsjW2oGmRg3OLa1a09UC71HwshZmhXgOAo-bL92V8pjO0Llec39sXIJ3JzONNqSiNTE7oO38jzjKNkW9_ljp3Py5zNpdlGlCmr0yjYfMoFE8PF8DnRbbbiFfxhl37U9o8giB-t-YI60T_wWj_msKB8LKgZ2tp_UCeiSMFWLAk-Ip3GZPMOmzLMuGHCKiiwB7BQzQY7lsOVTR3MEdJz4lMpzWwumuRHg9qAEVIamUG-QrKR1lHhhvgR8EaMStKM6Ey3ONVpCq_7Q"                                                                 # <-- your PAT
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