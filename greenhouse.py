import os
import requests
import base64
import json
from datetime import datetime, timezone
import logging
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
class Config:
    GREENHOUSE_API_KEY = "GREENHOUSE_API_HERE"
    MONDAY_API_KEY = "MONDAY_API_HERE"
    MONDAY_BOARD_ID = "MONDAY_BOARD_HERE"
    BASE_URL = "https://harvest.greenhouse.io/v1/"
    MONDAY_API_URL = "https://api.monday.com/v2"
    MAX_RETRIES = 5
    PER_PAGE = 100
    
    STUDIO_NAMES = [
        "PUBG STUDIOS", "Bluehole Studio", "RisingWings", "Striking Distance Studios",
        "Dreamotion", "Unknown Worlds", "5minlab", "KRAFTON Montréal Studio",
        "ReLU Games", "Flyway Games", "OVERDARE"
    ]
    
    STUDIO_LOCATIONS = {
        "san ramon, ca": "Striking Distance Studios",
        "montréal": "KRAFTON Montréal Studio"
    }

# Session Setup
session = requests.Session()
retries = Retry(
    total=Config.MAX_RETRIES,
    backoff_factor=0.1,
    status_forcelist=[500, 502, 503, 504]
)
session.mount('https://', HTTPAdapter(max_retries=retries))

# Headers
encoded_key = base64.b64encode(f"{Config.GREENHOUSE_API_KEY}:".encode("utf-8")).decode("utf-8")
HEADERS = {"Authorization": f"Basic {encoded_key}"}
MONDAY_HEADERS = {
    "Authorization": Config.MONDAY_API_KEY,
    "Content-Type": "application/json"
}

def find_studio_in_title(title: str, location: str) -> str:
    """Determine studio name based on job title and location."""
    location = location.lower()
    if location in Config.STUDIO_LOCATIONS:
        return Config.STUDIO_LOCATIONS[location]
    
    title_lower = title.lower()
    for studio in Config.STUDIO_NAMES:
        if studio.lower() in title_lower:
            return studio
    return "Krafton"

def get_open_roles() -> List[Dict]:
    """Fetch all open job roles from Greenhouse API."""
    jobs_endpoint = f"{Config.BASE_URL}jobs"
    params = {"status": "open", "per_page": Config.PER_PAGE}
    open_roles = []
    page = 1

    while True:
        try:
            params["page"] = page
            response = session.get(jobs_endpoint, headers=HEADERS, params=params, timeout=10)
            response.raise_for_status()
            
            jobs = response.json()
            if not jobs:
                break

            for job in jobs:
                job_id = job.get("id")
                if not job_id:
                    continue

                opened_at = job.get("opened_at")
                days_open = (
                    (datetime.now(timezone.utc) - 
                     datetime.strptime(opened_at, "%Y-%m-%dT%H:%M:%S.%fZ")
                    .replace(tzinfo=timezone.utc)).days
                    if opened_at else "N/A"
                )

                open_roles.append({
                    "Job ID": job_id,
                    "Job Title": job.get("name", "N/A"),
                    "Location": job.get("offices", [{}])[0].get("name", "Remote/Unspecified"),
                    "Department": job.get("departments", [{}])[0].get("name", "N/A"),
                    "Days Open": days_open,
                    "Studio": find_studio_in_title(job.get("name", "N/A"), 
                                                 job.get("offices", [{}])[0].get("name", "")),
                    "Opened At": opened_at,
                    "Recruiters": ", ".join(member["first_name"] for member in 
                                          job.get("hiring_team", {}).get("recruiters", [])),
                    "Coordinators": ", ".join(member["first_name"] for member in 
                                            job.get("hiring_team", {}).get("coordinators", []))
                })
                
            page += 1
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching jobs (Page {page}): {e}")
            break
        except Exception as e:
            logging.error(f"Error processing job data: {e}")
            break

    return open_roles

def create_monday_item(role: Dict) -> bool:
    """Create a single item on Monday.com board."""
    query = """
    mutation ($board_id: ID!, $item_name: String!, $column_values: JSON!) {
        create_item(board_id: $board_id, item_name: $item_name, column_values: $column_values) {
            id
        }
    }
    """
    
    column_values = {
        "job_title__1": str(role["Job ID"]),
        "department__1": role["Department"],
        "text__1": role["Location"],
        "text2__1": role["Studio"],
        "text1__1": str(role["Days Open"]),
        "date_Mjj5SQ4B": role["Opened At"].split("T")[0] if role["Opened At"] else None,
        "text_Mjj5V04k": role["Recruiters"],
        "text_Mjj5gr7J": role["Coordinators"]
    }
    
    payload = {
        "query": query,
        "variables": {
            "board_id": Config.MONDAY_BOARD_ID,
            "item_name": role["Job Title"],
            "column_values": json.dumps(column_values)
        }
    }

    try:
        response = session.post(Config.MONDAY_API_URL, headers=MONDAY_HEADERS, 
                              json=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"Added job {role['Job ID']} ({role['Job Title']}) successfully.")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to add job {role['Job Title']}. Error: {e}")
        return False

def send_roles_to_monday(open_roles: List[Dict]) -> int:
    """Send open roles to Monday.com using parallel processing."""
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(create_monday_item, open_roles))
    return sum(results)

def main():
    """Main execution function."""
    try:
        open_roles = get_open_roles()
        if not open_roles:
            logging.info("No open roles found.")
            return
            
        added_jobs = send_roles_to_monday(open_roles)
        logging.info(f"Processed {len(open_roles)} jobs, successfully added {added_jobs}")
        
    except Exception as e:
        logging.error(f"Unexpected error in main execution: {e}")

if __name__ == "__main__":
    main()
