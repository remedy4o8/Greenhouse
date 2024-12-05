import requests
import base64
from datetime import datetime, timezone

# Greenhouse API Key
api_key = ""

# Encode API key for Basic Authentication
encoded_key = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("utf-8")

# API Base URL
base_url = "https://harvest.greenhouse.io/v1/"

# Headers with encoded API Key
headers = {
    "Authorization": f"Basic {encoded_key}"
}

# List of studio names to check in job titles
studio_names = [
    "PUBG STUDIOS", "Bluehole Studio", "RisingWings", "Striking Distance Studios",
    "Dreamotion", "Unknown Worlds", "5minlab", "KRAFTON Montréal Studio",
    "ReLU Games", "Flyway Games", "OVERDARE"
]

def find_studio_in_title(title):
    """
    Check if any studio name is present in the job title.
    Default to 'Krafton' if no studio name is found.
    """
    for studio in studio_names:
        if studio.lower() in title.lower():
            return studio
    return "Krafton"

def get_job_details(job_id):
    """
    Fetch detailed information for a specific job using its ID.
    """
    response = requests.get(f"{base_url}jobs/{job_id}", headers=headers)
    
    if response.status_code != 200:
        print(f"Error: Unable to fetch details for Job ID {job_id}, Status Code: {response.status_code}")
        return {
            "Job Title": "N/A",
            "Location": "N/A",
            "Department": "N/A",
            "Days Open": "N/A",
            "Studio": "Krafton"
        }
    
    job = response.json()
    
    # Fetch job details with default fallbacks
    location = job.get("offices", [{}])[0].get("name", "Remote/Unspecified")
    department = job.get("departments", [{}])[0].get("name", "N/A")
    title = job.get("name", "N/A")
    
    # Calculate days open
    opened_at = job.get("opened_at")
    days_open = "N/A"
    if opened_at:
        try:
            opened_date = datetime.strptime(opened_at, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
            days_open = (datetime.now(timezone.utc) - opened_date).days
        except ValueError:
            print(f"Error parsing opened_at field: {opened_at}")
    
    # Check for studio name in the title
    studio = find_studio_in_title(title)
    
    return {
        "Job Title": title,
        "Location": location,
        "Department": department,
        "Days Open": days_open,
        "Studio": studio
    }

def get_open_roles():
    """
    Fetch all open roles from Greenhouse.
    """
    response = requests.get(f"{base_url}jobs", headers=headers, params={"status": "open"})
    
    if response.status_code != 200:
        print(f"Error: Unable to fetch jobs, Status Code: {response.status_code}")
        return []
    
    jobs = response.json()
    open_roles = []
    
    for job in jobs:
        job_id = job.get("id")
        if job_id:
            job_details = get_job_details(job_id)
            open_roles.append({
                "Job ID": job_id,
                **job_details
            })
    
    return open_roles

if __name__ == "__main__":
    open_roles = get_open_roles()
    
    if open_roles:
        print("Open Roles in Greenhouse:")
        for role in open_roles:
            print(f"- Job ID: {role['Job ID']}, Title: {role['Job Title']}, Location: {role['Location']}, "
                  f"Department: {role['Department']}, Days Open: {role['Days Open']}, Studio: {role['Studio']}")
    else:
        print("No open roles found.")
