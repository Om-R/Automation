import os
import requests
import subprocess
import json
from dotenv import load_dotenv

# ====== CONFIG ======
load_dotenv()

JIRA_URL = "https://lendingkart.atlassian.net"
JIRA_USER = os.getenv("JIRA_USER")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")

JQL = 'project = TSE AND labels = fyno-suppression AND attachments IS NOT EMPTY AND status != Done ORDER BY created DESC'

INPUT_FILE = "/Users/omrupeshjain/Documents/Fyno/Fyno list.csv"
SUPPRESSION_SCRIPT = "/Users/omrupeshjain/PycharmProjects/Automation-Dashboard/Fyno supression list/test/main.py"
PROCESSED_FILE = "/Users/omrupeshjain/Documents/Fyno/processed_tickets.txt"

auth = (JIRA_USER, JIRA_API_TOKEN)

# ===== Helper Functions =====
def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE, "r") as f:
        return set(line.strip() for line in f.readlines())

def save_processed(issue_key):
    with open(PROCESSED_FILE, "a") as f:
        f.write(issue_key + "\n")

def fetch_new_tickets():
    print("📡 Fetching new tickets...")
    url = f"{JIRA_URL}/rest/api/3/search/jql"
    params = {"jql": JQL, "maxResults": 5, "fields": "attachment,key,summary"}
    resp = requests.get(url, params=params, auth=auth)
    print(f"🧾 Jira API Response Code: {resp.status_code}")
    if resp.status_code != 200:
        print(f"❌ Failed: {resp.text}")
        return []
    data = resp.json()
    issues = data.get("issues", [])
    print(f"✅ Found {len(issues)} ticket(s).")
    return issues

def download_attachment(issue_key, attachments):
    for att in attachments:
        filename = att["filename"]
        if filename.lower().endswith(".csv"):
            print(f"📥 Found CSV: {filename}")
            content_url = att["content"]
            resp = requests.get(content_url, auth=auth)
            resp.raise_for_status()
            with open(INPUT_FILE, "wb") as f:
                f.write(resp.content)
            print(f"✅ Saved attachment → {INPUT_FILE}")
            return True
    print("⚠️ No CSV attachment found.")
    return False

def comment_on_jira(issue_key, message):
    url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}/comment"
    data = {
        "body": {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {"type": "text", "text": message}
                    ]
                }
            ]
        }
    }
    resp = requests.post(url, json=data, auth=auth)
    if resp.status_code == 201:
        print(f"💬 Comment added to {issue_key}")
    else:
        print(f"⚠️ Failed to comment on {issue_key}: {resp.text}")

def transition_to_done(issue_key):
    def get_transition_id(issue_key, transition_name):
        url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}/transitions"
        resp = requests.get(url, auth=auth)
        if resp.status_code != 200:
            print(f"⚠️ Failed to fetch transitions for {issue_key}: {resp.text}")
            return None

        transitions = resp.json().get("transitions", [])
        for t in transitions:
            if t["name"].lower() == transition_name.lower():
                return t["id"]
        print(f"⚠️ '{transition_name}' not available for {issue_key}. Available: {[t['name'] for t in transitions]}")
        return None

    def do_transition(issue_key, transition_name):
        transition_id = get_transition_id(issue_key, transition_name)
        if not transition_id:
            return False
        url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}/transitions"
        data = {"transition": {"id": transition_id}}
        resp = requests.post(url, json=data, auth=auth)
        if resp.status_code == 204:
            print(f"✅ Transitioned {issue_key} to '{transition_name}'")
            return True
        else:
            print(f"⚠️ Failed to transition {issue_key} to '{transition_name}': {resp.text}")
            return False

    # Try Start Progress first
    if do_transition(issue_key, "Start Progress"):
        # Then Resolve the issue / Done
        if do_transition(issue_key, "Resolve the issue"):
            print("🎯 Ticket transitioned through *Start Progress* → *Resolve the issue* successfully.")
        else:
            print(f"⚠️ Could not move {issue_key} to 'Resolve the issue'")
    else:
        print(f"⚠️ Could not move {issue_key} to 'Start Progress'")



def process_ticket(issue_key):
    print(f"🚀 Processing ticket: {issue_key}")
    # comment_on_jira(issue_key, "Processing Fyno suppression request...")

    try:
        subprocess.run(["python3", SUPPRESSION_SCRIPT], check=True)
        comment_on_jira(issue_key, "User has been added to Fyno Suppression List successfully.")
        transition_to_done(issue_key)
        print(f"✅ Completed {issue_key}")
        save_processed(issue_key)

    except subprocess.CalledProcessError as e:
        comment_on_jira(issue_key, f"❌ Error running suppression script: {e}")
        print(f"❌ Error processing {issue_key}: {e}")

# ===== Main Logic =====
def main():
    processed = load_processed()
    tickets = fetch_new_tickets()

    for issue in tickets:
        issue_key = issue["key"]
        if issue_key in processed:
            print(f"⏭️ Skipping already processed ticket: {issue_key}")
            continue

        attachments = issue["fields"].get("attachment", [])
        if not attachments:
            print(f"⚠️ No attachments in {issue_key}, skipping...")
            continue

        if download_attachment(issue_key, attachments):
            process_ticket(issue_key)

if __name__ == "__main__":
    main()
