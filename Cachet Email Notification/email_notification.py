import psycopg2
import select
import smtplib
from email.mime.text import MIMEText
import logging
import json
import sys
import time
import re

# ==========================
# Configuration
# ==========================
DB_NAME = "db name"
DB_USER = "user"
DB_PASSWORD = "password"
DB_HOST = "db host"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
FROM_EMAIL = "virendra.kaushik@lendingkart.com"
EMAIL_PASSWORD = "email app password"
TO_EMAIL = "abinash.samal@lendingkart.com"


# Poll interval (how often to check database)
POLL_INTERVAL = 10  # seconds
LOG_FILE = "incident_mail_notifier.log"
# ==========================================================
# Logging Setup
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# ==========================================================
# Database Connection
# ==========================================================
def connect_to_db():
    """Connect to PostgreSQL database with retry mechanism."""
    while True:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST
            )
            logging.info("✅ Connected to PostgreSQL database.")
            return conn
        except Exception as e:
            logging.error(f"❌ Database connection failed: {e}")
            logging.info("Retrying in 5 seconds...")
            time.sleep(5)

# ==========================================================
# Email Formatting
# ==========================================================
def format_email(incident):
    message = incident.get('message', '')

    # Extract impacted service
    impacted_match = re.search(r'Impacted Services:\s*(.+)', message)
    impacted_services = impacted_match.group(1).strip() if impacted_match else "N/A"

    # Extract ticket links
    tse_match = re.search(r'https:\/\/lendingkart\.atlassian\.net\/browse\/[A-Z]+-\d+', message)
    inc_match = re.search(r'https:\/\/lendingkart\.atlassian\.net\/browse\/INC-\w+', message)

    tse_link = tse_match.group(0) if tse_match else "https://lendingkart.atlassian.net/browse/TSE-XXX"
    inc_link = inc_match.group(0) if inc_match else "https://lendingkart.atlassian.net/browse/INC-XXXX"

    email_body = f"""
A new incident was reported at Lendingkart Cachet Status Page.

Summary: {incident.get('name', 'N/A')}

Incident Start Time: {incident.get('occurred_at', '')}

Incident End Time:

Impacted Services: {impacted_services}

TSE Ticket Link (only if reported by Ops): {tse_link}

INC Ticket Link (for RCA Tracking): {inc_link}

Thanks,  
Lendingkart Services Status Page
""".strip()

    return email_body

# ==========================================================
# Email Sending
# ==========================================================
def send_email(subject, body):
    """Send email notification using TLS-secured SMTP."""
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = FROM_EMAIL
        msg['To'] = TO_EMAIL

        logging.info("📨 Connecting to SMTP server...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls()  # 🔒 Enable TLS encryption
            smtp.login(FROM_EMAIL, EMAIL_PASSWORD)
            smtp.send_message(msg)

        logging.info(f"📧 Email sent successfully to {TO_EMAIL}")

    except Exception as e:
        logging.error(f"❌ Failed to send email: {e}")

# ==========================================================
# Database Polling
# ==========================================================
def get_latest_incident(cur):
    """Fetch the latest incident from the database."""
    cur.execute("""
        SELECT id, component_id, name, status, message, created_at, occurred_at
        FROM incidents
        ORDER BY id DESC LIMIT 1;
    """)
    row = cur.fetchone()
    if row:
        return {
            "id": row[0],
            "component_id": row[1],
            "name": row[2],
            "status": row[3],
            "message": row[4],
            "created_at": str(row[5]),
            "occurred_at": str(row[6])
        }
    return None

# ==========================================================
# Main Logic
# ==========================================================
def main():
    """Main polling loop for incident monitoring."""
    conn = connect_to_db()
    cur = conn.cursor()

    last_seen_id = None
    logging.info("👂 Watching for new incidents...")

    while True:
        try:
            incident = get_latest_incident(cur)
            if incident:
                if last_seen_id is None:
                    last_seen_id = incident["id"]
                    logging.info(f"Initial last seen ID: {last_seen_id}")
                elif incident["id"] > last_seen_id:
                    logging.info(f"🆕 New incident detected: {incident}")
                    subject = f"🚨 New Incident Alert: {incident['name']}"
                    email_body = format_email(incident)
                    send_email(subject, email_body)
                    last_seen_id = incident["id"]

            time.sleep(POLL_INTERVAL)

        except Exception as e:
            logging.error(f"❌ Error while polling incidents: {e}")
            time.sleep(5)
            conn = connect_to_db()
            cur = conn.cursor()

# ==========================================================
# Entry Point
# ==========================================================
if __name__ == "__main__":
    main()