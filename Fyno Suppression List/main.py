from http.client import responses

import pandas as pd
import requests
import logging
import os
from dotenv import load_dotenv
from datetime import datetime
import smtplib
from email.message import EmailMessage
load_dotenv()

# ---------------- CONFIGURATION ---------------- #
FYNO_API_KEY = os.getenv("FYNO_API_KEY")
WSID = os.getenv("WSID")
SENDER_EMAIL = "om.jain@lendingkart.com"
RECEIVER_EMAIL = "om.jain@lendingkart.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_PASSWORD = os.getenv("EMAIL_PASSWORD")  # If using Gmail, use an App Password
INPUT_FILE = "/Users/omrupeshjain/Documents/Fyno/Fyno list.xlsx"
FYNO_BASE_URL = f"https://api.fyno.io/v1/{WSID}/suppressions"


# ------------------------------------------------ #

def setup_logging():
    """Create a unique log file per run and also keep a master log."""
    log_dir = os.path.join(os.path.dirname(INPUT_FILE), "logs")
    os.makedirs(log_dir, exist_ok=True)

    # unique filename per run
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = os.path.join(log_dir, f"fyno_suppression_{timestamp}.log")

    # main cumulative log (optional)
    latest_log = os.path.join(log_dir, "fyno_suppression_latest.log")

    if os.path.exists(latest_log):
        os.remove(latest_log)

    # Configure both handlers
    file_handler = logging.FileHandler(log_filename, mode='a')
    latest_handler = logging.FileHandler(latest_log, mode='a')
    console_handler = logging.StreamHandler()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[file_handler, latest_handler, console_handler],
    )

    logging.info(f"Log started: {log_filename}")
    return log_filename



def add_to_fyno_suppression(destination, channel, reason):
    """Add user to Fyno suppression list."""
    payload = {
        "destination": destination,
        "channel": channel,
        "description": reason
    }
    headers = {
        "Authorization": f"Bearer {FYNO_API_KEY}",
        "Content-Type": "application/json"
    }
    response = requests.post(FYNO_BASE_URL, headers=headers, json=payload)
    if response.status_code == 200:
        logging.info(f"Added user {destination} ({channel}) ({reason})to suppression list.")
        return True
    else:
        logging.warning(f"Failed to add {destination} | Status: {response.status_code} | Response: {response.text}")
        return False


# ------------------------------------------------ #
# Read Input File
# ------------------------------------------------ #
def read_input_file(file_path):
    """Read CSV or Excel file."""
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    elif file_path.endswith(".xlsx"):
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file type. Use CSV or XLSX.")
    return df


# ------------------------------------------------ #
# Send Email Summary
# ------------------------------------------------ #
def send_summary_email(summary_file, total, success, failed):
    """Send summary report via email."""
    msg = EmailMessage()
    msg["Subject"] = f"Fyno Suppression Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL

    msg.set_content(
        f"""
Hi Team,

Please find attached the summary report for today's Fyno suppression.

Summary:
- Total Request Processed: {total}
- Successfully Added/Updated: {success}
- Failed: {failed}


Regards,
Automation Script
"""
    )

    with open(summary_file, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=os.path.basename(summary_file),
        )

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, SMTP_PASSWORD)
        server.send_message(msg)
        logging.info(f"Email sent to {RECEIVER_EMAIL}")


# ------------------------------------------------ #
# Main Function
# ------------------------------------------------ #
def main():
    log_file = setup_logging()
    logging.info("Fyno Suppression Script Started")

    df = read_input_file(INPUT_FILE)
    df.columns = df.columns.str.lower()  # normalize case

    required_cols = {"destination", "channel", "reason"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Input file must contain these columns: {required_cols}")

    results = []
    total, success, failed = 0, 0, 0

    # for index, row in df.iterrows():
    #     destination = str(row["destination"]).strip()
    #     channel = str(row["channel"]).strip()
    #     reason = str(row["reason"]).strip()
    #
    #     if not destination or not channel or not reason:
    #         logging.warning(f"Skipping row {index + 1}: Missing required data.")
    #         continue
    #
    #     total += 1
    #     status = add_to_fyno_suppression(destination, channel, reason)
    #
    #     if status:
    #         success += 1
    #     else:
    #         failed += 1
    #
    #     results.append({
    #         "Destination": destination,
    #         "Channel": channel,
    #         "Reason": reason,
    #         "status": "Added/Updated" if status else f"Failed - {responses}"
    #     })

    for index, row in df.iterrows():
        destination = str(row["destination"]).strip()
        channel = str(row["channel"]).strip()
        reason = str(row["reason"]).strip()

        if not destination or not channel or not reason:
            logging.warning(f"Skipping row {index + 1}: Missing required data.")
            continue

        total += 1

        # Make API call and capture response
        payload = {
            "destination": destination,
            "channel": channel,
            "description": reason
        }
        headers = {
            "Authorization": f"Bearer {FYNO_API_KEY}",
            "Content-Type": "application/json"
        }

        response = requests.post(FYNO_BASE_URL, headers=headers, json=payload)
        response_text = response.text.strip()
        try:
            response_json = response.json()
            message = response_json.get("_message", response.text.strip())
        except Exception:
            message = response.text.strip()

        if response.status_code == 200:
            success += 1
            status = "Added/Updated"
            logging.info(f"Added user {destination} ({channel}) ({reason}) to suppression list.")
        else:
            failed += 1
            status = f"Failed"
            logging.warning(f"Failed to add {destination} | Status: {response.status_code} | Response: {response_text}")

        results.append({
            "Destination": destination,
            "Channel": channel,
            "Reason": reason,
            "Status": status,
            "API Response": message
        })

    # Save Summary File
    summary_df = pd.DataFrame(results)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    summary_filename = os.path.join(
        os.path.dirname(INPUT_FILE),
        f"fyno_summary_{timestamp}.xlsx"
    )
    summary_df.to_excel(summary_filename, index=False)

    logging.info(f"📄 Summary saved as {summary_filename}")
    logging.info(f"✅ Process completed | Total: {total} | Success: {success} | Failed: {failed}")

    # Send Email with Summary
    send_summary_email(summary_filename, total, success, failed)

    logging.info("Script finished successfully.")
    print(f"\nSummary: Total={total}, Success={success}, Failed={failed}")
    print(f"Log file: {log_file}")
    print("Process completed.")


# ------------------------------------------------ #
if __name__ == "__main__":
    main()