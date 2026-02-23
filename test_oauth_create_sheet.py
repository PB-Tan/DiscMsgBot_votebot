from __future__ import annotations
from datetime import datetime, timezone
import os

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

CLIENT_SECRET = r"C:\Users\tanph\OneDrive\DAYWA\votebot\client_secret_445598494417-j7fqdg200r3eaglqcsgqefejdd66lvg0.apps.googleusercontent.com.json"  
TOKEN_FILE = r"C:\Users\tanph\OneDrive\DAYWA\votebot\token.json"            
DRIVE_FOLDER_ID = "1LMl_NmCXNGRShr4G-3DRNkRcRxK1kPL6"                                 

def get_creds():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return creds

def main():
    creds = get_creds()
    sheets = build("sheets", "v4", credentials=creds)
    drive = build("drive", "v3", credentials=creds)

    title = "VoteBot_Test_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    body = {
        "properties": {"title": title},
        "sheets": [{"properties": {"title": "Names"}}, {"properties": {"title": "Votes"}}, {"properties": {"title": "Tally"}}],
    }

    spreadsheet = sheets.spreadsheets().create(body=body).execute()
    spreadsheet_id = spreadsheet["spreadsheetId"]
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    print("Created:", url)

    # Move into folder if provided
    if DRIVE_FOLDER_ID:
        file = drive.files().get(fileId=spreadsheet_id, fields="parents").execute()
        prev = ",".join(file.get("parents", []))
        drive.files().update(
            fileId=spreadsheet_id,
            addParents=DRIVE_FOLDER_ID,
            removeParents=prev,
            fields="id,parents"
        ).execute()
        print("Moved into folder OK")

if __name__ == "__main__":
    main()