import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# CHANGE THIS PATH
GOOGLE_CREDS_JSON = r"C:\Users\tanph\OneDrive\DAYWA\votebot\test-discmsgbot-7742f7d11711.json"

# OPTIONAL: put your Drive folder ID here
DRIVE_FOLDER_ID = "1LMl_NmCXNGRShr4G-3DRNkRcRxK1kPL6"  # or "" if not using

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds = Credentials.from_service_account_file(GOOGLE_CREDS_JSON, scopes=scopes)
gc = gspread.authorize(creds)
print("Authenticated OK")

title = f"VoteBot_Test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
sh = gc.create(title)
print("Created sheet:", sh.id)

if DRIVE_FOLDER_ID:
    gc.request(
        "post",
        f"https://www.googleapis.com/drive/v3/files/{sh.id}?addParents={DRIVE_FOLDER_ID}&removeParents=root&fields=id,parents",
    )
    print("Moved into folder OK")

print("SUCCESS URL:", sh.url)