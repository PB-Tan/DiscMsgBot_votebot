from __future__ import annotations

import os
import asyncio
import html
import re
import csv
import time
import json
from itertools import zip_longest
from datetime import datetime, timezone, timedelta
from typing import Optional, Callable, Awaitable, Any
from urllib.parse import urlparse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes
)
from telegram.ext import PollAnswerHandler
import uuid

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# --------------------
# Config
# --------------------
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ["BOT_TOKEN"]
TELEGRAM_WEBHOOK_URL = os.environ["TELEGRAM_WEBHOOK_URL"]  # e.g. https://your-render-service.onrender.com/webhook
PORT = int(os.environ.get("PORT", "10000"))
CLIENT_JSON = os.environ["OAUTH_CLIENT_JSON"].strip()  # file path or raw OAuth client JSON
TOKEN_JSON = os.environ["OAUTH_TOKEN_JSON"].strip()  # file path or raw authorized-user token JSON
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()
SHEET_LINK_SHARE_ROLE = os.environ.get("SHEET_LINK_SHARE_ROLE", "").strip().lower()  # "", reader, writer, commenter
SHEET_LINK_ALLOW_DISCOVERY = os.environ.get("SHEET_LINK_ALLOW_DISCOVERY", "false").strip().lower() in {"1", "true", "yes"}
TRACKER_OVERVIEW_TITLE = os.environ.get("TRACKER_OVERVIEW_TITLE", "0.VoteBot_metadata").strip() or "0.VoteBot_metadata"
MEMBER_CHECK_CSV_PATH = os.environ.get("MEMBER_CHECK_CSV_PATH", "").strip()
MEMBER_CHECK_SOURCE = os.environ.get("MEMBER_CHECK_SOURCE", "").strip()  # spreadsheet ID or full URL
MEMBER_CHECK_TAB = os.environ.get("MEMBER_CHECK_TAB", "Member Check").strip() or "Member Check"
MEMBER_CHECK_REFRESH_SECONDS = int(os.environ.get("MEMBER_CHECK_REFRESH_SECONDS", "60"))
MEMBER_RAW_SOURCE = os.environ.get("MEMBER_RAW_SOURCE", "").strip()  # spreadsheet ID or full URL
MEMBER_RAW_EXISTING_HANDLE_RANGE = os.environ.get("MEMBER_RAW_EXISTING_HANDLE_RANGE", "Existing Members!D5:D").strip()
MEMBER_RAW_EXISTING_GENDER_RANGE = os.environ.get("MEMBER_RAW_EXISTING_GENDER_RANGE", "Existing Members!E5:E").strip()
MEMBER_RAW_NEWCOMER_HANDLE_RANGE = os.environ.get("MEMBER_RAW_NEWCOMER_HANDLE_RANGE", "DAYWA Newcomers List!F2:F").strip()
MEMBER_RAW_NEWCOMER_GENDER_RANGE = os.environ.get("MEMBER_RAW_NEWCOMER_GENDER_RANGE", "DAYWA Newcomers List!G2:G").strip()
NATIVE_POLL_STATE_FILE = os.environ.get("NATIVE_POLL_STATE_FILE", "native_poll_states.json").strip() or "native_poll_states.json"
ALLOWED_TELEGRAM_USER_IDS_RAW = os.environ.get("ALLOWED_TELEGRAM_USER_IDS", "").strip()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

CHOICES = [
    ("discussion session only", "No"),   # (label, Lunch)
    ("discussion session + lunch", "Yes"),
]


def _parse_allowed_telegram_user_ids(raw: str) -> set[int]:
    out: set[int] = set()
    invalid: list[str] = []
    for token in re.split(r"[\s,]+", (raw or "").strip()):
        if not token:
            continue
        try:
            out.add(int(token))
        except ValueError:
            invalid.append(token)
    if invalid:
        print("Ignoring invalid ALLOWED_TELEGRAM_USER_IDS values:", ", ".join(invalid))
    return out


ALLOWED_TELEGRAM_USER_IDS = _parse_allowed_telegram_user_ids(ALLOWED_TELEGRAM_USER_IDS_RAW)

NAMES_HEADERS = [
    'TG Link',
    "Name (auto)",
    "TG Handle (auto)",
    "Sign up status \n[Signed up, Invite sent, Confirmed, Pulled out, Waitlisted]",
    "Chosen Option (auto)",
    "Lunch\n[Yes, No]",
    "Profile ",
    "Gender",
    "New? (auto)",
    "Comments / Changes from VoteBot",
]

SIGNUP_STATUS_OPTIONS = [
    "Signed up",
    "Invite sent",
    "Confirmed",
    "Pulled out",
    "Waitlisted",
]

PROFILE_OPTIONS = [
    "Newcomer",
    "Enhanced Facilitation",
    "Facilitator",
]

GROUPINGS_AUTOMATED_SHEET_TITLE = "Groupings (automated)"
GROUPING_FACILITATOR_PROFILE_OPTIONS = [
    "A. Experienced",
    "B. Regular",
    "C. New",
]
GROUPING_FACILITATOR_ACTIVE_OPTIONS = ["Yes", "No"]
GROUPING_FACILITATOR_DEFAULT_ROWS = [
    ("A. Experienced", "Yes"),
    ("A. Experienced", "Yes"),
    ("A. Experienced", "Yes"),
    ("B. Regular", "Yes"),
    ("B. Regular", "Yes"),
    ("B. Regular", "Yes"),
    ("B. Regular", "Yes"),
    ("C. New", "Yes"),
    ("C. New", "Yes"),
]

TALLY_STATUS_ORDER = [
    "Signed up",
    "Invite sent",
    "Confirmed",
    "Pulled out",
    "Waitlisted",
]

VOTES_HEADERS = [
    "date_utc8", "time_utc8", "user_id", "username", "full_name", "choice", "lunch", "action"
]
TRACKER_OVERVIEW_TAB = "Tracker"
TRACKER_OVERVIEW_HEADERS = [
    "S/N",
    "poll_id",
    "event_date",
    "event_title",
    "gSheet Url",
    "poll_status",
    "created_by",
    "date_created",
    "time_created",
    "date_closed",
    "time_closed",
    "closed_by",
    "total_votes",
    "option1_votes",
    "option2_votes",
    "option3_votes",
    "option4_votes",
]

PUBLISHPOLL_SAMPLE_TEMPLATE = (
    "/publishpoll\n"
    "title=<Enter event title here>\n"
    "date=22-Feb-2026\n"
    "desc=<Write a short description of the event>\n"
    "venue=<Enter event location here>\n"
    "lunch=12:30-2pm\n"
    "session=2-4pm\n"
    "option1=discussion session only\n"
    "option2=discussion session + lunch\n"
    "cap=50\n\n"
)

PUBLISHPOLL_MINIMAL_TEMPLATE = (
    "/publishpoll\n"
    "title=<Enter event title here>\n"
    "date=22-Feb-2026\n\n"
)

PUBLISHPOLL_SAMPLE_GUIDE = (
    "Get started with /publishpoll\n\n"
    "1. Copy one of the templates below.\n"
    "2. Fill at least title and date.\n"
    "3. Copy-paste it as one Telegram message.\n"
    "4. Review the preview, then tap Publish.\n\n"
    "Some rules:\n"
    "- Use one key per line in key=value or key:value format.\n"
    "- date is required and must be DD-MMM-YYYY (example: 22-Feb-2026).\n"
    "- option1 (discussion session) and option2 (discussion session + lunch) are used by default if not provided.\n"
    "- option3 and option4 are optional and only used when filled.\n"
    "- cap is optional; if cap > 0, poll auto-closes at max votes; if not provided, poll has to be manually closed\n"
    "- lunch1..lunch4 are optional lunch flags per option.\n"
    "- lunch1 (No) and lunch2 (Yes) are the default values if not provided.\n\n"
    "Minimal template:\n"
    f"{PUBLISHPOLL_MINIMAL_TEMPLATE}"
    "Full template:\n"
    f"{PUBLISHPOLL_SAMPLE_TEMPLATE}"
)


# --------------------
# Google OAuth helpers
# --------------------
def _env_json_or_path(value: str, env_name: str) -> tuple[Optional[dict], Optional[str]]:
    raw = (value or "").strip()
    if not raw:
        raise ValueError(f"{env_name} is empty")

    if raw.startswith("{"):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"{env_name} looks like JSON but could not be parsed: {e}") from e
        if not isinstance(parsed, dict):
            raise ValueError(f"{env_name} must be a JSON object")
        return parsed, None

    return None, raw


def _write_token_file(token_path: str, creds: Credentials) -> None:
    token_dir = os.path.dirname(token_path)
    if token_dir:
        os.makedirs(token_dir, exist_ok=True)
    with open(token_path, "w", encoding="utf-8") as f:
        f.write(creds.to_json())


def load_or_login_creds() -> Credentials:
    client_info, client_path = _env_json_or_path(CLIENT_JSON, "OAUTH_CLIENT_JSON")
    token_info, token_path = _env_json_or_path(TOKEN_JSON, "OAUTH_TOKEN_JSON")

    creds: Optional[Credentials] = None
    if token_info is not None:
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)
    elif token_path and os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if creds and not creds.valid and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            if token_path:
                _write_token_file(token_path, creds)
        except Exception as e:
            print("OAuth token refresh failed; falling back to interactive login:", e)

    if not creds or not creds.valid:
        if client_info is not None:
            flow = InstalledAppFlow.from_client_config(client_info, SCOPES)
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_path, SCOPES)
        creds = flow.run_local_server(port=0)
        if token_path:
            _write_token_file(token_path, creds)
        else:
            print("OAUTH_TOKEN_JSON is raw JSON; new token cannot be persisted automatically.")
    return creds


def _compact_sheet_title_part(value: str, max_len: int = 50) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "")).strip()
    cleaned = re.sub(r"[^\w\s\-]", "", cleaned)
    cleaned = cleaned.replace(" ", "_")
    return cleaned[:max_len].strip("_")


def _normalize_share_role(role: str) -> str:
    r = (role or "").strip().lower()
    aliases = {
        "view": "reader",
        "viewer": "reader",
        "read": "reader",
        "reader": "reader",
        "comment": "commenter",
        "commenter": "commenter",
        "edit": "writer",
        "editor": "writer",
        "write": "writer",
        "writer": "writer",
    }
    return aliases.get(r, "")


def _format_actor_label(handle: Optional[str], user_id: Optional[str]) -> str:
    actor_handle = str(handle or "").strip()
    if actor_handle:
        return actor_handle
    actor_user_id = str(user_id or "").strip()
    if actor_user_id:
        return f"user_id:{actor_user_id}"
    return ""


def _escape_drive_query_value(value: str) -> str:
    return str(value or "").replace("\\", "\\\\").replace("'", "\\'")


def _sheet_col_letter(index_1_based: int) -> str:
    n = max(1, int(index_1_based))
    letters = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


TRACKER_OVERVIEW_END_COL = _sheet_col_letter(len(TRACKER_OVERVIEW_HEADERS))
TRACKER_OVERVIEW_COL_SN = TRACKER_OVERVIEW_HEADERS.index("S/N")
TRACKER_OVERVIEW_COL_POLL_ID = TRACKER_OVERVIEW_HEADERS.index("poll_id")
TRACKER_OVERVIEW_COL_EVENT_DATE = TRACKER_OVERVIEW_HEADERS.index("event_date")
TRACKER_OVERVIEW_COL_EVENT_TITLE = TRACKER_OVERVIEW_HEADERS.index("event_title")
TRACKER_OVERVIEW_COL_GSHEET_URL = TRACKER_OVERVIEW_HEADERS.index("gSheet Url")
TRACKER_OVERVIEW_COL_POLL_STATUS = TRACKER_OVERVIEW_HEADERS.index("poll_status")
TRACKER_OVERVIEW_COL_CREATED_BY = TRACKER_OVERVIEW_HEADERS.index("created_by")
TRACKER_OVERVIEW_COL_DATE_CREATED = TRACKER_OVERVIEW_HEADERS.index("date_created")
TRACKER_OVERVIEW_COL_TIME_CREATED = TRACKER_OVERVIEW_HEADERS.index("time_created")
TRACKER_OVERVIEW_COL_DATE_CLOSED = TRACKER_OVERVIEW_HEADERS.index("date_closed")
TRACKER_OVERVIEW_COL_TIME_CLOSED = TRACKER_OVERVIEW_HEADERS.index("time_closed")
TRACKER_OVERVIEW_COL_CLOSED_BY = TRACKER_OVERVIEW_HEADERS.index("closed_by")
TRACKER_OVERVIEW_COL_TOTAL_VOTES = TRACKER_OVERVIEW_HEADERS.index("total_votes")
TRACKER_OVERVIEW_COL_OPTION1_VOTES = TRACKER_OVERVIEW_HEADERS.index("option1_votes")
TRACKER_OVERVIEW_COL_OPTION2_VOTES = TRACKER_OVERVIEW_HEADERS.index("option2_votes")
TRACKER_OVERVIEW_COL_OPTION3_VOTES = TRACKER_OVERVIEW_HEADERS.index("option3_votes")
TRACKER_OVERVIEW_COL_OPTION4_VOTES = TRACKER_OVERVIEW_HEADERS.index("option4_votes")


def _normalize_tracker_row_values(values: list[Any]) -> list[str]:
    row_values = [str(v) for v in list(values[:len(TRACKER_OVERVIEW_HEADERS)])]
    while len(row_values) < len(TRACKER_OVERVIEW_HEADERS):
        row_values.append("")
    return row_values


def _normalize_tracker_option_counts(values: Optional[list[int]]) -> list[int]:
    out: list[int] = []
    raw = list(values or [])
    for i in range(4):
        try:
            v = int(raw[i])
        except (IndexError, TypeError, ValueError):
            v = 0
        out.append(max(0, v))
    return out


def _next_tracker_serial_number(sheets, tracker_spreadsheet_id: str) -> int:
    resp = sheets.spreadsheets().values().get(
        spreadsheetId=tracker_spreadsheet_id,
        range=f"{TRACKER_OVERVIEW_TAB}!A2:A",
    ).execute()
    max_sn = 0
    for row in resp.get("values", []):
        if not row:
            continue
        try:
            sn = int(str(row[0]).strip())
        except (TypeError, ValueError):
            continue
        if sn > max_sn:
            max_sn = sn
    return max_sn + 1 if max_sn > 0 else 1


def _ensure_file_in_drive_folder(drive, file_id: str) -> None:
    if not DRIVE_FOLDER_ID:
        return
    file_meta = drive.files().get(fileId=file_id, fields="parents").execute()
    prev_parents = ",".join(file_meta.get("parents", []))
    drive.files().update(
        fileId=file_id,
        addParents=DRIVE_FOLDER_ID,
        removeParents=prev_parents,
        fields="id,parents",
    ).execute()


def _ensure_tracker_overview_layout(sheets, tracker_spreadsheet_id: str) -> None:
    meta = sheets.spreadsheets().get(
        spreadsheetId=tracker_spreadsheet_id,
        fields="sheets.properties(title)",
    ).execute()
    tab_titles = {
        str(((sheet.get("properties") or {}).get("title") or "")).strip()
        for sheet in meta.get("sheets", [])
    }
    if TRACKER_OVERVIEW_TAB not in tab_titles:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=tracker_spreadsheet_id,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {"title": TRACKER_OVERVIEW_TAB}
                        }
                    }
                ]
            },
        ).execute()

    sheets.spreadsheets().values().update(
        spreadsheetId=tracker_spreadsheet_id,
        range=f"{TRACKER_OVERVIEW_TAB}!A1:{TRACKER_OVERVIEW_END_COL}1",
        valueInputOption="RAW",
        body={"values": [TRACKER_OVERVIEW_HEADERS]},
    ).execute()


def get_or_create_tracker_overview_spreadsheet(sheets, drive) -> tuple[str, str]:
    global TRACKER_OVERVIEW_SPREADSHEET_ID

    if TRACKER_OVERVIEW_SPREADSHEET_ID:
        tracker_id = TRACKER_OVERVIEW_SPREADSHEET_ID
        _ensure_tracker_overview_layout(sheets, tracker_id)
        return tracker_id, f"https://docs.google.com/spreadsheets/d/{tracker_id}"

    escaped_title = _escape_drive_query_value(TRACKER_OVERVIEW_TITLE)
    query_parts = [
        "mimeType='application/vnd.google-apps.spreadsheet'",
        "trashed=false",
        f"name='{escaped_title}'",
    ]
    if DRIVE_FOLDER_ID:
        query_parts.append(f"'{_escape_drive_query_value(DRIVE_FOLDER_ID)}' in parents")
    query = " and ".join(query_parts)

    found = drive.files().list(
        q=query,
        fields="files(id,name,createdTime)",
        pageSize=5,
        orderBy="createdTime desc",
    ).execute()
    files = found.get("files", [])
    if files:
        tracker_id = str(files[0].get("id", "")).strip()
    else:
        ss = sheets.spreadsheets().create(
            body={
                "properties": {"title": TRACKER_OVERVIEW_TITLE},
                "sheets": [{"properties": {"title": TRACKER_OVERVIEW_TAB}}],
            }
        ).execute()
        tracker_id = ss["spreadsheetId"]
        _ensure_file_in_drive_folder(drive, tracker_id)

        share_role = _normalize_share_role(SHEET_LINK_SHARE_ROLE)
        if share_role:
            drive.permissions().create(
                fileId=tracker_id,
                body={
                    "type": "anyone",
                    "role": share_role,
                    "allowFileDiscovery": SHEET_LINK_ALLOW_DISCOVERY,
                },
                fields="id,type,role",
            ).execute()

    TRACKER_OVERVIEW_SPREADSHEET_ID = tracker_id
    _ensure_tracker_overview_layout(sheets, tracker_id)
    return tracker_id, f"https://docs.google.com/spreadsheets/d/{tracker_id}"


def _find_tracker_row_by_poll_id(
    sheets,
    tracker_spreadsheet_id: str,
    poll_id: str,
) -> Optional[int]:
    wanted_poll_id = str(poll_id or "").strip()
    if not wanted_poll_id:
        return None

    resp = sheets.spreadsheets().values().get(
        spreadsheetId=tracker_spreadsheet_id,
        range=f"{TRACKER_OVERVIEW_TAB}!B2:B",
    ).execute()
    values = resp.get("values", [])
    for idx, row in enumerate(values, start=2):
        existing_poll_id = str(row[0]).strip() if row else ""
        if existing_poll_id == wanted_poll_id:
            return idx
    return None


def upsert_tracker_overview_row(
    sheets,
    drive,
    *,
    poll_id: str,
    poll_title: str,
    poll_date: str,
    gsheet_url: str,
    poll_status: str,
    created_by: str,
    date_created: str = "",
    time_created: str = "",
    date_closed: str = "",
    time_closed: str = "",
    closed_by: str = "",
    total_votes: int = 0,
    option_vote_counts: Optional[list[int]] = None,
) -> None:
    normalized_poll_id = str(poll_id or "").strip()
    if not normalized_poll_id:
        return

    normalized_status = str(poll_status or "open").strip() or "open"
    normalized_poll_date = str(poll_date or "").strip()
    if not normalized_poll_date:
        normalized_poll_date, _ = now_utc8_date_time()
    normalized_date_created = str(date_created or "").strip()
    normalized_time_created = str(time_created or "").strip()
    if not normalized_date_created or not normalized_time_created:
        default_date, default_time = now_utc8_date_time()
        if not normalized_date_created:
            normalized_date_created = default_date
        if not normalized_time_created:
            normalized_time_created = default_time
    try:
        normalized_total_votes = max(0, int(total_votes))
    except (TypeError, ValueError):
        normalized_total_votes = 0
    option_counts = _normalize_tracker_option_counts(option_vote_counts)

    tracker_spreadsheet_id, _ = get_or_create_tracker_overview_spreadsheet(sheets, drive)
    target_row = _find_tracker_row_by_poll_id(sheets, tracker_spreadsheet_id, normalized_poll_id)
    if target_row is None:
        row_values = [""] * len(TRACKER_OVERVIEW_HEADERS)
        row_values[TRACKER_OVERVIEW_COL_SN] = str(_next_tracker_serial_number(sheets, tracker_spreadsheet_id))
        row_values[TRACKER_OVERVIEW_COL_POLL_ID] = normalized_poll_id
        row_values[TRACKER_OVERVIEW_COL_EVENT_DATE] = normalized_poll_date
        row_values[TRACKER_OVERVIEW_COL_EVENT_TITLE] = str(poll_title or "").strip()
        row_values[TRACKER_OVERVIEW_COL_GSHEET_URL] = str(gsheet_url or "").strip()
        row_values[TRACKER_OVERVIEW_COL_POLL_STATUS] = normalized_status
        row_values[TRACKER_OVERVIEW_COL_CREATED_BY] = str(created_by or "").strip()
        row_values[TRACKER_OVERVIEW_COL_DATE_CREATED] = normalized_date_created
        row_values[TRACKER_OVERVIEW_COL_TIME_CREATED] = normalized_time_created
        row_values[TRACKER_OVERVIEW_COL_DATE_CLOSED] = str(date_closed or "").strip()
        row_values[TRACKER_OVERVIEW_COL_TIME_CLOSED] = str(time_closed or "").strip()
        row_values[TRACKER_OVERVIEW_COL_CLOSED_BY] = str(closed_by or "").strip()
        row_values[TRACKER_OVERVIEW_COL_TOTAL_VOTES] = str(normalized_total_votes)
        row_values[TRACKER_OVERVIEW_COL_OPTION1_VOTES] = str(option_counts[0])
        row_values[TRACKER_OVERVIEW_COL_OPTION2_VOTES] = str(option_counts[1])
        row_values[TRACKER_OVERVIEW_COL_OPTION3_VOTES] = str(option_counts[2])
        row_values[TRACKER_OVERVIEW_COL_OPTION4_VOTES] = str(option_counts[3])
        sheets.spreadsheets().values().append(
            spreadsheetId=tracker_spreadsheet_id,
            range=f"{TRACKER_OVERVIEW_TAB}!A:{TRACKER_OVERVIEW_END_COL}",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row_values]},
        ).execute()
        return

    row_resp = sheets.spreadsheets().values().get(
        spreadsheetId=tracker_spreadsheet_id,
        range=f"{TRACKER_OVERVIEW_TAB}!A{target_row}:{TRACKER_OVERVIEW_END_COL}{target_row}",
    ).execute()
    existing = (row_resp.get("values") or [[]])[0]
    row_values = _normalize_tracker_row_values(existing)
    if not row_values[TRACKER_OVERVIEW_COL_SN].strip().isdigit():
        row_values[TRACKER_OVERVIEW_COL_SN] = str(_next_tracker_serial_number(sheets, tracker_spreadsheet_id))
    row_values[TRACKER_OVERVIEW_COL_POLL_ID] = normalized_poll_id
    row_values[TRACKER_OVERVIEW_COL_EVENT_DATE] = normalized_poll_date
    row_values[TRACKER_OVERVIEW_COL_EVENT_TITLE] = str(poll_title or "").strip()
    row_values[TRACKER_OVERVIEW_COL_GSHEET_URL] = str(gsheet_url or "").strip()
    row_values[TRACKER_OVERVIEW_COL_POLL_STATUS] = normalized_status
    row_values[TRACKER_OVERVIEW_COL_CREATED_BY] = str(created_by or "").strip()
    row_values[TRACKER_OVERVIEW_COL_DATE_CREATED] = normalized_date_created
    row_values[TRACKER_OVERVIEW_COL_TIME_CREATED] = normalized_time_created
    row_values[TRACKER_OVERVIEW_COL_DATE_CLOSED] = str(date_closed or "").strip()
    row_values[TRACKER_OVERVIEW_COL_TIME_CLOSED] = str(time_closed or "").strip()
    row_values[TRACKER_OVERVIEW_COL_CLOSED_BY] = str(closed_by or "").strip()
    row_values[TRACKER_OVERVIEW_COL_TOTAL_VOTES] = str(normalized_total_votes)
    row_values[TRACKER_OVERVIEW_COL_OPTION1_VOTES] = str(option_counts[0])
    row_values[TRACKER_OVERVIEW_COL_OPTION2_VOTES] = str(option_counts[1])
    row_values[TRACKER_OVERVIEW_COL_OPTION3_VOTES] = str(option_counts[2])
    row_values[TRACKER_OVERVIEW_COL_OPTION4_VOTES] = str(option_counts[3])

    sheets.spreadsheets().values().update(
        spreadsheetId=tracker_spreadsheet_id,
        range=f"{TRACKER_OVERVIEW_TAB}!A{target_row}:{TRACKER_OVERVIEW_END_COL}{target_row}",
        valueInputOption="RAW",
        body={"values": [row_values]},
    ).execute()


def update_tracker_overview_poll_status(
    sheets,
    drive,
    *,
    poll_id: str,
    poll_status: str,
    closed_by: str = "",
) -> None:
    normalized_poll_id = str(poll_id or "").strip()
    if not normalized_poll_id:
        return

    normalized_status = str(poll_status or "").strip()
    if not normalized_status:
        return

    tracker_spreadsheet_id, _ = get_or_create_tracker_overview_spreadsheet(sheets, drive)
    target_row = _find_tracker_row_by_poll_id(sheets, tracker_spreadsheet_id, normalized_poll_id)
    close_date, close_time = now_utc8_date_time()
    if target_row is None:
        row_values = [""] * len(TRACKER_OVERVIEW_HEADERS)
        row_values[TRACKER_OVERVIEW_COL_SN] = str(_next_tracker_serial_number(sheets, tracker_spreadsheet_id))
        row_values[TRACKER_OVERVIEW_COL_POLL_ID] = normalized_poll_id
        row_values[TRACKER_OVERVIEW_COL_POLL_STATUS] = normalized_status
        row_values[TRACKER_OVERVIEW_COL_TOTAL_VOTES] = "0"
        row_values[TRACKER_OVERVIEW_COL_OPTION1_VOTES] = "0"
        row_values[TRACKER_OVERVIEW_COL_OPTION2_VOTES] = "0"
        row_values[TRACKER_OVERVIEW_COL_OPTION3_VOTES] = "0"
        row_values[TRACKER_OVERVIEW_COL_OPTION4_VOTES] = "0"
        if normalized_status.lower() == "closed":
            row_values[TRACKER_OVERVIEW_COL_DATE_CLOSED] = close_date
            row_values[TRACKER_OVERVIEW_COL_TIME_CLOSED] = close_time
            row_values[TRACKER_OVERVIEW_COL_CLOSED_BY] = str(closed_by or "").strip()
        sheets.spreadsheets().values().append(
            spreadsheetId=tracker_spreadsheet_id,
            range=f"{TRACKER_OVERVIEW_TAB}!A:{TRACKER_OVERVIEW_END_COL}",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row_values]},
        ).execute()
        return

    row_resp = sheets.spreadsheets().values().get(
        spreadsheetId=tracker_spreadsheet_id,
        range=f"{TRACKER_OVERVIEW_TAB}!A{target_row}:{TRACKER_OVERVIEW_END_COL}{target_row}",
    ).execute()
    existing = (row_resp.get("values") or [[]])[0]
    row_values = _normalize_tracker_row_values(existing)
    if not row_values[TRACKER_OVERVIEW_COL_SN].strip().isdigit():
        row_values[TRACKER_OVERVIEW_COL_SN] = str(_next_tracker_serial_number(sheets, tracker_spreadsheet_id))
    row_values[TRACKER_OVERVIEW_COL_POLL_STATUS] = normalized_status
    if normalized_status.lower() == "closed":
        row_values[TRACKER_OVERVIEW_COL_DATE_CLOSED] = close_date
        row_values[TRACKER_OVERVIEW_COL_TIME_CLOSED] = close_time
        row_values[TRACKER_OVERVIEW_COL_CLOSED_BY] = str(closed_by or "").strip()

    sheets.spreadsheets().values().update(
        spreadsheetId=tracker_spreadsheet_id,
        range=f"{TRACKER_OVERVIEW_TAB}!A{target_row}:{TRACKER_OVERVIEW_END_COL}{target_row}",
        valueInputOption="RAW",
        body={"values": [row_values]},
    ).execute()


def update_tracker_overview_aggregates(
    sheets,
    drive,
    *,
    poll_id: str,
    total_votes: int,
    option_vote_counts: Optional[list[int]] = None,
) -> None:
    normalized_poll_id = str(poll_id or "").strip()
    if not normalized_poll_id:
        return

    try:
        normalized_total_votes = max(0, int(total_votes))
    except (TypeError, ValueError):
        normalized_total_votes = 0
    option_counts = _normalize_tracker_option_counts(option_vote_counts)

    tracker_spreadsheet_id, _ = get_or_create_tracker_overview_spreadsheet(sheets, drive)
    target_row = _find_tracker_row_by_poll_id(sheets, tracker_spreadsheet_id, normalized_poll_id)
    if target_row is None:
        return

    row_resp = sheets.spreadsheets().values().get(
        spreadsheetId=tracker_spreadsheet_id,
        range=f"{TRACKER_OVERVIEW_TAB}!A{target_row}:{TRACKER_OVERVIEW_END_COL}{target_row}",
    ).execute()
    existing = (row_resp.get("values") or [[]])[0]
    row_values = _normalize_tracker_row_values(existing)
    row_values[TRACKER_OVERVIEW_COL_TOTAL_VOTES] = str(normalized_total_votes)
    row_values[TRACKER_OVERVIEW_COL_OPTION1_VOTES] = str(option_counts[0])
    row_values[TRACKER_OVERVIEW_COL_OPTION2_VOTES] = str(option_counts[1])
    row_values[TRACKER_OVERVIEW_COL_OPTION3_VOTES] = str(option_counts[2])
    row_values[TRACKER_OVERVIEW_COL_OPTION4_VOTES] = str(option_counts[3])

    sheets.spreadsheets().values().update(
        spreadsheetId=tracker_spreadsheet_id,
        range=f"{TRACKER_OVERVIEW_TAB}!A{target_row}:{TRACKER_OVERVIEW_END_COL}{target_row}",
        valueInputOption="RAW",
        body={"values": [row_values]},
    ).execute()


def extract_poll_metadata(raw_text: str) -> dict[str, str]:
    parts = [p.strip() for p in re.split(r"\r?\n|\|", (raw_text or "").strip()) if p.strip()]
    aliases = {
        "title": "title",
        "session title": "title",
        "name": "title",
        "desc": "desc",
        "description": "desc",
        "blurb": "desc",
        "date": "date",
        "session date": "date",
        "venue": "venue",
        "location": "venue",
        "lunch": "lunch",
        "lunch time": "lunch",
        "session": "session",
        "session time": "session",
        "cap": "cap",
        "max": "cap",
        "max cap": "cap",
        "capacity": "cap",
        "option1": "option1",
        "option 1": "option1",
        "choice1": "option1",
        "choice 1": "option1",
        "answer1": "option1",
        "answer 1": "option1",
        "option2": "option2",
        "option 2": "option2",
        "choice2": "option2",
        "choice 2": "option2",
        "answer2": "option2",
        "answer 2": "option2",
        "option3": "option3",
        "option 3": "option3",
        "choice3": "option3",
        "choice 3": "option3",
        "answer3": "option3",
        "answer 3": "option3",
        "option4": "option4",
        "option 4": "option4",
        "choice4": "option4",
        "choice 4": "option4",
        "answer4": "option4",
        "answer 4": "option4",
        "lunch1": "lunch1",
        "lunch 1": "lunch1",
        "lunch2": "lunch2",
        "lunch 2": "lunch2",
        "lunch3": "lunch3",
        "lunch 3": "lunch3",
        "lunch4": "lunch4",
        "lunch 4": "lunch4",
    }
    out: dict[str, str] = {}
    for part in parts:
        sep = "=" if "=" in part else ":"
        if sep not in part:
            continue
        key_raw, value = part.split(sep, 1)
        key = aliases.get(key_raw.strip().lower())
        value = value.strip()
        if key and value:
            out[key] = value
    return out


def normalize_poll_date_dd_mmm_yyyy(value: str) -> tuple[str, Optional[str]]:
    raw = str(value or "").strip()
    month_abbr_to_num = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    num_to_month_abbr = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }
    if not raw:
        return "", "Missing required field: date (DD-MMM-YYYY)."

    m = re.fullmatch(r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", raw)
    if not m:
        return "", f"Invalid date format: {raw}. Use DD-MMM-YYYY (example: 22-Feb-2026)."
    day = int(m.group(1))
    month_text = m.group(2).lower()
    year = int(m.group(3))
    month_num = month_abbr_to_num.get(month_text)
    if month_num is None:
        return "", f"Invalid month in date: {raw}. Use DD-MMM-YYYY (example: 22-Feb-2026)."
    try:
        datetime(year, month_num, day)
    except ValueError:
        return "", f"Invalid calendar date: {raw}. Use a real date in DD-MMM-YYYY."
    normalized = f"{day:02d}-{num_to_month_abbr[month_num]}-{year:04d}"
    return normalized, None


def _append_or_override_date_in_raw_body(raw_body: str, normalized_date: str) -> str:
    text = (raw_body or "").strip()
    if not normalized_date:
        return text
    if not text:
        return f"date={normalized_date}"
    # Append normalized date so metadata parsers (last one wins) keep canonical format.
    return f"{text}\ndate={normalized_date}"


def validate_publishpoll_required_fields(
    raw_body: str,
) -> tuple[Optional[dict[str, str]], str, str, Optional[str]]:
    poll_metadata = extract_poll_metadata(raw_body)
    poll_title = str((poll_metadata.get("title") or extract_poll_title(raw_body) or "")).strip()
    if not poll_title:
        return None, "", "", "Missing required field: title."

    normalized_date, date_error = normalize_poll_date_dd_mmm_yyyy(poll_metadata.get("date", ""))
    if date_error:
        return None, "", "", date_error

    poll_metadata["title"] = poll_title
    poll_metadata["date"] = normalized_date
    normalized_raw_body = _append_or_override_date_in_raw_body(raw_body, normalized_date)
    return poll_metadata, poll_title, normalized_raw_body, None


def build_poll_info_rows(
    *,
    file_title: str,
    created_utc: str,
    gsheet_url: str,
    poll_title: Optional[str],
    poll_metadata: Optional[dict[str, str]],
    choices: list[tuple[str, str]],
    creator_handle: Optional[str] = None,
    creator_user_id: Optional[str] = None,
    poll_id: Optional[str] = None,
    poll_status: str = "open",
    date_created: str = "",
    time_created: str = "",
    date_closed: str = "",
    time_closed: str = "",
    closed_by: str = "",
) -> list[list[str]]:
    meta = dict(poll_metadata or {})
    if poll_title and not meta.get("title"):
        meta["title"] = poll_title
    for i, (label, lunch) in enumerate(choices[:4], start=1):
        if not meta.get(f"option{i}"):
            meta[f"option{i}"] = label
        if not meta.get(f"lunch{i}"):
            meta[f"lunch{i}"] = lunch
    created_by = _format_actor_label(creator_handle, creator_user_id)
    normalized_status = (poll_status or "open").strip() or "open"

    normalized_date_created = str(date_created or "").strip()
    normalized_time_created = str(time_created or "").strip()
    if not normalized_date_created or not normalized_time_created:
        fallback_date, fallback_time = now_utc8_date_time()
        if not normalized_date_created:
            normalized_date_created = fallback_date
        if not normalized_time_created:
            normalized_time_created = fallback_time

    rows = [
        ["Key", "Value"],
        ["file_title", file_title],
        ["created_utc", created_utc],
        ["poll_id", str(poll_id or "")],
        ["poll_title", meta.get("title", "")],
        ["poll_date", meta.get("date", "")],
        ["gSheet Url", str(gsheet_url or "").strip()],
        ["poll_status", normalized_status],
        ["status", normalized_status],
        ["created_by", created_by],
        ["date_created", normalized_date_created],
        ["time_created", normalized_time_created],
        ["date_closed", str(date_closed or "").strip()],
        ["time_closed", str(time_closed or "").strip()],
        ["closed_by", str(closed_by or "").strip()],
        ["creator_handle", creator_handle or ""],
        ["creator_user_id", creator_user_id or ""],
        ["title", meta.get("title", "")],
        ["desc", meta.get("desc", "")],
        ["date", meta.get("date", "")],
        ["venue", meta.get("venue", "")],
        ["lunch", meta.get("lunch", "")],
        ["session", meta.get("session", "")],
        ["cap", meta.get("cap", "")],
        ["option1", meta.get("option1", "")],
        ["option2", meta.get("option2", "")],
        ["option3", meta.get("option3", "")],
        ["option4", meta.get("option4", "")],
        ["lunch1", meta.get("lunch1", "")],
        ["lunch2", meta.get("lunch2", "")],
        ["lunch3", meta.get("lunch3", "")],
        ["lunch4", meta.get("lunch4", "")],
    ]
    return rows


def create_new_spreadsheet(
    sheets,
    drive,
    poll_title: Optional[str] = None,
    choices: Optional[list[tuple[str, str]]] = None,
    poll_metadata: Optional[dict[str, str]] = None,
    creator_handle: Optional[str] = None,
    creator_user_id: Optional[str] = None,
    poll_id: Optional[str] = None,
    poll_status: str = "open",
) -> tuple[str, str, str]:
    choices = choices or CHOICES
    sgt = timezone(timedelta(hours=8))
    ts = datetime.now(sgt).strftime("%Y-%m-%d_%H%M%S%z")
    title = f"VoteBot_{ts}"
    suffix = _compact_sheet_title_part(poll_title or "")
    if suffix:
        title = f"{suffix}_{title}"

    body = {
        "properties": {"title": title},
        "sheets": [
            {"properties": {"title": "Names"}},
            {"properties": {"title": GROUPINGS_AUTOMATED_SHEET_TITLE}},
            {"properties": {"title": "Votes"}},
            {"properties": {"title": "Tally"}},
            {"properties": {"title": "Poll Info"}},
        ],
    }

    ss = sheets.spreadsheets().create(body=body).execute()
    spreadsheet_id = ss["spreadsheetId"]
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    date_created, time_created = now_utc8_date_time()

    # Move into folder if provided
    _ensure_file_in_drive_folder(drive, spreadsheet_id)

    share_role = _normalize_share_role(SHEET_LINK_SHARE_ROLE)
    if share_role:
        drive.permissions().create(
            fileId=spreadsheet_id,
            body={
                "type": "anyone",
                "role": share_role,
                "allowFileDiscovery": SHEET_LINK_ALLOW_DISCOVERY,
            },
            fields="id,type,role",
        ).execute()

    # Write headers
    poll_info_rows = build_poll_info_rows(
        file_title=title,
        created_utc=ts,
        gsheet_url=url,
        poll_title=poll_title,
        poll_metadata=poll_metadata,
        choices=list(choices),
        creator_handle=creator_handle,
        creator_user_id=creator_user_id,
        poll_id=poll_id,
        poll_status=poll_status,
        date_created=date_created,
        time_created=time_created,
    )
    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "RAW",
            "data": [
                {"range": "Names!A1:J1", "values": [NAMES_HEADERS]},
                {"range": "Votes!A1:H1", "values": [VOTES_HEADERS]},
                {"range": f"Poll Info!A1:B{len(poll_info_rows)}", "values": poll_info_rows},
                {
                    "range": f"Tally!A1:B{len(choices) + 1}",
                    "values": [["Option", "Count"]] + [[label, 0] for label, _ in choices],
                },
            ],
        },
    ).execute()
    write_tally_status_summary(sheets, spreadsheet_id)
    write_groupings_automated_sheet(sheets, spreadsheet_id)

    # Add dropdown validation for Names!D:D (Sign up status), starting from row 2.
    names_sheet_id = None
    groupings_sheet_id = None
    for sheet in ss.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == "Names":
            names_sheet_id = props.get("sheetId")
        if props.get("title") == GROUPINGS_AUTOMATED_SHEET_TITLE:
            groupings_sheet_id = props.get("sheetId")

    requests = []
    if names_sheet_id is not None:
        requests.extend([
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": names_sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": 3,
                        "endColumnIndex": 4,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [
                                {"userEnteredValue": option}
                                for option in SIGNUP_STATUS_OPTIONS
                            ],
                        },
                        "showCustomUi": True,
                        "strict": True,
                    },
                }
            },
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": names_sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": 6,
                        "endColumnIndex": 7,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [
                                {"userEnteredValue": option}
                                for option in PROFILE_OPTIONS
                            ],
                        },
                        "showCustomUi": True,
                        "strict": True,
                    },
                }
            },
        ])
    if groupings_sheet_id is not None:
        requests.extend([
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": groupings_sheet_id,
                        "startRowIndex": 2,
                        "endRowIndex": 200,
                        "startColumnIndex": 1,
                        "endColumnIndex": 2,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [
                                {"userEnteredValue": option}
                                for option in GROUPING_FACILITATOR_PROFILE_OPTIONS
                            ],
                        },
                        "showCustomUi": True,
                        "strict": True,
                    },
                }
            },
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": groupings_sheet_id,
                        "startRowIndex": 2,
                        "endRowIndex": 200,
                        "startColumnIndex": 2,
                        "endColumnIndex": 3,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [
                                {"userEnteredValue": option}
                                for option in GROUPING_FACILITATOR_ACTIVE_OPTIONS
                            ],
                        },
                        "showCustomUi": True,
                        "strict": True,
                    },
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": groupings_sheet_id,
                        "gridProperties": {"frozenRowCount": 2},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
        ])
    if requests:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": requests
            },
        ).execute()

    return spreadsheet_id, url, title


def append_row(sheets, spreadsheet_id: str, range_a1: str, row: list):
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


def now_utc8_date_time() -> tuple[str, str]:
    utc8 = timezone(timedelta(hours=8))
    now = datetime.now(utc8)
    return now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S")


def build_tally_status_rows() -> list[list[str]]:
    status_range = "Names!D:D"
    rows = [["Sign up status", "Count"]]
    for status_name in TALLY_STATUS_ORDER:
        if status_name == "Signed up":
            formula = (
                f'=COUNTIF({status_range},"Signed up")'
                f'+COUNTIF({status_range},"Invite sent")'
                f'+COUNTIF({status_range},"Confirmed")'
            )
        else:
            formula = f'=COUNTIF({status_range},"{status_name}")'
        rows.append([status_name, formula])
    return rows


def write_tally_status_summary(sheets, spreadsheet_id: str):
    status_rows = build_tally_status_rows()
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"Tally!D1:E{len(status_rows)}",
        valueInputOption="USER_ENTERED",
        body={"values": status_rows},
    ).execute()


def write_groupings_automated_sheet(sheets, spreadsheet_id: str):
    row1 = [
        "Click + to edit faci list",
        "Sort by A to Z",
        "Sort by Z to A",
        "",
        "This table automatically populates. Do not make edits to columns E to H; copy-paste to another sheet if you need to edit groupings.",
        "",
        "",
        "",
        "This table auto populates confirmed attendees. Do not make edits to columns I to L. Copy-paste to another sheet to edit.",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "Automatically populates based on Names sheet - do not edit",
    ]
    row2 = [
        "",
        "Facilitator profile",
        "Activated as facil?",
        "Facilitator name",
        "Newcomer / Enhanced facilitation",
        "Male",
        "Female",
        "Spillover",
        "Newcomer / Enhanced facilitation",
        "Male + spillover",
        "Female + spillover",
        "Spillover",
        "",
        "No. of confirmed attendees not grouped (should show 0 if all confirmed attendees have been grouped)",
        "may show negative if faci has yet to confirm attendance",
        "",
        "",
        "Conditional formatting helper",
    ]

    default_rows = [["", profile, active] for profile, active in GROUPING_FACILITATOR_DEFAULT_ROWS]
    # N5:N7 helper labels to match the legacy sheet layout.
    default_rows[2].extend([""] * 10 + ["Male"])
    default_rows[3].extend([""] * 10 + ["Female"])
    default_rows[4].extend([""] * 10 + ["Newcomer"])
    for idx in range(len(default_rows)):
        if idx not in (2, 3, 4):
            default_rows[idx].extend([""] * 11)

    core_formula_row = [[
        '=IFERROR(FILTER(Names!B:B,Names!G:G="Facilitator",Names!D:D="Confirmed"),"")',
        '=IFERROR(ARRAY_CONSTRAIN({FILTER(Names!B:B,Names!D:D="Confirmed",Names!G:G="Newcomer");FILTER(Names!B:B,Names!D:D="Confirmed",Names!G:G="Enhanced Facilitation")},COUNTIF(C:C,"Yes"),1),"")',
        '=IFERROR(ARRAY_CONSTRAIN(FILTER(Names!B:B,Names!D:D="Confirmed",Names!G:G="",REGEXMATCH(TO_TEXT(Names!H:H),"(?i)^(m|male)$")),COUNTA(D3:D1502),1),"#REF!")',
        '=IFERROR(ARRAY_CONSTRAIN(FILTER(Names!B:B,Names!D:D="Confirmed",Names!G:G="",REGEXMATCH(TO_TEXT(Names!H:H),"(?i)^(f|female)$")),COUNTA(D3:D1502),1),"#REF!")',
        '=IFERROR(UNIQUE({E3:E1001;F3:F1001;G3:G1001;FILTER(Names!B:B,Names!D:D="Confirmed",Names!G:G<>"Facilitator")},,TRUE),"null")',
        '=IFNA(E3:E1001,"")',
        '=UNIQUE({F3:F1001;H3:H1001},,TRUE)',
        '=UNIQUE({G3:G1001;H3:H1001;F3:F1001;J3:J1001},,TRUE)',
        '=UNIQUE({G3:G1001;H3:H1001;F3:F1001;J3:J1001;K3:K1001},,TRUE)',
        "",
        '=Tally!E4-SUBTOTAL(103,D3:H1502)+COUNTIF(E:H,"null")+COUNTIF(C:C,"No")',
    ]]

    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": f"'{GROUPINGS_AUTOMATED_SHEET_TITLE}'!A1:R1", "values": [row1]},
                {"range": f"'{GROUPINGS_AUTOMATED_SHEET_TITLE}'!A2:R2", "values": [row2]},
                {"range": f"'{GROUPINGS_AUTOMATED_SHEET_TITLE}'!A3:N11", "values": default_rows},
                {"range": f"'{GROUPINGS_AUTOMATED_SHEET_TITLE}'!D3:N3", "values": core_formula_row},
            ],
        },
    ).execute()


def update_tally(sheets, spreadsheet_id: str, choices: list[tuple[str, str]], counts: list[int]):
    tally_rows = []
    for i, (label, _) in enumerate(list(choices or CHOICES)):
        try:
            count = int(counts[i])
        except (IndexError, TypeError, ValueError):
            count = 0
        tally_rows.append([label, count])

    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"Tally!A2:B{len(tally_rows) + 1}",
        valueInputOption="RAW",
        body={"values": tally_rows},
    ).execute()
    write_tally_status_summary(sheets, spreadsheet_id)


POLL_INFO_KEY_ROWS = {
    "poll_id": 6,
    "status": 7,
    "poll_status": 7,
    "date_closed": 0,
    "time_closed": 0,
    "closed_by": 0,
}


def _load_poll_info_key_rows(sheets, spreadsheet_id: str) -> dict[str, int]:
    key_rows: dict[str, int] = {}
    try:
        resp = sheets.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="Poll Info!A1:A300",
        ).execute()
    except Exception:
        return key_rows

    for idx, row in enumerate(resp.get("values", []), start=1):
        key = str(row[0]).strip() if row else ""
        if key:
            key_rows[key] = idx
    return key_rows


def update_poll_info_fields(sheets, spreadsheet_id: str, **fields: str):
    key_rows = _load_poll_info_key_rows(sheets, spreadsheet_id)
    data = []
    for key, value in fields.items():
        key_text = str(key)
        row_num = key_rows.get(key_text) or POLL_INFO_KEY_ROWS.get(key_text)
        if not row_num:
            continue
        data.append(
            {
                "range": f"Poll Info!B{row_num}:B{row_num}",
                "values": [[str(value or "")]],
            }
        )
    if not data:
        return

    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "RAW",
            "data": data,
        },
    ).execute()


def _normalize_handle(value: str, *, allow_missing_at: bool = False) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    if not v.startswith("@"):
        if not allow_missing_at:
            return ""
        v = "@" + v
    if not re.fullmatch(r"@[A-Za-z0-9_]{2,}", v):
        return ""
    return v.lower()


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).casefold()


def _normalize_gender(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    low = v.casefold()
    if low in {"m", "male"}:
        return "Male"
    if low in {"f", "female"}:
        return "Female"
    return v


def _empty_member_index(source_label: str = "") -> dict:
    return {
        "enabled": False,
        "source": source_label,
        "handles": set(),
        "names": {},  # normalized_name -> set(handles)
        "genders": {},  # normalized_handle -> gender
    }


def _build_member_index_from_rows(rows, source_label: str) -> dict:
    index = _empty_member_index(source_label)
    for row in rows:
        cells = [(c or "").strip() for c in row]
        if len(cells) < 3:
            continue

        serial = cells[1] if len(cells) > 1 else ""
        if not serial.isdigit():
            continue

        name = cells[2] if len(cells) > 2 else ""
        name_norm = _normalize_name(name)
        if name_norm in {"", "null"}:
            name_norm = ""

        row_handles = {_normalize_handle(c) for c in cells if _normalize_handle(c)}
        row_handles.discard("@0")
        row_gender = ""
        for c in cells:
            g = _normalize_gender(c)
            if g in {"Male", "Female"}:
                row_gender = g
                break
        if not row_handles and not name_norm:
            continue

        index["handles"].update(row_handles)
        if row_gender:
            for h in row_handles:
                index["genders"][h] = row_gender
        if name_norm:
            name_set = index["names"].setdefault(name_norm, set())
            name_set.update(row_handles)

    index["enabled"] = bool(index["handles"] or index["names"])
    return index


def _extract_spreadsheet_id(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", raw)
    return m.group(1) if m else raw


def load_member_check_index(csv_path: str) -> dict:
    index = _empty_member_index(csv_path)
    if not csv_path or not os.path.exists(csv_path):
        return index

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        return _build_member_index_from_rows(list(reader), csv_path)


def load_member_check_index_from_sheet(sheets, source: str, tab_name: str) -> dict:
    spreadsheet_id = _extract_spreadsheet_id(source)
    source_label = f"{spreadsheet_id}:{tab_name}" if spreadsheet_id else source
    index = _empty_member_index(source_label)
    if not spreadsheet_id:
        return index

    rows = (
        sheets.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"'{tab_name}'!A:Z")
        .execute()
        .get("values", [])
    )
    return _build_member_index_from_rows(rows, source_label)


def _load_sheet_column_values(sheets, spreadsheet_id: str, a1_range: str) -> list[str]:
    rows = (
        sheets.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=a1_range)
        .execute()
        .get("values", [])
    )
    out = []
    for row in rows:
        if not row:
            continue
        val = (row[0] or "").strip()
        if val:
            out.append(val)
    return out


def load_member_check_index_from_raw_source(sheets, source: str) -> dict:
    spreadsheet_id = _extract_spreadsheet_id(source)
    source_label = f"{spreadsheet_id}:raw-vstack" if spreadsheet_id else source
    index = _empty_member_index(source_label)
    if not spreadsheet_id:
        return index

    existing_handles = _load_sheet_column_values(sheets, spreadsheet_id, MEMBER_RAW_EXISTING_HANDLE_RANGE)
    existing_genders = _load_sheet_column_values(sheets, spreadsheet_id, MEMBER_RAW_EXISTING_GENDER_RANGE)
    newcomer_handles = _load_sheet_column_values(sheets, spreadsheet_id, MEMBER_RAW_NEWCOMER_HANDLE_RANGE)
    newcomer_genders = _load_sheet_column_values(sheets, spreadsheet_id, MEMBER_RAW_NEWCOMER_GENDER_RANGE)

    all_handles = [*existing_handles, *newcomer_handles]
    all_genders = [*existing_genders, *newcomer_genders]

    for handle_val, gender_val in zip_longest(all_handles, all_genders, fillvalue=""):
        h = _normalize_handle(handle_val)
        if not h:
            continue
        index["handles"].add(h)
        g = _normalize_gender(gender_val)
        if g:
            index["genders"][h] = g

    index["enabled"] = bool(index["handles"])
    return index


def classify_newcomer(member_index: dict, username: str, full_name: str) -> str:
    if not member_index.get("enabled"):
        return ""

    handle_norm = _normalize_handle(username, allow_missing_at=True)
    name_norm = _normalize_name(full_name)

    if handle_norm and handle_norm in member_index["handles"]:
        return "No"

    if name_norm and name_norm in member_index["names"]:
        # Same name but handle changed/missing; flag for manual check.
        return "Review"

    return "Yes"


def lookup_member_gender(member_index: dict, username: str) -> str:
    if not member_index.get("enabled"):
        return ""
    handle_norm = _normalize_handle(username, allow_missing_at=True)
    if not handle_norm:
        return ""
    return member_index.get("genders", {}).get(handle_norm, "")


def upsert_name_result(
    sheets,
    spreadsheet_id: str,
    poll_state: dict,
    *,
    user_id: int,
    username_link: str,
    full_name: str,
    handle: str,
    status: str,
    selected_option: str,
    lunch: str,
    gender: str,
    newcomer: str,
):
    row_num = poll_state["names_row_by_user"].get(user_id)
    if row_num is None:
        row_num = poll_state["next_names_row"]
        poll_state["names_row_by_user"][user_id] = row_num
        poll_state["next_names_row"] += 1

    # Update A:F, I, J and optionally H (gender). G is preserved for manual profile edits.
    data = [
        {
            "range": f"Names!A{row_num}:F{row_num}",
            "values": [[username_link, full_name, handle, status, selected_option, lunch]],
        },
        {
            "range": f"Names!I{row_num}:I{row_num}",
            "values": [[newcomer]],
        },
        {
            "range": f"Names!J{row_num}:J{row_num}",
            "values": [[""]],
        },
    ]
    if gender:
        data.insert(
            1,
            {
                "range": f"Names!H{row_num}:H{row_num}",
                "values": [[gender]],
            },
        )

    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "RAW",
            "data": data,
        },
    ).execute()


# --------------------
# Telegram bot
# --------------------
POLL_STATES = {}  # ("native", poll_id) -> state
PENDING_PUBLISH_PREVIEWS = {}  # token -> {raw_body, chat_id, user_id, created_ts}
PENDING_STOPPOLL_CONFIRMATIONS = {}  # token -> {poll_id, chat_id, user_id, created_ts}
PENDING_STOPPOLL_PICKERS = {}  # token -> {poll_ids, chat_id, user_id, created_ts}
MEMBER_INDEX = {"enabled": False, "source": "", "handles": set(), "names": {}}
MEMBER_INDEX_LAST_REFRESH_TS = 0.0
SHEETS = None
DRIVE = None
TRACKER_OVERVIEW_SPREADSHEET_ID = ""
PUBLISH_PREVIEW_TTL_SECONDS = 15 * 60


def _prune_pending_publish_previews() -> None:
    now = time.time()
    expired = []
    for token, item in PENDING_PUBLISH_PREVIEWS.items():
        try:
            created_ts = float(item.get("created_ts", 0))
        except (TypeError, ValueError, AttributeError):
            created_ts = 0.0
        if created_ts <= 0 or (now - created_ts) > PUBLISH_PREVIEW_TTL_SECONDS:
            expired.append(token)
    for token in expired:
        PENDING_PUBLISH_PREVIEWS.pop(token, None)


def _prune_pending_stoppoll_confirmations() -> None:
    now = time.time()
    expired = []
    for token, item in PENDING_STOPPOLL_CONFIRMATIONS.items():
        try:
            created_ts = float(item.get("created_ts", 0))
        except (TypeError, ValueError, AttributeError):
            created_ts = 0.0
        if created_ts <= 0 or (now - created_ts) > PUBLISH_PREVIEW_TTL_SECONDS:
            expired.append(token)
    for token in expired:
        PENDING_STOPPOLL_CONFIRMATIONS.pop(token, None)


def _prune_pending_stoppoll_pickers() -> None:
    now = time.time()
    expired = []
    for token, item in PENDING_STOPPOLL_PICKERS.items():
        try:
            created_ts = float(item.get("created_ts", 0))
        except (TypeError, ValueError, AttributeError):
            created_ts = 0.0
        if created_ts <= 0 or (now - created_ts) > PUBLISH_PREVIEW_TTL_SECONDS:
            expired.append(token)
    for token in expired:
        PENDING_STOPPOLL_PICKERS.pop(token, None)


def _serialize_poll_state(state: dict) -> dict:
    choices = list(state.get("choices") or CHOICES)
    choice_count = max(2, len(choices))
    raw_counts = list(state.get("counts", []))
    counts = []
    for i in range(choice_count):
        try:
            counts.append(int(raw_counts[i]))
        except (IndexError, TypeError, ValueError):
            counts.append(0)
    return {
        "poll_title": str(state.get("poll_title", "")),
        "spreadsheet_id": str(state.get("spreadsheet_id", "")),
        "spreadsheet_url": str(state.get("spreadsheet_url", "")),
        "spreadsheet_title": str(state.get("spreadsheet_title", "")),
        "choices": [[str(label), str(lunch)] for label, lunch in choices],
        "votes": {str(k): int(v) for k, v in state.get("votes", {}).items()},
        "counts": counts,
        "names_row_by_user": {str(k): int(v) for k, v in state.get("names_row_by_user", {}).items()},
        "next_names_row": int(state.get("next_names_row", 2)),
        "chat_id": str(state.get("chat_id", "")),
        "message_id": str(state.get("message_id", "")),
        "cap": int(state.get("cap", 0) or 0),
        "closed": bool(state.get("closed", False)),
    }


def _deserialize_poll_state(raw: dict) -> Optional[dict]:
    if not isinstance(raw, dict):
        return None
    poll_title = str(raw.get("poll_title", "")).strip()
    spreadsheet_id = str(raw.get("spreadsheet_id", "")).strip()
    spreadsheet_url = str(raw.get("spreadsheet_url", "")).strip()
    spreadsheet_title = str(raw.get("spreadsheet_title", "")).strip()
    if not spreadsheet_id:
        return None

    raw_choices = raw.get("choices") or CHOICES
    choices = []
    for item in list(raw_choices)[:4]:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            choices.append((str(item[0]), str(item[1])))
    if len(choices) < 2:
        choices = list(CHOICES)
    else:
        choices = choices[:4]

    votes = {}
    for k, v in (raw.get("votes") or {}).items():
        try:
            votes[int(k)] = int(v)
        except (TypeError, ValueError):
            continue

    names_row_by_user = {}
    for k, v in (raw.get("names_row_by_user") or {}).items():
        try:
            names_row_by_user[int(k)] = int(v)
        except (TypeError, ValueError):
            continue

    raw_counts = list(raw.get("counts") or [])
    counts = []
    for i in range(len(choices)):
        try:
            counts.append(int(raw_counts[i]))
        except (IndexError, TypeError, ValueError):
            counts.append(0)

    try:
        next_names_row = int(raw.get("next_names_row", 2))
    except (TypeError, ValueError):
        next_names_row = 2

    try:
        cap = int(raw.get("cap", 0) or 0)
    except (TypeError, ValueError):
        cap = 0

    return {
        "poll_title": poll_title,
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_url": spreadsheet_url,
        "spreadsheet_title": spreadsheet_title,
        "choices": choices,
        "votes": votes,
        "counts": counts,
        "names_row_by_user": names_row_by_user,
        "next_names_row": max(2, next_names_row),
        "chat_id": str(raw.get("chat_id", "")),
        "message_id": str(raw.get("message_id", "")),
        "cap": cap,
        "closed": bool(raw.get("closed", False)),
    }


def save_native_poll_states() -> None:
    payload = {"native_polls": {}}
    for key, state in POLL_STATES.items():
        if not isinstance(key, tuple) or len(key) != 2 or key[0] != "native":
            continue
        payload["native_polls"][str(key[1])] = _serialize_poll_state(state)

    with open(NATIVE_POLL_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2, sort_keys=True)


def load_native_poll_states() -> int:
    if not NATIVE_POLL_STATE_FILE or not os.path.exists(NATIVE_POLL_STATE_FILE):
        return 0
    try:
        with open(NATIVE_POLL_STATE_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        print("Native poll state load failed:", e)
        return 0

    native_polls = payload.get("native_polls", {})
    if not isinstance(native_polls, dict):
        return 0

    restored = 0
    for poll_id, raw_state in native_polls.items():
        state = _deserialize_poll_state(raw_state)
        if not state:
            continue
        POLL_STATES[("native", str(poll_id))] = state
        restored += 1
    return restored


def create_poll_state(
    poll_key,
    poll_title: Optional[str] = None,
    choices: Optional[list[tuple[str, str]]] = None,
    cap: int = 0,
    poll_metadata: Optional[dict[str, str]] = None,
    creator_handle: Optional[str] = None,
    creator_user_id: Optional[str] = None,
):
    poll_choices = list(choices or CHOICES)
    native_poll_id = ""
    if isinstance(poll_key, tuple) and len(poll_key) == 2 and poll_key[0] == "native":
        native_poll_id = str(poll_key[1])
    spreadsheet_id, spreadsheet_url, spreadsheet_title = create_new_spreadsheet(
        SHEETS,
        DRIVE,
        poll_title=poll_title,
        choices=poll_choices,
        poll_metadata=poll_metadata,
        creator_handle=creator_handle,
        creator_user_id=creator_user_id,
        poll_id=native_poll_id,
        poll_status="open",
    )
    poll_date = ""
    if isinstance(poll_metadata, dict):
        poll_date = str(poll_metadata.get("date", "") or "").strip()
    created_by = _format_actor_label(creator_handle, creator_user_id)
    date_created, time_created = now_utc8_date_time()
    try:
        upsert_tracker_overview_row(
            SHEETS,
            DRIVE,
            poll_id=native_poll_id,
            poll_title=str(poll_title or "").strip(),
            poll_date=poll_date,
            gsheet_url=spreadsheet_url,
            poll_status="open",
            created_by=created_by,
            date_created=date_created,
            time_created=time_created,
            total_votes=0,
            option_vote_counts=[0, 0, 0, 0],
        )
    except Exception as e:
        print("Tracker overview upsert failed for create_poll_state:", e)
    print("Spreadsheet created:", spreadsheet_url, "for", poll_key)
    return {
        "poll_title": str(poll_title or ""),
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_url": spreadsheet_url,
        "spreadsheet_title": spreadsheet_title,
        "choices": poll_choices,
        "votes": {},               # user_id -> choice_idx
        "counts": [0] * len(poll_choices),
        "names_row_by_user": {},   # user_id -> row number
        "next_names_row": 2,
        "cap": int(cap or 0),
        "closed": False,
    }


def extract_poll_title(query_text: str) -> Optional[str]:
    raw = (query_text or "").strip()
    if not raw:
        return None

    parts = [p.strip() for p in re.split(r"\r?\n|\|", raw) if p.strip()]
    for part in parts:
        sep = "=" if "=" in part else ":"
        if sep not in part:
            continue
        key_raw, value = part.split(sep, 1)
        key = key_raw.strip().lower()
        if key in {"title", "session title", "name"}:
            value = value.strip()
            return value or None
    return None


def extract_native_poll_options(raw_text: str) -> list[str]:
    return [label for label, _ in extract_native_poll_choices(raw_text)]


def extract_native_poll_choices(raw_text: str) -> list[tuple[str, str]]:
    parts = [p.strip() for p in re.split(r"\r?\n|\|", (raw_text or "").strip()) if p.strip()]
    parsed = {}
    aliases = {
        "option1": "option1",
        "option 1": "option1",
        "choice1": "option1",
        "choice 1": "option1",
        "answer1": "option1",
        "answer 1": "option1",
        "option2": "option2",
        "option 2": "option2",
        "choice2": "option2",
        "choice 2": "option2",
        "answer2": "option2",
        "answer 2": "option2",
        "option3": "option3",
        "option 3": "option3",
        "choice3": "option3",
        "choice 3": "option3",
        "answer3": "option3",
        "answer 3": "option3",
        "option4": "option4",
        "option 4": "option4",
        "choice4": "option4",
        "choice 4": "option4",
        "answer4": "option4",
        "answer 4": "option4",
        "lunch1": "lunch1",
        "lunch 1": "lunch1",
        "lunch2": "lunch2",
        "lunch 2": "lunch2",
        "lunch3": "lunch3",
        "lunch 3": "lunch3",
        "lunch4": "lunch4",
        "lunch 4": "lunch4",
    }
    for part in parts:
        sep = "=" if "=" in part else ":"
        if sep not in part:
            continue
        key_raw, value = part.split(sep, 1)
        key = aliases.get(key_raw.strip().lower())
        value = value.strip()
        if key and value:
            parsed[key] = value
    out = [
        (parsed.get("option1", CHOICES[0][0]), parsed.get("lunch1", CHOICES[0][1])),
        (parsed.get("option2", CHOICES[1][0]), parsed.get("lunch2", CHOICES[1][1])),
    ]
    if parsed.get("option3"):
        out.append((parsed["option3"], parsed.get("lunch3", "")))
    if parsed.get("option4"):
        out.append((parsed["option4"], parsed.get("lunch4", "")))
    return out[:4]


def extract_poll_cap(raw_text: str) -> int:
    parts = [p.strip() for p in re.split(r"\r?\n|\|", (raw_text or "").strip()) if p.strip()]
    aliases = {"cap", "max", "max cap", "capacity"}
    for part in parts:
        sep = "=" if "=" in part else ":"
        if sep not in part:
            continue
        key_raw, value = part.split(sep, 1)
        if key_raw.strip().lower() not in aliases:
            continue
        try:
            cap = int(value.strip())
        except ValueError:
            return 0
        return max(0, cap)
    return 0


def strip_prompt_line(poll_prompt: str, parse_mode: Optional[str]) -> str:
    text = (poll_prompt or "").strip()
    if not text:
        return ""
    if parse_mode == "HTML":
        text = re.sub(r"\n?<b>Please vote:</b>\s*$", "", text, flags=re.IGNORECASE)
    else:
        text = re.sub(r"\n?Please vote:\s*$", "", text, flags=re.IGNORECASE)
    return text.strip()


def extract_command_body(message_text: str, command: str) -> str:
    text = message_text or ""
    m = re.match(rf"^/{re.escape(command)}(?:@\w+)?\s*", text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return text[m.end():]


def build_poll_prompt(query_text: str) -> tuple[str, Optional[str]]:
    raw = (query_text or "").strip()
    if not raw:
        return "Please vote:", None

    # Allow simple inline formatting like:
    # @Bot title=ABC<newline>date=01/03/2026
    # (also supports "|" as a fallback separator)
    parts = [p.strip() for p in re.split(r"\r?\n|\|", raw) if p.strip()]
    if not parts:
        return "Please vote:", None

    aliases = {
        "title": "title",
        "session title": "title",
        "name": "title",
        "date": "date",
        "session date": "date",
        "venue": "venue",
        "location": "venue",
        "lunch": "lunch",
        "lunch time": "lunch",
        "session": "session",
        "session time": "session",
        "desc": "desc",
        "description": "desc",
        "blurb": "desc",
        "cap": "cap",
        "max": "cap",
        "max cap": "cap",
        "capacity": "cap",
        "option1": "option1",
        "option 1": "option1",
        "choice1": "option1",
        "choice 1": "option1",
        "answer1": "option1",
        "answer 1": "option1",
        "option2": "option2",
        "option 2": "option2",
        "choice2": "option2",
        "choice 2": "option2",
        "answer2": "option2",
        "answer 2": "option2",
        "option3": "option3",
        "option 3": "option3",
        "choice3": "option3",
        "choice 3": "option3",
        "answer3": "option3",
        "answer 3": "option3",
        "option4": "option4",
        "option 4": "option4",
        "choice4": "option4",
        "choice 4": "option4",
        "answer4": "option4",
        "answer 4": "option4",
        "lunch1": "lunch1",
        "lunch 1": "lunch1",
        "lunch2": "lunch2",
        "lunch 2": "lunch2",
        "lunch3": "lunch3",
        "lunch 3": "lunch3",
        "lunch4": "lunch4",
        "lunch 4": "lunch4",
    }
    fields: dict[str, str] = {}
    free_lines: list[str] = []

    for part in parts:
        sep = "=" if "=" in part else ":"
        if sep not in part:
            free_lines.append(part)
            continue
        key_raw, value = part.split(sep, 1)
        key = key_raw.strip().lower()
        value = value.strip()
        field_name = aliases.get(key)
        if not field_name or not value:
            free_lines.append(part)
            continue
        fields[field_name] = value

    # Structured event-style message if known fields were provided.
    if fields:
        lines = []
        if fields.get("title"):
            lines.append(f"<b>{html.escape(fields['title'])}</b>")
        if fields.get("desc"):
            if lines:
                lines.append("")
            lines.append(html.escape(fields["desc"]))

        info_lines = []
        if fields.get("date"):
            info_lines.append(f"📅 {html.escape(fields['date'])}")
        if fields.get("venue"):
            info_lines.append(f"📍 {html.escape(fields['venue'])}")
        if fields.get("lunch"):
            info_lines.append(f"🍽️ Lunch: {html.escape(fields['lunch'])}")
        if fields.get("session"):
            info_lines.append(f"🧘 Session: {html.escape(fields['session'])}")
        if fields.get("cap"):
            info_lines.append(f"👥 Cap: {html.escape(fields['cap'])}")
        if info_lines:
            if lines:
                lines.append("")
            lines.extend(info_lines)

        if free_lines:
            if lines:
                lines.append("")
            lines.extend(html.escape(x) for x in free_lines)

        if lines:
            lines.append("")
        lines.append("<b>Please vote:</b>")
        return "\n".join(lines), "HTML"

    return "Please vote:\n" + "\n".join(parts), None


def publishpoll_preview_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Publish", callback_data=f"ppc|{token}|ok"),
            InlineKeyboardButton("Cancel", callback_data=f"ppc|{token}|cancel"),
        ]
    ])


def stoppoll_confirmation_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Yes", callback_data=f"spc|{token}|ok"),
            InlineKeyboardButton("No", callback_data=f"spc|{token}|cancel"),
        ]
    ])


def stoppoll_picker_keyboard(token: str, poll_items: list[tuple[str, dict]]) -> InlineKeyboardMarkup:
    rows = []
    for idx, (poll_id, state) in enumerate(poll_items):
        poll_title = str((state or {}).get("poll_title", "") or "").strip() or f"poll_id={poll_id}"
        label = poll_title if len(poll_title) <= 60 else poll_title[:59].rstrip() + "…"
        rows.append([InlineKeyboardButton(label, callback_data=f"sps|{token}|{idx}")])
    rows.append([InlineKeyboardButton("Cancel", callback_data=f"sps|{token}|cancel")])
    return InlineKeyboardMarkup(rows)


def _preview_plain_text(value: str, parse_mode: Optional[str]) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    if parse_mode == "HTML":
        text = re.sub(r"<[^>]+>", "", text)
        text = html.unescape(text)
    return text.strip()


def _condense_poll_question(raw_body: str, max_len: int = 300) -> str:
    poll_prompt, parse_mode = build_poll_prompt(raw_body)
    context_text = strip_prompt_line(poll_prompt, parse_mode)
    plain = _preview_plain_text(context_text, parse_mode)

    if not plain:
        return (extract_poll_title(raw_body) or "Please vote").strip()[:max_len]

    parts = [line.strip() for line in plain.splitlines() if line.strip()]
    if not parts:
        return (extract_poll_title(raw_body) or "Please vote").strip()[:max_len]

    question = "\n".join(parts)
    if len(question) <= max_len:
        return question

    # Keep title first, then append as much detail as fits.
    title = parts[0]
    if len(title) >= max_len:
        return title[: max_len - 1].rstrip() + "…"

    out = title
    for part in parts[1:]:
        candidate = f"{out}\n{part}"
        if len(candidate) > max_len:
            break
        out = candidate

    if out == title and len(question) > max_len:
        return title[: max_len - 1].rstrip() + "…"
    return out


async def _send_native_poll_and_track(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id,
    raw_body: str,
    actor_user,
):
    poll_question = _condense_poll_question(raw_body)
    poll_metadata = extract_poll_metadata(raw_body)
    normalized_date, date_error = normalize_poll_date_dd_mmm_yyyy(poll_metadata.get("date", ""))
    if date_error:
        raise ValueError(date_error)
    poll_metadata["date"] = normalized_date
    poll_choices = extract_native_poll_choices(raw_body)
    poll_cap = extract_poll_cap(raw_body)
    spreadsheet_title = (poll_metadata.get("title") or extract_poll_title(raw_body) or poll_question).strip()
    creator_handle = f"@{actor_user.username}" if actor_user and getattr(actor_user, "username", None) else ""
    creator_user_id = str(actor_user.id) if actor_user else ""
    poll_options = [label for label, _ in poll_choices]

    poll_msg = await context.bot.send_poll(
        chat_id=chat_id,
        question=poll_question[:300],
        options=poll_options,
        is_anonymous=False,
        allows_multiple_answers=False,
    )

    native_poll = getattr(poll_msg, "poll", None)
    if not (native_poll and native_poll.id):
        return None

    poll_key = ("native", native_poll.id)
    loop = asyncio.get_running_loop()
    poll_state = await loop.run_in_executor(
        None,
        lambda: create_poll_state(
            poll_key,
            poll_title=spreadsheet_title,
            choices=poll_choices,
            cap=poll_cap,
            poll_metadata=poll_metadata,
            creator_handle=creator_handle,
            creator_user_id=creator_user_id,
        ),
    )
    poll_state["chat_id"] = str(poll_msg.chat_id)
    poll_state["message_id"] = str(poll_msg.message_id)
    POLL_STATES[poll_key] = poll_state
    save_native_poll_states()
    confirmation_lines = [
        f"poll_id: {native_poll.id}",
        f"title={spreadsheet_title}",
        f"Tracking sheet (internal circulation): {poll_state['spreadsheet_url']}",
    ]
    if DRIVE_FOLDER_ID:
        folder_url = DRIVE_FOLDER_ID
        if not re.match(r"^https?://", folder_url, flags=re.IGNORECASE):
            folder_url = f"https://drive.google.com/drive/folders/{folder_url}"
        confirmation_lines.append(f"Drive folder: {folder_url}")
    await context.bot.send_message(
        chat_id=chat_id,
        text="\n".join(confirmation_lines),
    )
    return poll_state


def _extract_update_user_id(update: Update) -> Optional[int]:
    effective_user = getattr(update, "effective_user", None)
    if effective_user and getattr(effective_user, "id", None) is not None:
        return int(effective_user.id)

    answer = getattr(update, "poll_answer", None)
    answer_user = getattr(answer, "user", None) if answer else None
    if answer_user and getattr(answer_user, "id", None) is not None:
        return int(answer_user.id)
    return None


async def _ensure_allowed_user(update: Update) -> bool:
    if not ALLOWED_TELEGRAM_USER_IDS:
        return True

    user_id = _extract_update_user_id(update)
    if user_id is not None and user_id in ALLOWED_TELEGRAM_USER_IDS:
        return True

    query = getattr(update, "callback_query", None)
    if query:
        try:
            await query.answer("You are not authorized to use this bot.", show_alert=True)
        except Exception:
            pass
        return False

    msg = getattr(update, "message", None)
    if msg:
        try:
            await msg.reply_text("You are not authorized to use this bot.")
        except Exception:
            pass
        return False

    if user_id is None:
        print("Blocked update without user id while allowlist is enabled.")
    else:
        print("Blocked update from unauthorized Telegram user id:", user_id)
    return False


def with_allowed_user_check(
    handler: Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[Any]]
) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[Any]]:
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await _ensure_allowed_user(update):
            return
        return await handler(update, context)

    wrapped.__name__ = f"{handler.__name__}_allowlist"
    return wrapped


async def startall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Ready.\n"
        "Use /publishpoll to preview then send a native Telegram poll.\n"
        "Use /sample to get a copy-paste template for /publishpoll.\n"
        "Use /metadata to open the metadata spreadsheet.\n"
        "Use /pollstatus [poll_id ...] to check tracked/open/closed status.\n"
        "Use /stoppoll <poll_id> to close a poll and stop tracking it.\n"
        "A new spreadsheet is created per poll published."
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Ready.\n"
        "Use /sample to get a copy-paste template for publishpoll.\n"
        "Use /publishpoll to preview then send a Telegram poll.\n"
        "Use /metadata to open the metadata spreadsheet.\n"
        "Use /pollstatus to check tracked/open/closed status for all polls.\n"
        "Use /stoppoll to select and close a poll\n"
        "A new spreadsheet is created per poll published."
    )

async def sample(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    await msg.reply_text(PUBLISHPOLL_SAMPLE_GUIDE)


async def metadata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    loop = asyncio.get_running_loop()
    try:
        tracker_url = await loop.run_in_executor(
            None,
            lambda: get_or_create_tracker_overview_spreadsheet(SHEETS, DRIVE)[1],
        )
    except Exception as e:
        print("Metadata command failed:", e)
        await msg.reply_text("Metadata sheet unavailable right now. Please try again.")
        return

    lines = [
        "Metadata sheet:",
        str(tracker_url or "").strip(),
    ]
    if DRIVE_FOLDER_ID:
        folder_url = DRIVE_FOLDER_ID
        if not re.match(r"^https?://", folder_url, flags=re.IGNORECASE):
            folder_url = f"https://drive.google.com/drive/folders/{folder_url}"
        lines.extend(["", f"Drive folder: {folder_url}"])
    await msg.reply_text("\n".join(lines))


async def activesheets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    native_items = []
    for key, state in POLL_STATES.items():
        if isinstance(key, tuple) and len(key) == 2 and key[0] == "native":
            native_items.append((str(key[1]), state))

    if not native_items:
        await msg.reply_text("No tracked native poll sheets found.")
        return

    def _sort_key(item):
        poll_id, state = item
        try:
            message_id = int(state.get("message_id", 0))
        except (TypeError, ValueError):
            message_id = 0
        return (message_id, poll_id)

    native_items.sort(key=_sort_key, reverse=True)

    missing_title_ids = []
    for _, state in native_items:
        if state.get("spreadsheet_title"):
            continue
        spreadsheet_id = str(state.get("spreadsheet_id", "") or "").strip()
        if spreadsheet_id:
            missing_title_ids.append(spreadsheet_id)

    if missing_title_ids:
        loop = asyncio.get_running_loop()

        def _fetch_titles(ids: list[str]) -> dict[str, str]:
            out: dict[str, str] = {}
            seen: set[str] = set()
            for spreadsheet_id in ids:
                if spreadsheet_id in seen:
                    continue
                seen.add(spreadsheet_id)
                try:
                    meta = DRIVE.files().get(fileId=spreadsheet_id, fields="name").execute()
                    name = str(meta.get("name", "") or "").strip()
                    if name:
                        out[spreadsheet_id] = name
                except Exception as e:
                    print("Sheet title lookup failed for", spreadsheet_id, ":", e)
            return out

        fetched_titles = await loop.run_in_executor(None, lambda: _fetch_titles(missing_title_ids))
        if fetched_titles:
            updated = False
            for _, state in native_items:
                spreadsheet_id = str(state.get("spreadsheet_id", "") or "").strip()
                title = fetched_titles.get(spreadsheet_id, "")
                if title and not state.get("spreadsheet_title"):
                    state["spreadsheet_title"] = title
                    updated = True
            if updated:
                save_native_poll_states()

    lines = [f"Tracked poll sheets: {len(native_items)}"]
    if DRIVE_FOLDER_ID:
        folder_url = DRIVE_FOLDER_ID
        if not re.match(r"^https?://", folder_url, flags=re.IGNORECASE):
            folder_url = f"https://drive.google.com/drive/folders/{folder_url}"
        lines.append(f"Drive folder: {folder_url}")
    for poll_id, state in native_items:
        choices = list(state.get("choices") or CHOICES)
        counts = list(state.get("counts") or [])
        votes = state.get("votes") or {}
        total_votes = len(votes)
        cap = int(state.get("cap", 0) or 0)
        status = "closed" if state.get("closed") else "open"
        if cap > 0:
            status += f" (cap={cap})"

        count_parts = []
        for i, (label, _) in enumerate(choices):
            try:
                count_val = int(counts[i])
            except (IndexError, TypeError, ValueError):
                count_val = 0
            count_parts.append(f"Option {i+1}:{label}={count_val}")

        lines.append("")
        lines.append(f"poll_id={poll_id}")
        lines.append(f"status={status}")
        spreadsheet_title = str(state.get("spreadsheet_title", "") or "").strip()
        if not spreadsheet_title:
            spreadsheet_title = f"(sheet id: {state.get('spreadsheet_id', '-')})"
        lines.append(f"sheet={spreadsheet_title}")
        lines.append(f"total_votes={total_votes}")
        lines.append("breakdown: \n" + "\n".join(count_parts))
        lines.append(f"Google Sheet URL:")
        if state.get("spreadsheet_url"):
            lines.append(str(state["spreadsheet_url"]))

    text = "\n".join(lines)
    if len(text) <= 4000:
        await msg.reply_text(text)
        return

    chunk_lines = []
    chunk_len = 0
    for line in lines:
        line_len = len(line) + 1
        if chunk_lines and chunk_len + line_len > 3800:
            await msg.reply_text("\n".join(chunk_lines))
            chunk_lines = [line]
            chunk_len = line_len
            continue
        chunk_lines.append(line)
        chunk_len += line_len
    if chunk_lines:
        await msg.reply_text("\n".join(chunk_lines))


async def forgetpoll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    raw_arg = extract_command_body(msg.text or "", "forgetpoll").strip()
    if not raw_arg:
        await msg.reply_text(
            "Usage: /forgetpoll <poll_id>\n"
            "Tip: use /activesheets to copy a poll_id.\n"
            "This removes local tracking only (does not delete the Telegram poll or Google Sheet)."
        )
        return

    poll_id = raw_arg.split()[0].strip()
    if poll_id.lower().startswith("poll_id="):
        poll_id = poll_id.split("=", 1)[1].strip()
    poll_id = poll_id.strip(",.;")
    if not poll_id:
        await msg.reply_text("Invalid poll_id. Use /activesheets to copy a valid one.")
        return

    matched_key = None
    matched_state = None
    for key, state in POLL_STATES.items():
        if not (isinstance(key, tuple) and len(key) == 2 and key[0] == "native"):
            continue
        if str(key[1]) == poll_id:
            matched_key = key
            matched_state = state
            break

    if matched_key is None:
        await msg.reply_text(f"Tracked poll not found: {poll_id}")
        return

    POLL_STATES.pop(matched_key, None)
    save_native_poll_states()

    spreadsheet_url = ""
    if isinstance(matched_state, dict):
        spreadsheet_url = str(matched_state.get("spreadsheet_url", "") or "")

    reply = [
        f"Removed tracking for poll_id={poll_id}.",
        "Telegram poll and Google Sheet were not deleted.",
    ]
    if spreadsheet_url:
        reply.append(spreadsheet_url)
    await msg.reply_text("\n".join(reply))


async def pollstatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    tracker_url = ""
    loop = asyncio.get_running_loop()
    try:
        tracker_url = await loop.run_in_executor(
            None,
            lambda: get_or_create_tracker_overview_spreadsheet(SHEETS, DRIVE)[1],
        )
    except Exception as e:
        print("Tracker URL lookup failed for pollstatus:", e)

    raw_arg = extract_command_body(msg.text or "", "pollstatus").strip()

    native_items = []
    for key, state in POLL_STATES.items():
        if isinstance(key, tuple) and len(key) == 2 and key[0] == "native":
            native_items.append((str(key[1]), state))

    tracked_map = {poll_id: state for poll_id, state in native_items}

    # Per-poll lookup mode: /pollstatus <poll_id> [poll_id...]
    if raw_arg:
        raw_parts = raw_arg.replace("\n", " ").split()
        poll_ids = []
        seen: set[str] = set()
        for part in raw_parts:
            poll_id = part.strip()
            if poll_id.lower().startswith("poll_id="):
                poll_id = poll_id.split("=", 1)[1].strip()
            poll_id = poll_id.strip(",.;")
            if not poll_id or poll_id in seen:
                continue
            seen.add(poll_id)
            poll_ids.append(poll_id)

        if not poll_ids:
            await msg.reply_text(
                "Usage: /pollstatus [poll_id ...]\n"
                "Without arguments: shows tracked poll summary.\n"
                "With poll_id(s): shows tracked/untracked and open/closed (if tracked)."
            )
            return

        lines = ["Metadata sheet:"]
        if tracker_url:
            lines.append(str(tracker_url))
        else:
            lines.append("(unavailable)")
        lines.extend(["", "Poll status lookup:"])
        for poll_id in poll_ids:
            state = tracked_map.get(poll_id)
            if not isinstance(state, dict):
                lines.append(f"poll_id={poll_id} status=untracked")
                continue

            status = "closed" if state.get("closed") else "open"
            votes = state.get("votes") or {}
            total_votes = len(votes) if isinstance(votes, dict) else 0
            cap = int(state.get("cap", 0) or 0)
            line = f"poll_id={poll_id} status=tracked/{status} total_votes={total_votes}"
            if cap > 0:
                line += f" cap={cap}"
            lines.append(line)

            sheet_title = str(state.get("spreadsheet_title", "") or "").strip()
            if sheet_title:
                lines.append(f"sheet={sheet_title}")
            elif state.get("spreadsheet_id"):
                lines.append(f"sheet_id={state.get('spreadsheet_id')}")
        lines.append("")
        lines.append(
            "Note: untracked polls cannot be checked for open/closed status because local tracking is missing."
        )
        await msg.reply_text("\n".join(lines))
        return

    # Summary mode: /pollstatus
    open_ids = []
    closed_ids = []
    for poll_id, state in native_items:
        if isinstance(state, dict) and state.get("closed"):
            closed_ids.append(poll_id)
        else:
            open_ids.append(poll_id)

    def _poll_sort_key(poll_id: str):
        state = tracked_map.get(poll_id) or {}
        try:
            message_id = int(state.get("message_id", 0))
        except (TypeError, ValueError):
            message_id = 0
        return (message_id, poll_id)

    open_ids.sort(key=_poll_sort_key, reverse=True)
    closed_ids.sort(key=_poll_sort_key, reverse=True)

    lines = [
        "Metadata sheet:",
        tracker_url if tracker_url else "(unavailable)",
        "",
        "Poll tracking status:",
        f"total active polls={len(open_ids)}",
    ]

    if open_ids:
        lines.append("")
        lines.append("Active poll_ids:")
        for poll_id in open_ids:
            lines.append(f"- {poll_id}")
            state = tracked_map.get(poll_id) or {}
            poll_title = str(state.get("poll_title", "") or "").strip()
            lines.append(f"  title={poll_title}")

    if not native_items:
        lines.append("")
        lines.append("No tracked native polls found.")

    text = "\n".join(lines)
    if len(text) <= 4000:
        await msg.reply_text(text)
        return

    chunk_lines = []
    chunk_len = 0
    for line in lines:
        line_len = len(line) + 1
        if chunk_lines and chunk_len + line_len > 3800:
            await msg.reply_text("\n".join(chunk_lines))
            chunk_lines = [line]
            chunk_len = line_len
            continue
        chunk_lines.append(line)
        chunk_len += line_len
    if chunk_lines:
        await msg.reply_text("\n".join(chunk_lines))


async def _stop_tracked_poll_and_remove(
    context: ContextTypes.DEFAULT_TYPE,
    poll_id: str,
    *,
    closed_by: str = "",
) -> str:
    matched_key = None
    matched_state = None
    for key, state in POLL_STATES.items():
        if not (isinstance(key, tuple) and len(key) == 2 and key[0] == "native"):
            continue
        if str(key[1]) == poll_id:
            matched_key = key
            matched_state = state
            break

    if matched_key is None or not isinstance(matched_state, dict):
        raise ValueError(f"Tracked poll not found: {poll_id}")

    was_closed = bool(matched_state.get("closed"))

    chat_id = matched_state.get("chat_id")
    message_id_raw = matched_state.get("message_id")
    try:
        message_id = int(message_id_raw)
    except (TypeError, ValueError):
        message_id = None

    if not chat_id or message_id is None:
        raise ValueError(
            f"Cannot stop poll_id={poll_id}: missing tracked Telegram message id/chat id."
        )

    if not was_closed:
        try:
            await context.bot.stop_poll(chat_id=chat_id, message_id=message_id)
        except Exception as e:
            raise RuntimeError(f"Failed to stop poll_id={poll_id}: {e}") from e
        matched_state["closed"] = True

    spreadsheet_id = str(matched_state.get("spreadsheet_id", "") or "")
    if spreadsheet_id and not was_closed:
        close_date, close_time = now_utc8_date_time()
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: update_poll_info_fields(
                    SHEETS,
                    spreadsheet_id,
                    status="closed",
                    poll_status="closed",
                    date_closed=close_date,
                    time_closed=close_time,
                    closed_by=closed_by,
                ),
            )
        except Exception as e:
            print("Poll Info status update failed for stoppoll:", e)
    if not was_closed:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: update_tracker_overview_poll_status(
                    SHEETS,
                    DRIVE,
                    poll_id=poll_id,
                    poll_status="closed",
                    closed_by=closed_by,
                ),
            )
        except Exception as e:
            print("Tracker overview status update failed for stoppoll:", e)

    POLL_STATES.pop(matched_key, None)
    save_native_poll_states()

    reply = []
    if was_closed:
        reply.append(f"Poll already closed: {poll_id}")
    else:
        reply.append(f"Closed poll_id={poll_id}.")
        reply.append("Participants can no longer vote or edit their vote.")
    reply.append("Tracking removed for this poll.")
    spreadsheet_url = str(matched_state.get("spreadsheet_url", "") or "")
    if spreadsheet_url:
        reply.append(spreadsheet_url)
    return "\n".join(reply)


async def _send_stoppoll_confirmation_prompt(
    reply_target,
    poll_id: str,
    poll_state: dict,
    *,
    chat_id,
    user_id: str,
) -> None:
    _prune_pending_stoppoll_confirmations()
    token = uuid.uuid4().hex
    PENDING_STOPPOLL_CONFIRMATIONS[token] = {
        "poll_id": poll_id,
        "chat_id": str(chat_id),
        "user_id": str(user_id or ""),
        "created_ts": time.time(),
    }

    poll_title = str(poll_state.get("poll_title", "") or "").strip()
    lines = [
        f"poll_id={poll_id}",
        f"title={poll_title}",
        "",
        "Note that stopping poll is irreversible and users can no longer add or edit their votes.",
        "Proceed?",
    ]
    await reply_target.reply_text(
        "\n".join(lines),
        reply_markup=stoppoll_confirmation_keyboard(token),
    )


async def stoppoll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    raw_arg = extract_command_body(msg.text or "", "stoppoll").strip()
    if not raw_arg:
        native_items = []
        for key, state in POLL_STATES.items():
            if isinstance(key, tuple) and len(key) == 2 and key[0] == "native":
                native_items.append((str(key[1]), state))

        if not native_items:
            await msg.reply_text("No tracked native polls found.")
            return

        def _sort_key(item):
            poll_id, state = item
            try:
                message_id = int(state.get("message_id", 0))
            except (TypeError, ValueError):
                message_id = 0
            return (message_id, poll_id)

        native_items.sort(key=_sort_key, reverse=True)

        _prune_pending_stoppoll_pickers()
        token = uuid.uuid4().hex
        PENDING_STOPPOLL_PICKERS[token] = {
            "poll_ids": [poll_id for poll_id, _ in native_items],
            "chat_id": str(msg.chat_id),
            "user_id": str(msg.from_user.id) if msg.from_user else "",
            "created_ts": time.time(),
        }

        await msg.reply_text(
            "Select a poll to stop (you will be asked to confirm next):",
            reply_markup=stoppoll_picker_keyboard(token, native_items),
        )
        return

    poll_id = raw_arg.split()[0].strip()
    if poll_id.lower().startswith("poll_id="):
        poll_id = poll_id.split("=", 1)[1].strip()
    poll_id = poll_id.strip(",.;")
    if not poll_id:
        await msg.reply_text("Invalid poll_id. Use /pollstatus to copy a valid one.")
        return

    matched_key = None
    matched_state = None
    for key, state in POLL_STATES.items():
        if not (isinstance(key, tuple) and len(key) == 2 and key[0] == "native"):
            continue
        if str(key[1]) == poll_id:
            matched_key = key
            matched_state = state
            break

    if matched_key is None or not isinstance(matched_state, dict):
        await msg.reply_text(f"Tracked poll not found: {poll_id}")
        return

    await _send_stoppoll_confirmation_prompt(
        msg,
        poll_id,
        matched_state,
        chat_id=msg.chat_id,
        user_id=str(msg.from_user.id) if msg.from_user else "",
    )


async def publishpoll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    raw_body = extract_command_body(msg.text or "", "publishpoll")
    if not raw_body.strip():
        await msg.reply_text(
            "Usage error: /publishpoll requires title and date.\n"
            "Refer to /sample for copy and paste-ready template.\n\n"
        )
        return

    _, _, normalized_raw_body, validation_error = validate_publishpoll_required_fields(raw_body)
    if validation_error:
        await msg.reply_text(
            "Usage error:\n"
            f"{validation_error}\n"
            "Refer to /sample for format rules."
        )
        return

    poll_prompt, parse_mode = build_poll_prompt(normalized_raw_body)
    context_text = strip_prompt_line(poll_prompt, parse_mode)
    poll_question = _condense_poll_question(normalized_raw_body)
    poll_choices = extract_native_poll_choices(normalized_raw_body)
    poll_cap = extract_poll_cap(normalized_raw_body)
    poll_options = [label for label, _ in poll_choices]
    context_preview = _preview_plain_text(context_text, parse_mode)

    preview_lines = ["Preview only (not published yet)"]
    if context_preview:
        preview_lines.extend(["", "Poll details:", context_preview])
    else:
        preview_lines.extend(["", "Poll question:", poll_question])
    preview_lines.append("")
    preview_lines.append("Options:")
    for i, option in enumerate(poll_options, start=1):
        preview_lines.append(f"{i}. {option}")
    if poll_cap > 0:
        preview_lines.append(f"Cap: {poll_cap}")
    preview_lines.extend(["", "Publish this poll?"])

    _prune_pending_publish_previews()
    token = uuid.uuid4().hex
    PENDING_PUBLISH_PREVIEWS[token] = {
        "raw_body": normalized_raw_body,
        "chat_id": str(msg.chat_id),
        "user_id": str(msg.from_user.id) if msg.from_user else "",
        "created_ts": time.time(),
    }
    await msg.reply_text(
        "\n".join(preview_lines),
        reply_markup=publishpoll_preview_keyboard(token),
    )


async def on_publishpoll_preview_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    parts = (query.data or "").split("|")
    if len(parts) != 3 or parts[0] != "ppc":
        await query.answer()
        return

    _, token, action = parts
    _prune_pending_publish_previews()
    pending = PENDING_PUBLISH_PREVIEWS.get(token)
    if not pending:
        await query.answer("Preview expired. Run /publishpoll again.", show_alert=True)
        return

    requester_id = str(pending.get("user_id", ""))
    actor_id = str(query.from_user.id) if query.from_user else ""
    if requester_id and actor_id and requester_id != actor_id:
        await query.answer("Only the requester can confirm/cancel.", show_alert=True)
        return

    if action == "cancel":
        PENDING_PUBLISH_PREVIEWS.pop(token, None)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.answer("Cancelled")
        if query.message:
            await query.message.reply_text("Cancelled. Poll was not published.")
        return

    if action != "ok":
        await query.answer()
        return

    PENDING_PUBLISH_PREVIEWS.pop(token, None)
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    await query.answer("Publishing...")
    if not query.message:
        return

    try:
        await _send_native_poll_and_track(
            context,
            chat_id=query.message.chat_id,
            raw_body=str(pending.get("raw_body", "")),
            actor_user=query.from_user,
        )
    except Exception as e:
        print("Publish from preview failed:", e)
        await query.message.reply_text(f"Publish failed: {e}")


async def on_stoppoll_confirmation_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    parts = (query.data or "").split("|")
    if len(parts) != 3 or parts[0] != "spc":
        await query.answer()
        return

    _, token, action = parts
    _prune_pending_stoppoll_confirmations()
    pending = PENDING_STOPPOLL_CONFIRMATIONS.get(token)
    if not pending:
        await query.answer("Confirmation expired. Run /stoppoll again.", show_alert=True)
        return

    requester_id = str(pending.get("user_id", ""))
    actor_id = str(query.from_user.id) if query.from_user else ""
    if requester_id and actor_id and requester_id != actor_id:
        await query.answer("Only the requester can confirm/cancel.", show_alert=True)
        return

    if action == "cancel":
        PENDING_STOPPOLL_CONFIRMATIONS.pop(token, None)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.answer("Cancelled")
        if query.message:
            await query.message.reply_text("Cancelled. Poll was not stopped.")
        return

    if action != "ok":
        await query.answer()
        return

    PENDING_STOPPOLL_CONFIRMATIONS.pop(token, None)
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    poll_id = str(pending.get("poll_id", "") or "").strip()
    if not poll_id:
        await query.answer("Missing poll_id. Run /stoppoll again.", show_alert=True)
        return

    await query.answer("Stopping...")
    if not query.message:
        return

    closer = ""
    if query.from_user:
        if getattr(query.from_user, "username", None):
            closer = f"@{query.from_user.username}"
        else:
            closer = f"user_id:{query.from_user.id}"

    try:
        result_text = await _stop_tracked_poll_and_remove(context, poll_id, closed_by=closer)
    except ValueError as e:
        await query.message.reply_text(str(e))
        return
    except RuntimeError as e:
        await query.message.reply_text(str(e))
        return
    except Exception as e:
        print("stoppoll confirm action failed:", e)
        await query.message.reply_text(f"Failed to stop poll_id={poll_id}: {e}")
        return

    await query.message.reply_text(result_text)


async def on_stoppoll_picker_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    parts = (query.data or "").split("|")
    if len(parts) != 3 or parts[0] != "sps":
        await query.answer()
        return

    _, token, action = parts
    _prune_pending_stoppoll_pickers()
    pending = PENDING_STOPPOLL_PICKERS.get(token)
    if not pending:
        await query.answer("Selection expired. Run /stoppoll again.", show_alert=True)
        return

    requester_id = str(pending.get("user_id", ""))
    actor_id = str(query.from_user.id) if query.from_user else ""
    if requester_id and actor_id and requester_id != actor_id:
        await query.answer("Only the requester can select/cancel.", show_alert=True)
        return

    if action == "cancel":
        PENDING_STOPPOLL_PICKERS.pop(token, None)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.answer("Cancelled")
        if query.message:
            await query.message.reply_text("Cancelled. Poll was not stopped.")
        return

    try:
        idx = int(action)
    except (TypeError, ValueError):
        await query.answer()
        return

    poll_ids = pending.get("poll_ids") or []
    if not isinstance(poll_ids, list) or idx < 0 or idx >= len(poll_ids):
        await query.answer("Selection invalid. Run /stoppoll again.", show_alert=True)
        return

    poll_id = str(poll_ids[idx]).strip()
    if not poll_id:
        await query.answer("Selection invalid. Run /stoppoll again.", show_alert=True)
        return

    matched_state = None
    for key, state in POLL_STATES.items():
        if isinstance(key, tuple) and len(key) == 2 and key[0] == "native" and str(key[1]) == poll_id:
            matched_state = state
            break
    if not isinstance(matched_state, dict):
        await query.answer("Poll no longer tracked. Run /stoppoll again.", show_alert=True)
        return

    PENDING_STOPPOLL_PICKERS.pop(token, None)
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    await query.answer("Selected")
    if not query.message:
        return

    await _send_stoppoll_confirmation_prompt(
        query.message,
        poll_id,
        matched_state,
        chat_id=query.message.chat_id,
        user_id=actor_id,
    )


async def on_native_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = update.poll_answer
    if not answer or not answer.user:
        return

    poll_key = ("native", answer.poll_id)
    poll_state = POLL_STATES.get(poll_key)
    if not poll_state:
        print("Native poll answer received for untracked poll:", answer.poll_id)
        return

    loop = asyncio.get_running_loop()

    global MEMBER_INDEX, MEMBER_INDEX_LAST_REFRESH_TS
    if MEMBER_RAW_SOURCE or MEMBER_CHECK_SOURCE:
        now = time.time()
        refresh_due = (
            MEMBER_INDEX_LAST_REFRESH_TS <= 0
            or MEMBER_CHECK_REFRESH_SECONDS <= 0
            or (now - MEMBER_INDEX_LAST_REFRESH_TS) >= MEMBER_CHECK_REFRESH_SECONDS
        )
        if refresh_due:
            try:
                if MEMBER_RAW_SOURCE:
                    MEMBER_INDEX = await loop.run_in_executor(
                        None,
                        lambda: load_member_check_index_from_raw_source(SHEETS, MEMBER_RAW_SOURCE),
                    )
                else:
                    MEMBER_INDEX = await loop.run_in_executor(
                        None,
                        lambda: load_member_check_index_from_sheet(SHEETS, MEMBER_CHECK_SOURCE, MEMBER_CHECK_TAB),
                    )
                MEMBER_INDEX_LAST_REFRESH_TS = time.time()
            except Exception as e:
                print("Member check live refresh failed:", e)

    poll_choices = poll_state.get("choices", list(CHOICES))
    user = answer.user
    prev_idx = poll_state["votes"].get(user.id)
    option_ids = list(answer.option_ids or [])

    # Vote removed / retracted
    if not option_ids:
        if prev_idx is None:
            return
        del poll_state["votes"][user.id]
        poll_state["counts"][prev_idx] -= 1
        action = "Cancelled vote"
        new_choice_text = "CANCELLED"
        new_lunch = ""

    else:
        idx = option_ids[0]
        if idx < 0 or idx >= len(poll_choices):
            return

        # Same answer event (usually no-op for Telegram, but guard anyway)
        if prev_idx is not None and prev_idx == idx:
            return

        if prev_idx is not None:
            poll_state["votes"][user.id] = idx
            poll_state["counts"][prev_idx] -= 1
            poll_state["counts"][idx] += 1
            action = f"Changed vote: {poll_choices[prev_idx][0]} -> {poll_choices[idx][0]}"
        else:
            poll_state["votes"][user.id] = idx
            poll_state["counts"][idx] += 1
            action = "Recorded vote"

        new_choice_text, new_lunch = poll_choices[idx]

    username = user.username or ""
    full_name = (user.full_name or "").strip()
    username_link = f"https://t.me/{username}" if username else ""
    handle = f"@{username}" if username else full_name
    newcomer_value = classify_newcomer(MEMBER_INDEX, username, full_name)
    gender_value = lookup_member_gender(MEMBER_INDEX, username)
    row_date, row_time = now_utc8_date_time()

    def _write():
        append_row(
            SHEETS,
            poll_state["spreadsheet_id"],
            "Votes!A:H",
            [
                row_date,
                row_time,
                str(user.id),
                username,
                full_name,
                new_choice_text,
                new_lunch,
                action,
            ],
        )

        upsert_name_result(
            SHEETS,
            poll_state["spreadsheet_id"],
            poll_state,
            user_id=user.id,
            username_link=username_link,
            full_name=full_name,
            handle=handle,
            status="Pulled out" if new_choice_text == "CANCELLED" else "Signed up",
            selected_option=new_choice_text,
            lunch=new_lunch,
            gender=gender_value,
            newcomer=newcomer_value,
        )

    await loop.run_in_executor(None, _write)
    await loop.run_in_executor(
        None,
        lambda: update_tally(
            SHEETS,
            poll_state["spreadsheet_id"],
            poll_state.get("choices", CHOICES),
            poll_state.get("counts", [0, 0]),
        ),
    )
    try:
        await loop.run_in_executor(
            None,
            lambda: update_tracker_overview_aggregates(
                SHEETS,
                DRIVE,
                poll_id=str(answer.poll_id),
                total_votes=len(poll_state.get("votes", {})),
                option_vote_counts=list(poll_state.get("counts", [])),
            ),
        )
    except Exception as e:
        print("Tracker overview aggregate update failed:", e)
    save_native_poll_states()

    cap = int(poll_state.get("cap", 0) or 0)
    if not poll_state.get("closed") and cap > 0 and len(poll_state["votes"]) >= cap:
        chat_id = poll_state.get("chat_id")
        message_id_raw = poll_state.get("message_id")
        try:
            message_id = int(message_id_raw)
        except (TypeError, ValueError):
            message_id = None

        if chat_id and message_id is not None:
            try:
                await context.bot.stop_poll(chat_id=chat_id, message_id=message_id)
                poll_state["closed"] = True
                save_native_poll_states()
                spreadsheet_id = str(poll_state.get("spreadsheet_id", "") or "")
                if spreadsheet_id:
                    close_date, close_time = now_utc8_date_time()
                    try:
                        await loop.run_in_executor(
                            None,
                            lambda: update_poll_info_fields(
                                SHEETS,
                                spreadsheet_id,
                                status="closed",
                                poll_status="closed",
                                date_closed=close_date,
                                time_closed=close_time,
                                closed_by="auto_cap",
                            ),
                        )
                    except Exception as e:
                        print("Poll Info status update failed for auto-close:", e)
                try:
                    await loop.run_in_executor(
                        None,
                        lambda: update_tracker_overview_poll_status(
                            SHEETS,
                            DRIVE,
                            poll_id=str(answer.poll_id),
                            poll_status="closed",
                            closed_by="auto_cap",
                        ),
                    )
                except Exception as e:
                    print("Tracker overview status update failed for auto-close:", e)
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"Poll closed automatically (cap reached: {cap}).",
                    )
                except Exception as e:
                    print("Native poll cap-close notice failed:", e)
            except Exception as e:
                print("Native poll auto-close failed:", e)


def initialize_runtime_services():
    global SHEETS, DRIVE, MEMBER_INDEX, MEMBER_INDEX_LAST_REFRESH_TS

    if ALLOWED_TELEGRAM_USER_IDS:
        allowed = ", ".join(str(x) for x in sorted(ALLOWED_TELEGRAM_USER_IDS))
        print("Telegram allowlist enabled for user IDs:", allowed)
    else:
        print("Telegram allowlist disabled (ALLOWED_TELEGRAM_USER_IDS is empty).")

    creds = load_or_login_creds()
    SHEETS = build("sheets", "v4", credentials=creds)
    DRIVE = build("drive", "v3", credentials=creds)

    restored_native = load_native_poll_states()
    if restored_native:
        print("Restored native poll trackers:", restored_native, "from", NATIVE_POLL_STATE_FILE)

    if MEMBER_RAW_SOURCE:
        try:
            MEMBER_INDEX = load_member_check_index_from_raw_source(SHEETS, MEMBER_RAW_SOURCE)
            MEMBER_INDEX_LAST_REFRESH_TS = time.time()
            if MEMBER_INDEX.get("enabled"):
                print(
                    "Member check (live raw) loaded:",
                    len(MEMBER_INDEX["handles"]),
                    "handles from",
                    MEMBER_INDEX["source"],
                )
            else:
                print("Member check (live raw) loaded but empty:", MEMBER_INDEX["source"])
        except Exception as e:
            MEMBER_INDEX = _empty_member_index(MEMBER_RAW_SOURCE)
            print("Member check live raw load failed:", e)
    elif MEMBER_CHECK_SOURCE:
        try:
            MEMBER_INDEX = load_member_check_index_from_sheet(SHEETS, MEMBER_CHECK_SOURCE, MEMBER_CHECK_TAB)
            MEMBER_INDEX_LAST_REFRESH_TS = time.time()
            if MEMBER_INDEX.get("enabled"):
                print(
                    "Member check (live) loaded:",
                    len(MEMBER_INDEX["handles"]),
                    "handles from",
                    MEMBER_INDEX["source"],
                )
            else:
                print("Member check (live) loaded but empty:", MEMBER_INDEX["source"])
        except Exception as e:
            MEMBER_INDEX = _empty_member_index(f"{MEMBER_CHECK_SOURCE}:{MEMBER_CHECK_TAB}")
            print("Member check live load failed:", e)
    else:
        MEMBER_INDEX = load_member_check_index(MEMBER_CHECK_CSV_PATH)
        if MEMBER_INDEX.get("enabled"):
            print(
                "Member check loaded:",
                len(MEMBER_INDEX["handles"]),
                "handles from",
                MEMBER_INDEX["source"],
            )
        elif MEMBER_CHECK_CSV_PATH:
            print("Member check CSV not loaded:", MEMBER_CHECK_CSV_PATH)


def build_telegram_application() -> Application:
    telegram_app = Application.builder().token(BOT_TOKEN).build()
    telegram_app.add_handler(CommandHandler("start", with_allowed_user_check(start)))
    telegram_app.add_handler(CommandHandler("startall", with_allowed_user_check(startall)))
    telegram_app.add_handler(CommandHandler("sample", with_allowed_user_check(sample)))
    telegram_app.add_handler(CommandHandler("metadata", with_allowed_user_check(metadata)))
    telegram_app.add_handler(CommandHandler("pollstatus", with_allowed_user_check(pollstatus)))
    telegram_app.add_handler(CommandHandler("stoppoll", with_allowed_user_check(stoppoll)))
    telegram_app.add_handler(CommandHandler("publishpoll", with_allowed_user_check(publishpoll)))
    telegram_app.add_handler(PollAnswerHandler(with_allowed_user_check(on_native_poll_answer)))
    telegram_app.add_handler(
        CallbackQueryHandler(with_allowed_user_check(on_publishpoll_preview_action), pattern=r"^ppc\|")
    )
    telegram_app.add_handler(
        CallbackQueryHandler(with_allowed_user_check(on_stoppoll_picker_action), pattern=r"^sps\|")
    )
    telegram_app.add_handler(
        CallbackQueryHandler(with_allowed_user_check(on_stoppoll_confirmation_action), pattern=r"^spc\|")
    )
    return telegram_app


def webhook_url_path(url: str) -> str:
    # PTB run_webhook needs the URL path separately from the public webhook URL.
    path = (urlparse(url).path or "/webhook").lstrip("/")
    return path or "webhook"


def webhook_health_paths(url: str) -> list[str]:
    webhook_path = "/" + webhook_url_path(url).strip("/")
    candidates = ["/health", f"{webhook_path}/health"]
    seen: set[str] = set()
    out: list[str] = []
    for path in candidates:
        normalized = "/" + path.strip("/")
        if normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def install_ptb_webhook_health_routes(url: str) -> list[str]:
    """
    Patch PTB 20.x internal Tornado webhook app to expose health endpoints.

    This keeps using Application.run_webhook() (simple for Render) while adding
    GET/HEAD endpoints for uptime pings.
    """
    health_paths = webhook_health_paths(url)
    try:
        import tornado.web
        from telegram.ext import _updater as ptb_updater  # internal PTB module
        from telegram.ext._utils import webhookhandler as ptb_webhookhandler  # internal PTB module
    except Exception as e:
        print("Webhook health patch unavailable:", e)
        return health_paths

    base_cls = ptb_webhookhandler.WebhookAppClass
    if getattr(base_cls, "_votebot_health_patch", False):
        return health_paths

    class _HealthHandler(tornado.web.RequestHandler):
        def _write_body(self):
            self.set_header("Content-Type", "application/json")
            self.finish(
                json.dumps(
                    {
                        "status": "ok",
                        "service": "votebot",
                        "ts_utc": datetime.now(timezone.utc).isoformat(),
                    }
                )
            )

        def get(self):
            self._write_body()

        def head(self):
            self.set_status(200)
            self.finish()

    class _WebhookAppWithHealth(base_cls):
        _votebot_health_patch = True

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            extra_handlers = [
                (rf"{re.escape(path)}/?", _HealthHandler)
                for path in health_paths
            ]
            # Add health handlers after PTB registers the webhook route.
            self.add_handlers(r".*$", extra_handlers)

    ptb_webhookhandler.WebhookAppClass = _WebhookAppWithHealth
    ptb_updater.WebhookAppClass = _WebhookAppWithHealth
    return health_paths


def main():
    initialize_runtime_services()
    telegram_app = build_telegram_application()
    health_paths = install_ptb_webhook_health_routes(TELEGRAM_WEBHOOK_URL)
    if health_paths:
        print("Health endpoints:", ", ".join(health_paths))

    # PTB's built-in webhook server is enough for Render (no Flask/FastAPI needed).
    # Render assigns PORT and expects the service to bind to 0.0.0.0.
    telegram_app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=webhook_url_path(TELEGRAM_WEBHOOK_URL),
        webhook_url=TELEGRAM_WEBHOOK_URL,
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
