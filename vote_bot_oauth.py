from __future__ import annotations

import os
import asyncio
import html
import re
import csv
import time
import json
from itertools import zip_longest
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultArticle, InputTextMessageContent
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes
)
from telegram.ext import InlineQueryHandler, ChosenInlineResultHandler, PollAnswerHandler
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

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

CHOICES = [
    ("discussion session only", "No"),   # (label, Lunch)
    ("discussion session + lunch", "Yes"),
]

NAMES_HEADERS = [
    'TG Link',
    "Name (auto)",
    "TG Handle (auto)",
    "Sign up status \n[Signed up, Invite sent, Confirmed, Pulled out, Waitlisted]",
    "Lunch\n[Yes, No]",
    "Profile ",
    "Gender",
    "New? (auto)",
    "Comments / Changes from VoteBot",
]

VOTES_HEADERS = [
    "ts_utc", "chat_id", "message_id", "user_id", "username", "full_name", "choice", "lunch", "action"
]

PUBLISHPOLL_SAMPLE_TEMPLATE = (
    "/publishpoll\n"
    "title=DAYWA Discussions\n"
    "desc=Join us for an afternoon...\n"
    "date=23 Feb 2026\n"
    "venue=Balestier Road\n"
    "lunch=12:30-2pm\n"
    "session=2-4pm\n"
    "option1=discussion session only\n"
    "option2=discussion session + lunch\n"
    "cap=40\n\n"
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
        "lunch1": "lunch1",
        "lunch 1": "lunch1",
        "lunch2": "lunch2",
        "lunch 2": "lunch2",
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


def build_poll_info_rows(
    *,
    file_title: str,
    created_utc: str,
    poll_title: Optional[str],
    poll_metadata: Optional[dict[str, str]],
    choices: list[tuple[str, str]],
    creator_handle: Optional[str] = None,
    creator_user_id: Optional[str] = None,
) -> list[list[str]]:
    meta = dict(poll_metadata or {})
    if poll_title and not meta.get("title"):
        meta["title"] = poll_title
    if not meta.get("option1"):
        meta["option1"] = choices[0][0]
    if not meta.get("option2"):
        meta["option2"] = choices[1][0]
    if not meta.get("lunch1"):
        meta["lunch1"] = choices[0][1]
    if not meta.get("lunch2"):
        meta["lunch2"] = choices[1][1]

    rows = [
        ["Key", "Value"],
        ["file_title", file_title],
        ["created_utc", created_utc],
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
        ["lunch1", meta.get("lunch1", "")],
        ["lunch2", meta.get("lunch2", "")],
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
) -> tuple[str, str]:
    choices = choices or CHOICES
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%SZ")
    title = f"VoteBot_{ts}"
    suffix = _compact_sheet_title_part(poll_title or "")
    if suffix:
        title = f"{title}_{suffix}"

    body = {
        "properties": {"title": title},
        "sheets": [
            {"properties": {"title": "Names"}},
            {"properties": {"title": "Votes"}},
            {"properties": {"title": "Tally"}},
            {"properties": {"title": "Poll Info"}},
        ],
    }

    ss = sheets.spreadsheets().create(body=body).execute()
    spreadsheet_id = ss["spreadsheetId"]
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

    # Move into folder if provided
    if DRIVE_FOLDER_ID:
        file_meta = drive.files().get(fileId=spreadsheet_id, fields="parents").execute()
        prev_parents = ",".join(file_meta.get("parents", []))
        drive.files().update(
            fileId=spreadsheet_id,
            addParents=DRIVE_FOLDER_ID,
            removeParents=prev_parents,
            fields="id,parents",
        ).execute()

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
        poll_title=poll_title,
        poll_metadata=poll_metadata,
        choices=list(choices),
        creator_handle=creator_handle,
        creator_user_id=creator_user_id,
    )
    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "RAW",
            "data": [
                {"range": "Names!A1:I1", "values": [NAMES_HEADERS]},
                {"range": "Votes!A1:I1", "values": [VOTES_HEADERS]},
                {"range": f"Poll Info!A1:B{len(poll_info_rows)}", "values": poll_info_rows},
                {
                    "range": "Tally!A1:B3",
                    "values": [
                        ["Option", "Count"],
                        [choices[0][0], 0],
                        [choices[1][0], 0],
                    ],
                },
            ],
        },
    ).execute()

    return spreadsheet_id, url


def append_row(sheets, spreadsheet_id: str, range_a1: str, row: list):
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


def update_tally(sheets, spreadsheet_id: str, c0: int, c1: int):
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="Tally!B2:B3",
        valueInputOption="RAW",
        body={"values": [[c0], [c1]]},
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
    lunch: str,
    gender: str,
    newcomer: str,
):
    row_num = poll_state["names_row_by_user"].get(user_id)
    if row_num is None:
        row_num = poll_state["next_names_row"]
        poll_state["names_row_by_user"][user_id] = row_num
        poll_state["next_names_row"] += 1

    # Update A:E, H, I and optionally G (gender). F is preserved for manual profile edits.
    data = [
        {
            "range": f"Names!A{row_num}:E{row_num}",
            "values": [[username_link, full_name, handle, status, lunch]],
        },
        {
            "range": f"Names!H{row_num}:H{row_num}",
            "values": [[newcomer]],
        },
        {
            "range": f"Names!I{row_num}:I{row_num}",
            "values": [[""]],
        },
    ]
    if gender:
        data.insert(
            1,
            {
                "range": f"Names!G{row_num}:G{row_num}",
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
POLL_STATES = {}  # ("chat", chat_id, message_id) or ("inline", inline_message_id) -> state
MEMBER_INDEX = {"enabled": False, "source": "", "handles": set(), "names": {}}
MEMBER_INDEX_LAST_REFRESH_TS = 0.0
SHEETS = None
DRIVE = None


def _serialize_poll_state(state: dict) -> dict:
    choices = state.get("choices", CHOICES)
    return {
        "spreadsheet_id": str(state.get("spreadsheet_id", "")),
        "spreadsheet_url": str(state.get("spreadsheet_url", "")),
        "choices": [[str(label), str(lunch)] for label, lunch in choices],
        "votes": {str(k): int(v) for k, v in state.get("votes", {}).items()},
        "counts": [int(x) for x in list(state.get("counts", [0, 0]))[:2]],
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
    spreadsheet_id = str(raw.get("spreadsheet_id", "")).strip()
    spreadsheet_url = str(raw.get("spreadsheet_url", "")).strip()
    if not spreadsheet_id:
        return None

    raw_choices = raw.get("choices") or CHOICES
    choices = []
    for item in raw_choices[:2]:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            choices.append((str(item[0]), str(item[1])))
    if len(choices) != 2:
        choices = list(CHOICES)

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

    counts = raw.get("counts") or [0, 0]
    try:
        counts = [int(counts[0]), int(counts[1])]
    except (IndexError, TypeError, ValueError):
        counts = [0, 0]

    try:
        next_names_row = int(raw.get("next_names_row", 2))
    except (TypeError, ValueError):
        next_names_row = 2

    try:
        cap = int(raw.get("cap", 0) or 0)
    except (TypeError, ValueError):
        cap = 0

    return {
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_url": spreadsheet_url,
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
    spreadsheet_id, spreadsheet_url = create_new_spreadsheet(
        SHEETS,
        DRIVE,
        poll_title=poll_title,
        choices=poll_choices,
        poll_metadata=poll_metadata,
        creator_handle=creator_handle,
        creator_user_id=creator_user_id,
    )
    print("Spreadsheet created:", spreadsheet_url, "for", poll_key)
    return {
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_url": spreadsheet_url,
        "choices": poll_choices,
        "votes": {},               # user_id -> choice_idx
        "counts": [0, 0],
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
        "lunch1": "lunch1",
        "lunch 1": "lunch1",
        "lunch2": "lunch2",
        "lunch 2": "lunch2",
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
    return [
        (parsed.get("option1", CHOICES[0][0]), parsed.get("lunch1", CHOICES[0][1])),
        (parsed.get("option2", CHOICES[1][0]), parsed.get("lunch2", CHOICES[1][1])),
    ]


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
        "lunch1": "lunch1",
        "lunch 1": "lunch1",
        "lunch2": "lunch2",
        "lunch 2": "lunch2",
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


def build_inline_result_preview(poll_prompt: str) -> tuple[str, str]:
    # Telegram inline result cards only have title + description, so collapse the
    # formatted poll message into a compact preview.
    plain = re.sub(r"<[^>]+>", "", poll_prompt)
    plain = html.unescape(plain)
    lines = [line.strip() for line in plain.splitlines() if line.strip()]

    if not lines or lines == ["Please vote:"]:
        return (
            "Template keys: title/date/venue/lunch/session/desc",
            "Use new lines: title=... / desc=... / date=... / venue=... / lunch=... / session=...",
        )

    title_line = next((x for x in lines if x.lower() != "please vote:"), "Discussion vote (2 options)")
    remaining = [x for x in lines if x != title_line]
    description = " • ".join(remaining) if remaining else "Tap to send this vote"

    return title_line[:64], description[:100]


def vote_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(CHOICES[0][0], callback_data="v|0")],
        [InlineKeyboardButton(CHOICES[1][0], callback_data="v|1")],
    ])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Ready.\n"
        "Use /publishpoll to send a forwardable native Telegram poll.\n"
        "Use /sample to get a copy-paste template for /publishpoll.\n"
        "A new spreadsheet is created per poll message."
    )


async def sample(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    await msg.reply_text(
        "Copy-paste this below:\n"
        "Minimally title and date fields should be filled. \n\n"
        f"{PUBLISHPOLL_SAMPLE_TEMPLATE}"
    )


async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = update.inline_query.query or ""
    poll_prompt, parse_mode = build_poll_prompt(query_text)
    preview_title, preview_desc = build_inline_result_preview(poll_prompt)

    result = InlineQueryResultArticle(
        id=str(uuid.uuid4()),
        title=preview_title,
        description=preview_desc,
        input_message_content=InputTextMessageContent(poll_prompt, parse_mode=parse_mode),
        reply_markup=vote_keyboard(),  # your existing keyboard
    )
    await update.inline_query.answer([result], cache_time=0)


async def on_chosen_inline_result(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chosen = update.chosen_inline_result
    if not chosen or not chosen.inline_message_id:
        return

    poll_key = ("inline", chosen.inline_message_id)
    if poll_key in POLL_STATES:
        return

    loop = asyncio.get_running_loop()
    raw_query = chosen.query or ""
    poll_title = extract_poll_title(raw_query)
    poll_metadata = extract_poll_metadata(raw_query)
    creator_handle = f"@{chosen.from_user.username}" if getattr(chosen, "from_user", None) and chosen.from_user.username else ""
    creator_user_id = str(chosen.from_user.id) if getattr(chosen, "from_user", None) and chosen.from_user else ""
    POLL_STATES[poll_key] = await loop.run_in_executor(
        None,
        lambda: create_poll_state(
            poll_key,
            poll_title=poll_title,
            poll_metadata=poll_metadata,
            creator_handle=creator_handle,
            creator_user_id=creator_user_id,
        ),
    )


async def publishpoll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    raw_body = extract_command_body(msg.text or "", "publishpoll")
    if not raw_body.strip():
        await msg.reply_text(
            "Usage: /publishpoll cannot have empty title and date.\n"
            "Refer to /sample for copy and paste-ready template.\n\n"
        )
        return

    poll_prompt, parse_mode = build_poll_prompt(raw_body)
    context_text = strip_prompt_line(poll_prompt, parse_mode)
    poll_question = (extract_poll_title(raw_body) or "Please vote").strip()
    poll_metadata = extract_poll_metadata(raw_body)
    poll_choices = extract_native_poll_choices(raw_body)
    poll_cap = extract_poll_cap(raw_body)
    creator_handle = f"@{msg.from_user.username}" if msg.from_user and msg.from_user.username else ""
    creator_user_id = str(msg.from_user.id) if msg.from_user else ""
    poll_options = [label for label, _ in poll_choices]

    if context_text:
        await msg.reply_text(context_text, parse_mode=parse_mode)

    poll_msg = await context.bot.send_poll(
        chat_id=msg.chat_id,
        question=poll_question[:300],
        options=poll_options,
        is_anonymous=False,
        allows_multiple_answers=False,
    )

    native_poll = getattr(poll_msg, "poll", None)
    if native_poll and native_poll.id:
        poll_key = ("native", native_poll.id)
        loop = asyncio.get_running_loop()
        poll_state = await loop.run_in_executor(
            None,
            lambda: create_poll_state(
                poll_key,
                poll_title=poll_question,
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
        await msg.reply_text(f"Tracking sheet (internal circulation): {poll_state['spreadsheet_url']}")


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
    ts = datetime.now(timezone.utc).isoformat()

    row_chat_id = poll_state.get("chat_id", "native")
    row_message_id = poll_state.get("message_id", answer.poll_id)

    def _write():
        append_row(
            SHEETS,
            poll_state["spreadsheet_id"],
            "Votes!A:I",
            [
                ts,
                row_chat_id,
                row_message_id,
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
            poll_state["counts"][0],
            poll_state["counts"][1],
        ),
    )
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
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"Poll closed automatically (cap reached: {cap}).",
                    )
                except Exception as e:
                    print("Native poll cap-close notice failed:", e)
            except Exception as e:
                print("Native poll auto-close failed:", e)


async def on_vote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    msg = query.message
    user = query.from_user
    inline_msg_id = query.inline_message_id
    if not msg and not inline_msg_id:
        return

    parts = (query.data or "").split("|")
    if len(parts) != 2 or parts[0] != "v":
        return

    try:
        idx = int(parts[1])
    except ValueError:
        return
    if idx < 0 or idx >= len(CHOICES):
        return

    loop = asyncio.get_running_loop()

    if msg:
        poll_key = ("chat", msg.chat_id, msg.message_id)
        row_chat_id = str(msg.chat_id)
        row_message_id = str(msg.message_id)
    else:
        poll_key = ("inline", inline_msg_id)
        row_chat_id = "inline"
        row_message_id = inline_msg_id

    poll_state = POLL_STATES.get(poll_key)
    if poll_state is None:
        fallback_title = None
        fallback_metadata = None
        creator_handle = f"@{user.username}" if user and user.username else ""
        creator_user_id = str(user.id) if user else ""
        if msg and getattr(msg, "text", None):
            msg_text = msg.text or ""
            first_line = msg_text.splitlines()[0].strip()
            if first_line and first_line.lower() != "please vote:":
                fallback_title = first_line
                fallback_metadata = {"title": fallback_title}
            parsed_meta = extract_poll_metadata(msg_text)
            if parsed_meta:
                fallback_metadata = {**(fallback_metadata or {}), **parsed_meta}
        poll_state = await loop.run_in_executor(
            None,
            lambda: create_poll_state(
                poll_key,
                poll_title=fallback_title,
                poll_metadata=fallback_metadata,
                creator_handle=creator_handle,
                creator_user_id=creator_user_id,
            ),
        )
        POLL_STATES[poll_key] = poll_state

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

    prev_idx = poll_state["votes"].get(user.id)  # None if first vote

    # Same option clicked again => cancel
    if prev_idx is not None and prev_idx == idx:
        del poll_state["votes"][user.id]
        poll_state["counts"][idx] -= 1

        action = "Cancelled vote"
        new_choice_text = "CANCELLED"
        new_lunch = ""
        reply_text = "Cancelled your vote."

    # Different option clicked => change
    elif prev_idx is not None and prev_idx != idx:
        poll_state["votes"][user.id] = idx
        poll_state["counts"][prev_idx] -= 1
        poll_state["counts"][idx] += 1

        action = f"Changed vote: {CHOICES[prev_idx][0]} -> {CHOICES[idx][0]}"
        new_choice_text, new_lunch = CHOICES[idx][0], CHOICES[idx][1]
        reply_text = f"Changed: {new_choice_text} ✅"

    # First vote
    else:
        poll_state["votes"][user.id] = idx
        poll_state["counts"][idx] += 1

        action = "Recorded vote"
        new_choice_text, new_lunch = CHOICES[idx][0], CHOICES[idx][1]
        reply_text = f"Recorded: {new_choice_text} ✅"

    # Build user fields
    username = user.username or ""
    full_name = (user.full_name or "").strip()
    username_link = f"https://t.me/{username}" if username else ""
    handle = f"@{username}" if username else full_name
    newcomer_value = classify_newcomer(MEMBER_INDEX, username, full_name)
    gender_value = lookup_member_gender(MEMBER_INDEX, username)

    ts = datetime.now(timezone.utc).isoformat()

    # Run Google writes in a thread (so Telegram loop stays responsive)
    def _write():
        append_row(SHEETS, poll_state["spreadsheet_id"], "Votes!A:I",
                   [ts, row_chat_id, row_message_id, str(user.id), username, full_name, new_choice_text, new_lunch, action])

        upsert_name_result(
            SHEETS,
            poll_state["spreadsheet_id"],
            poll_state,
            user_id=user.id,
            username_link=username_link,
            full_name=full_name,
            handle=handle,
            status="Pulled out" if new_choice_text == "CANCELLED" else "Signed up",
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
            poll_state["counts"][0],
            poll_state["counts"][1],
        ),
    )

    if msg:
        await msg.reply_text(reply_text)

def initialize_runtime_services():
    global SHEETS, DRIVE, MEMBER_INDEX, MEMBER_INDEX_LAST_REFRESH_TS

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
    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CommandHandler("sample", sample))
    telegram_app.add_handler(CommandHandler("publishpoll", publishpoll))
    telegram_app.add_handler(PollAnswerHandler(on_native_poll_answer))
    telegram_app.add_handler(CallbackQueryHandler(on_vote))
    telegram_app.add_handler(InlineQueryHandler(inline_query))
    telegram_app.add_handler(ChosenInlineResultHandler(on_chosen_inline_result))
    return telegram_app


def webhook_url_path(url: str) -> str:
    # PTB run_webhook needs the URL path separately from the public webhook URL.
    path = (urlparse(url).path or "/webhook").lstrip("/")
    return path or "webhook"


def main():
    initialize_runtime_services()
    telegram_app = build_telegram_application()

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
