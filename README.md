# VoteBot (Telegram + Google Sheets)

A Telegram voting bot for DAYWA-style session signups.

It supports:
- Inline 2-option voting (via inline mode)
- Interactive Telegram vote messages with optional capacity (`/publishpoll`)
- Automatic Google Sheets tracking (one sheet per poll)
- Optional member-check enrichment from CSV or Google Sheets

## Tech Stack

- Python 3.11+
- `python-telegram-bot` (webhook mode)
- Google Sheets API + Google Drive API (OAuth)

## Commands

- `/start` - bot help
- `/sample` - show a `/publishpoll` template
- `/publishpoll` - create an interactive Telegram vote message and tracking sheet

Example `/publishpoll` body:

```text
channel=@yourchannel
title=DAYWA Discussions
desc=Join us for an afternoon...
date=23 Feb 2026
venue=Balestier Road
lunch=12:30-2pm
session=2-4pm
option1=discussion session only
option2=discussion session + lunch
cap=40
```

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Create a Telegram bot (via BotFather) and get the bot token.
3. Enable Google Sheets + Drive APIs and download OAuth client credentials.
4. Generate an OAuth user token locally (first run can open a browser login).
5. Set environment variables (required):

```bash
BOT_TOKEN=...
TELEGRAM_WEBHOOK_URL=https://your-domain.com/webhook
OAUTH_CLIENT_JSON=oauth_client.json      # path or raw JSON string
OAUTH_TOKEN_JSON=token.json              # path or raw JSON string
```

## Run

This app runs in webhook mode (not polling):

```bash
python vote_bot_oauth.py
```

It listens on `PORT` (default `10000`) and serves Telegram webhooks using `TELEGRAM_WEBHOOK_URL`.

## Deploy on Render (Step-by-Step)

This bot is already compatible with Render Web Services:
- it binds to `0.0.0.0`
- it reads `PORT` (default `10000`)
- it runs in webhook mode

The repo includes a `Procfile`:

```text
web: python vote_bot_oauth.py
```

### 1. Create a Render Web Service

In Render Dashboard:
1. `New` -> `Web Service`
2. Connect this GitHub repo
3. Select branch to deploy
4. Runtime: `Python 3`

Service settings:
- Build Command: `pip install -r requirements.txt`
- Start Command: `python vote_bot_oauth.py` (or leave Render to use the `Procfile`)

### 2. Set Required Environment Variables (Render -> Environment)

Add these variables before first deploy:

```bash
BOT_TOKEN=<telegram bot token>
TELEGRAM_WEBHOOK_URL=https://<your-render-service>.onrender.com/webhook
OAUTH_CLIENT_JSON=<raw oauth client json OR file path>
OAUTH_TOKEN_JSON=<raw oauth token json OR file path>
ALLOWED_TELEGRAM_USER_IDS=<optional comma/space-separated telegram user ids>
```

Notes:
- `TELEGRAM_WEBHOOK_URL` must be the full public HTTPS URL that Telegram will call.
- The path can be `/webhook` (recommended); the app derives the webhook path from this URL.
- The code also accepts `TELEGRAM_BOT_TOKEN` as an alternative to `BOT_TOKEN`.

### 3. OAuth on Render (Important)

Render cannot complete the first-time interactive Google OAuth login in a typical production deploy. The practical setup is:

1. Generate OAuth credentials locally (browser login) to produce `token.json`
2. Copy the contents of:
   - your OAuth client file (`oauth_client.json`)
   - your authorized user token (`token.json`)
3. Paste those JSON contents into Render as secret env vars:
   - `OAUTH_CLIENT_JSON`
   - `OAUTH_TOKEN_JSON`

This app supports both file paths and raw JSON strings for those variables, so pasting raw JSON works well on Render.

Alternative:
- Use Render Secret Files and point `OAUTH_CLIENT_JSON` / `OAUTH_TOKEN_JSON` to those file paths.

### 4. Deploy and Verify

After saving env vars, trigger a deploy (or select "Save, rebuild, and deploy" in Render).

Successful startup should show logs indicating:
- Google APIs initialized (Sheets/Drive)
- webhook server started

Then in Telegram:
1. Open your bot
2. Send `/start`
3. Send `/sample`
4. Try `/publishpoll` with the sample template

### 5. Optional Render Settings (Recommended)

- `PYTHON_VERSION`: pin a Python version if you want reproducible builds (for example `3.11.x`)
- Persistent Disk: recommended for live polls so accepted votes and active poll state survive restarts/redeploys

Recommended disk settings:

```text
Mount path: /var/data
Size: 1 GB
```

Recommended environment variables when a disk is attached:

```bash
TRACKED_POLL_STATE_FILE=/var/data/tracked_poll_states.json
PENDING_VOTE_EVENTS_FILE=/var/data/pending_vote_events.jsonl
```

Why a persistent disk helps:
- `tracked_poll_states.json` stores active poll tracking state locally
- `pending_vote_events.jsonl` stores accepted votes that are waiting to sync to Google Sheets
- file-based `OAUTH_TOKEN_JSON` can be refreshed/persisted to disk if you choose to store the token as a file

Without a persistent disk:
- local state files are ephemeral and may be lost on restart/redeploy
- use raw JSON env vars for OAuth secrets instead of file paths

If you use raw JSON for `OAUTH_TOKEN_JSON`, leave it as raw JSON. If you use a file path for the token and want refreshes to survive deploys, use a disk path:

```bash
OAUTH_TOKEN_JSON=/var/data/token.json
```

Live-poll reliability defaults can be overridden if needed:

```bash
VOTE_QUEUE_MAXSIZE=1000
VOTE_MESSAGE_REFRESH_DEBOUNCE_SECONDS=1.5
VOTE_TALLY_DEBOUNCE_SECONDS=3
VOTE_TRACKER_DEBOUNCE_SECONDS=8
TRACKED_STATE_SAVE_DEBOUNCE_SECONDS=1.5
VOTE_JOURNAL_COMPACT_SECONDS=30
VOTE_QUEUE_DRAIN_TIMEOUT_SECONDS=30
PENDING_VOTE_EVENTS_FSYNC=false
```

### 6. Common Render-Specific Issues

- `403/401` Google API errors:
  - OAuth token/client JSON is wrong, expired, or missing Sheets/Drive scopes
- Bot deploys but Telegram commands do nothing:
  - `BOT_TOKEN` is wrong
  - `TELEGRAM_WEBHOOK_URL` is incorrect (wrong domain/path)
  - the service is not publicly reachable yet
- Startup fails immediately:
  - one of the required env vars is missing (`BOT_TOKEN`, `TELEGRAM_WEBHOOK_URL`, `OAUTH_CLIENT_JSON`, `OAUTH_TOKEN_JSON`)
- Poll tracking resets after redeploy:
  - expected if using local `tracked_poll_states.json` without persistent storage

## Optional Environment Variables

Useful optional settings:
- `DEFAULT_PUBLISH_CHAT` - default destination chat/channel for `/publishpoll` (for example `@yourchannel` or `-1001234567890`)
- `DRIVE_FOLDER_ID` - parent folder for created sheets
- `SHEET_LINK_SHARE_ROLE` - `reader`, `commenter`, or `writer`
- `SHEET_LINK_ALLOW_DISCOVERY` - `true`/`false`
- `ALLOWED_TELEGRAM_USER_IDS` - restrict bot usage to specific Telegram user IDs (comma/space-separated), e.g. `12345678,23456789`
- `MEMBER_CHECK_CSV_PATH` - local CSV for member lookup
- `MEMBER_CHECK_SOURCE` / `MEMBER_CHECK_TAB` - live member lookup sheet
- `MEMBER_RAW_SOURCE` - raw member data sheet source
- `TRACKED_POLL_STATE_FILE` - local poll state file (default `tracked_poll_states.json`)
- `PENDING_VOTE_EVENTS_FILE` - local pending vote journal (default `pending_vote_events.jsonl`)
- `PENDING_VOTE_EVENTS_FSYNC` - set `true` to fsync each pending vote journal write
- `VOTE_QUEUE_MAXSIZE` - max queued pending vote writes per poll before vote handlers wait
- `VOTE_MESSAGE_REFRESH_DEBOUNCE_SECONDS` - minimum delay between Telegram count refreshes
- `VOTE_TALLY_DEBOUNCE_SECONDS` - minimum delay between Tally sheet summary writes
- `VOTE_TRACKER_DEBOUNCE_SECONDS` - minimum delay between tracker aggregate writes
- `TRACKED_STATE_SAVE_DEBOUNCE_SECONDS` - debounce delay for local poll state saves
- `VOTE_JOURNAL_COMPACT_SECONDS` - minimum interval between pending vote journal compactions
- `VOTE_QUEUE_DRAIN_TIMEOUT_SECONDS` - how long close/stop waits for pending vote writes to drain
