# VoteBot (Telegram + Google Sheets)

A Telegram voting bot for DAYWA-style session signups.

It supports:
- Inline 2-option voting (via inline mode)
- Native Telegram polls with optional capacity (`/publishpoll`)
- Automatic Google Sheets tracking (one sheet per poll)
- Optional member-check enrichment from CSV or Google Sheets

## Tech Stack

- Python 3.11+
- `python-telegram-bot` (webhook mode)
- Google Sheets API + Google Drive API (OAuth)

## Commands

- `/start` - bot help
- `/sample` - show a `/publishpoll` template
- `/publishpoll` - create a Telegram native poll and tracking sheet

Example `/publishpoll` body:

```text
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
- Persistent Disk: useful if you want local files to survive restarts (see below)

Why a persistent disk may help:
- `native_poll_states.json` stores native poll tracking state locally
- file-based `OAUTH_TOKEN_JSON` can be refreshed/persisted to disk

Without a persistent disk:
- local state files are ephemeral and may be lost on restart/redeploy
- use raw JSON env vars for OAuth secrets instead of file paths

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
  - expected if using local `native_poll_states.json` without persistent storage

## Optional Environment Variables

Useful optional settings:
- `DRIVE_FOLDER_ID` - parent folder for created sheets
- `SHEET_LINK_SHARE_ROLE` - `reader`, `commenter`, or `writer`
- `SHEET_LINK_ALLOW_DISCOVERY` - `true`/`false`
- `ALLOWED_TELEGRAM_USER_IDS` - restrict bot usage to specific Telegram user IDs (comma/space-separated), e.g. `12345678,23456789`
- `MEMBER_CHECK_CSV_PATH` - local CSV for member lookup
- `MEMBER_CHECK_SOURCE` / `MEMBER_CHECK_TAB` - live member lookup sheet
- `MEMBER_RAW_SOURCE` - raw member data sheet source
- `NATIVE_POLL_STATE_FILE` - local poll state file (default `native_poll_states.json`)
