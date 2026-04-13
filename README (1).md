# Talkdesk Recording Extractor

A desktop GUI tool for bulk-downloading call recordings from the Talkdesk API. Built with Python and Tkinter, it handles OAuth2 authentication, automatic date-range chunking, smart resume via manifest files, and per-recording metadata export.

![Python](https://img.shields.io/badge/Python-3.10%2B-blue) ![License](https://img.shields.io/badge/license-MIT-green)

---

## Features

- **Dark-themed GUI** — clean, Bloomberg-style interface with real-time activity log
- **OAuth2 client credentials** — authenticates using a JSON credentials file (no manual token management)
- **Auto date-range chunking** — splits any date range into ≤31-day chunks to respect API limits
- **Smart resume** — a per-run manifest CSV tracks every call and its download status; re-runs skip files already on disk
- **Integrity check** — on resume, cross-references the manifest against actual files on disk and re-queues any missing recordings
- **MP3 output** — recordings are always downloaded as MP3 (the Talkdesk CDN serves MP3 regardless of any format request)
- **Metadata export** — saves a `metadata_<recording_id>.json` file alongside each audio file
- **Pause / Resume / Stop** — full thread-safe playback control without killing the process
- **Optional SSL certificate** — browse and select a CA bundle to enable proper TLS verification; disabled by default for corporate proxy compatibility
- **tkcalendar fallback** — if `tkcalendar` is not installed, plain `YYYY-MM-DD` text fields are used instead

---

## Requirements

- Python **3.10 or higher**
- Dependencies:

```
pip install requests tkcalendar
```

> `tkcalendar` is optional but recommended. Without it, date fields become plain text inputs.

---

## Credentials File

The tool authenticates with Talkdesk using the **Client Credentials** OAuth2 flow. You need a JSON file with your API credentials in one of these formats:

```json
{
  "id": "your_client_id",
  "secret": "your_client_secret"
}
```

Or wrapped in a list (both formats are supported):

```json
[
  {
    "id": "your_client_id",
    "secret": "your_client_secret"
  }
]
```

The OAuth token endpoint used is:

```
https://<company_name>.talkdeskid.com/oauth/token
```

The following scopes are requested automatically:

```
data-reports:read  data-reports:write  recordings:read
```

---

## How to Run

```bash
python extractor_v2_5.py
```

Or on Windows, create a launcher `Iniciar.bat`:

```bat
@echo off
python extractor_v2_5.py
pause
```

---

## Usage

### Step 1 — Fill in Configuration

| Field | Description |
|---|---|
| **Company Name** | Your Talkdesk account subdomain (e.g. `mycompany` → token URL becomes `https://mycompany.talkdeskid.com/oauth/token`) |
| **Credentials File** | Path to your JSON credentials file (use the Browse button) |
| **Date Range** | Start and end dates for the extraction (defaults to the last 7 days) |
| **SSL Certificate** | Optional. Check **Enable** and browse to a `.pem` / `.crt` / `.cer` CA bundle to use for TLS verification. Leave unchecked to disable verification (default) |

### Step 2 — Press ▶ Start

The tool runs through the following pipeline:

1. **Disk pre-scan** — checks the `recordings/` folder for files already downloaded and builds a set of existing recording IDs
2. **Manifest check** — looks for an existing `Interactions to download YYYYMMDD-YYYYMMDD.csv` file from a previous run
   - If found: loads confirmed-done calls, skips them, and re-queues any whose audio files are missing from disk
   - If not found: proceeds to submit a fresh API report
3. **Authenticate** — fetches a Bearer token using your credentials
4. **Report submission** — submits one calls report job per ≤31-day chunk; waits for each job to finish (polls every 5 seconds)
5. **Manifest creation** — writes a new manifest CSV listing every call ID and its recording URL
6. **Download loop** — for each call with a recording:
   - Fetches the list of recording IDs for that call (`GET /calls/{id}/recordings`)
   - Skips any recording IDs already on disk
   - Downloads the audio file (`GET /recordings/{id}/media`) and its metadata JSON (`GET /recordings/{id}`)
   - Updates the manifest after each completed call

### Step 3 — Monitor Progress

- The **progress bar** shows overall completion percentage
- The **activity log** shows timestamped messages color-coded by severity:
  - 🔵 Blue — current step / call being processed
  - ✅ Green — successful downloads and SSL cert active
  - ⚠️ Yellow — warnings (missing files, skipped calls, SSL disabled)
  - ❌ Red — errors (failed downloads, API errors)
  - Gray — verbose detail (job IDs, chunk info, skip notices)

---

## SSL Certificate

By default the tool disables TLS certificate verification so it works behind corporate intercepting proxies. If your environment provides a custom CA bundle you can enable proper verification:

1. Tick the **Enable** checkbox in the *SSL Certificate* row
2. Click **Browse…** and select your CA bundle file (`.pem`, `.crt`, or `.cer`)
3. The filename appears next to the checkbox and the activity log will confirm it at run start

**To go back to disabled verification**, simply uncheck the **Enable** checkbox. The cert path is cleared automatically.

The active mode is always shown at the top of the activity log when a run starts:

```
[10:23:01] SSL verification: disabled (no certificate).   ← default
[10:23:01] SSL certificate: my-company-ca.pem             ← when a cert is loaded
```

### Accepted certificate formats

| Extension | Format |
|---|---|
| `.pem` | PEM-encoded CA bundle (most common) |
| `.crt` | PEM or DER-encoded certificate |
| `.cer` | DER or PEM-encoded certificate |

> For most corporate environments, the IT team provides a single `.pem` file that bundles all internal root and intermediate CAs. Pass that file directly.

---

## Output Files

All files are written to a `recordings/` folder created next to the script.

| File | Description |
|---|---|
| `recordings/audio_<recording_id>.mp3` | The call audio file |
| `recordings/metadata_<recording_id>.json` | Full recording metadata from the Talkdesk API |
| `Interactions to download YYYYMMDD-YYYYMMDD.csv` | Manifest file tracking download status for the run |

### Manifest File Format

```csv
call_id,recording_url,recording_ids,downloaded
abc123,https://...,rec_id1|rec_id2,yes
def456,https://...,rec_id3,no
```

- `recording_ids` stores all IDs for that call, pipe-separated (`|`)
- `downloaded` is `yes` once all recordings for that call have been saved

---

## Pause, Resume & Stop

| Button | Behavior |
|---|---|
| **⏸ Pause** | Suspends the download loop after the current file completes; does not kill the thread |
| **▶ Resume** | Resumes from exactly where it paused |
| **■ Stop** | Signals the thread to exit gracefully after the current operation; manifest is always saved before exit |

---

## Resuming a Previous Run

If the script is stopped partway through or crashes, simply run it again **with the same date range**. The tool will:

1. Find the existing manifest file matching those dates
2. Skip all calls already marked `downloaded = yes`
3. Re-queue any calls whose audio files are missing from disk (integrity check)
4. Continue downloading only what remains

To force a **full re-run** from scratch, delete the manifest CSV file before starting.

---

## API Endpoints Used

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `https://<company>.talkdeskid.com/oauth/token` | Obtain Bearer token |
| `POST` | `/data/reports/calls/jobs` | Submit calls report job |
| `GET` | `/data/reports/calls/jobs/{job_id}` | Poll report job status |
| `GET` | `/data/reports/calls/files/{job_id}` | Download report CSV |
| `GET` | `/calls/{interaction_id}/recordings` | List recording IDs for a call |
| `GET` | `/recordings/{recording_id}` | Fetch recording metadata |
| `GET` | `/recordings/{recording_id}/media` | Download audio file (MP3) |

---

## Project Structure

```
extractor_v2_5.py              # Main application (single file)
recordings/                    # Created automatically on first run
  audio_<id>.mp3               # Downloaded audio files
  metadata_<id>.json           # Recording metadata
Interactions to download       # Manifest CSV (created per run)
  YYYYMMDD-YYYYMMDD.csv
```

---

## Troubleshooting

**`tkcalendar not found` warning in the log**
Install it with `pip install tkcalendar`. The tool works without it but date pickers become plain text fields.

**`Token error 401`**
Your `client_id` or `client_secret` is incorrect, or the credentials file keys are not named `id` and `secret`.

**`Report ended with status: failed`**
The Talkdesk report job failed server-side. Try a narrower date range or check your account's reporting permissions (`data-reports:read`/`write` scopes).

**SSL handshake error / `CERTIFICATE_VERIFY_FAILED`**
Enable the SSL Certificate option and select your corporate CA bundle. If you don't have one, ask IT for the internal root CA in `.pem` format.

**Download stops mid-run**
Press **▶ Resume** if paused, or re-run the script with the same date range. The manifest ensures no files are re-downloaded unnecessarily.

**Files are re-downloaded despite existing**
The manifest cross-references filenames of the form `audio_<recording_id>.mp3`. If files were renamed or moved, the tool cannot detect them and will re-download.

---

## License

MIT
