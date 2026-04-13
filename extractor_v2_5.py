"""
Talkdesk Recording Extractor — GUI Edition
==========================================
A graphical interface for downloading call recordings from the Talkdesk API.
Supports company name configuration, credential file selection, date range
selection, smart resume via interaction manifest files, and optional SSL
certificate verification for corporate proxy environments.

Dependencies:
    pip install requests tkcalendar
"""

import json
import time
import requests
import csv
import io
import os
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime, timedelta, timezone

# ── GLOBAL SSL PATCH ──────────────────────────────────────────────────────────
import urllib3

# Suppress insecure-request warnings that appear when verify=False.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# _SSL_VERIFY controls certificate verification for every outbound request:
#   False        → skip verification entirely (default, corporate-proxy safe)
#   "/path/cert" → path to a CA bundle (.pem/.crt) to use for verification
# This variable is updated at runtime by the UI before each extraction run.
_SSL_VERIFY = False

_original_get  = requests.get
_original_post = requests.post

def _patched_get(*args, **kwargs):
    """Wrap requests.get to inject the current SSL verification setting."""
    kwargs.setdefault("verify", _SSL_VERIFY)
    return _original_get(*args, **kwargs)

def _patched_post(*args, **kwargs):
    """Wrap requests.post to inject the current SSL verification setting."""
    kwargs.setdefault("verify", _SSL_VERIFY)
    return _original_post(*args, **kwargs)

requests.get  = _patched_get
requests.post = _patched_post
# ──────────────────────────────────────────────────────────────────────────────

# Try importing tkcalendar; provide install guidance if missing
try:
    from tkcalendar import DateEntry
    HAS_TKCALENDAR = True
except ImportError:
    HAS_TKCALENDAR = False


# ──────────────────────────────────────────────────────────────────────────────
# THEME & CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

DARK_BG      = "#0f1117"
PANEL_BG     = "#1a1d27"
BORDER_COLOR = "#2a2d3e"
ACCENT       = "#4f8ef7"
ACCENT_DARK  = "#3a6fd4"
SUCCESS      = "#2ecc71"
WARNING      = "#f39c12"
DANGER       = "#e74c3c"
TEXT_PRIMARY = "#e8eaf0"
TEXT_MUTED   = "#6b7280"
FONT_MONO    = ("Consolas", 9)
FONT_UI      = ("Segoe UI", 10)
FONT_LABEL   = ("Segoe UI", 9)
FONT_TITLE   = ("Segoe UI Semibold", 11)

BASE_URL   = "https://api.talkdeskapp.com"


# ──────────────────────────────────────────────────────────────────────────────
# API LOGIC (pure functions, no UI dependencies)
# ──────────────────────────────────────────────────────────────────────────────

def build_token_url(company_name: str) -> str:
    """Build the OAuth token URL from the company subdomain."""
    return f"https://{company_name.strip()}.talkdeskid.com/oauth/token"


def split_date_range(start: datetime, end: datetime,
                     max_days: int = 31) -> list[tuple[datetime, datetime]]:
    """
    Split a date range into consecutive chunks of at most max_days each.
    The final chunk always ends exactly at `end`, so no data is missed.
    """
    chunks = []
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + timedelta(days=max_days), end)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    return chunks


def load_credentials(filepath: str) -> dict:
    """Load client credentials from a JSON file. Tolerates both dicts and lists."""
    with open(filepath, "r") as f:
        data = json.load(f)
        # If the JSON is wrapped in a list [{}], extract the first element.
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return data


def get_access_token(client_id: str, client_secret: str, token_url: str) -> str:
    """Request a Bearer token using client credentials grant."""
    payload = {
        "grant_type": "client_credentials",
        "scope": "data-reports:read data-reports:write recordings:read",
    }
    response = requests.post(token_url, data=payload, auth=(client_id, client_secret), timeout=30)
    if response.status_code != 200:
        raise Exception(f"Token error {response.status_code}: {response.text}")
    return response.json().get("access_token")


def execute_report(headers: dict, start_dt: datetime, end_dt: datetime) -> str:
    """Submit a calls report job and return the job ID."""
    url = f"{BASE_URL}/data/reports/calls/jobs"
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    payload = {
        "name": "GUI_Recording_Extraction",
        "timezone": "UTC",
        "format": "csv",
        "timespan": {
            "from": start_dt.strftime(fmt),
            "to":   end_dt.strftime(fmt),
        },
    }
    response = requests.post(url, headers=headers, json=payload, timeout=30)
    if response.status_code not in [200, 201, 202]:
        raise Exception(f"Report submit error {response.status_code}: {response.text}")
    return response.json().get("job", {}).get("id")


def wait_for_report(headers: dict, job_id: str,
                    log_fn, stop_event: threading.Event, pause_event: threading.Event) -> None:
    """Poll report status until done, respecting pause/stop signals."""
    url = f"{BASE_URL}/data/reports/calls/jobs/{job_id}"
    log_fn(f"Waiting for report {job_id}…")
    while True:
        if stop_event.is_set():
            raise Exception("Process stopped by user.")
        while pause_event.is_set():
            time.sleep(0.5)
            if stop_event.is_set():
                raise Exception("Process stopped by user.")

        response = requests.get(url, headers=headers, allow_redirects=False, timeout=30)

        if response.status_code in [302, 303]:
            log_fn("Report ready (redirect detected).")
            return

        if response.status_code == 204:
            time.sleep(5)
            continue

        response.raise_for_status()
        try:
            data = response.json()
        except ValueError:
            raise Exception("Invalid server response while polling report.")

        status = data.get("job", {}).get("status", "")
        if status == "done":
            log_fn("Report complete.")
            return
        elif status in ["failed", "error", "canceled", "deleted"]:
            raise Exception(f"Report ended with status: {status}")

        time.sleep(5)


def fetch_report_rows(headers: dict, job_id: str) -> list[dict]:
    """Download the report CSV and return all rows as a list of dicts."""
    url = f"{BASE_URL}/data/reports/calls/files/{job_id}"
    response = requests.get(url, headers=headers, allow_redirects=False, timeout=60)

    if response.status_code in [302, 303] and "Location" in response.headers:
        file_response = requests.get(response.headers["Location"], timeout=60)
        file_response.raise_for_status()
        csv_data = file_response.text
    else:
        response.raise_for_status()
        csv_data = response.text

    reader = csv.DictReader(io.StringIO(csv_data))
    return list(reader)


def get_recording_ids(headers: dict, interaction_id: str) -> list[str]:
    """Return all recording IDs for a given call interaction."""
    url = f"{BASE_URL}/calls/{interaction_id}/recordings"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    recordings = response.json().get("_embedded", {}).get("recordings", [])
    return [rec.get("id") for rec in recordings if rec.get("id")]


def download_recording_metadata(headers: dict, recording_id: str, folder: str) -> None:
    """Download and save recording metadata JSON."""
    url = f"{BASE_URL}/recordings/{recording_id}"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    filepath = os.path.join(folder, f"metadata_{recording_id}.json")
    with open(filepath, "w") as f:
        json.dump(response.json(), f, indent=4)


def download_recording_media(headers: dict, recording_id: str, folder: str) -> str:
    """
    Download the audio file for a recording and return the saved filename.
    The Talkdesk CDN always serves MP3 regardless of Accept headers, so the
    file is always saved as .mp3.
    """
    url = f"{BASE_URL}/recordings/{recording_id}/media"

    response = requests.get(url, headers=headers, stream=True,
                            allow_redirects=False, timeout=30)

    if response.status_code in [302, 303] and "Location" in response.headers:
        # Follow the CDN / S3 redirect — no auth headers needed, URL is pre-signed.
        response = requests.get(
            response.headers["Location"], stream=True, timeout=60,
        )
    else:
        response.raise_for_status()

    filename = f"audio_{recording_id}.mp3"
    filepath = os.path.join(folder, filename)

    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    return filename


# ──────────────────────────────────────────────────────────────────────────────
# MANIFEST HELPERS
# ──────────────────────────────────────────────────────────────────────────────

MANIFEST_FIELDS = ["call_id", "recording_url", "recording_ids", "downloaded"]


def manifest_path(start: datetime, end: datetime) -> str:
    """Return the canonical path for the interactions manifest file."""
    s = start.strftime("%Y%m%d")
    e = end.strftime("%Y%m%d")
    return f"Interactions to download {s}-{e}.csv"


def load_manifest(path: str) -> dict[str, set[str]]:
    """
    Load the manifest and return a dict of:
        call_id → set of recording_ids that were saved on disk.
    """
    if not os.path.exists(path):
        return {}
    result: dict[str, set[str]] = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("downloaded") == "yes":
                raw = row.get("recording_ids", "").strip()
                rec_ids = set(r for r in raw.split("|") if r) if raw else set()
                result[row["call_id"]] = rec_ids
    return result


def save_manifest(path: str, rows: list[dict]) -> None:
    """Write the full manifest file (all interactions, regardless of status)."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def scan_existing_recordings(folder: str) -> set[str]:
    """
    Return a set of recording IDs already present on disk by scanning
    filenames of the form  audio_<recording_id>.<ext>.
    """
    ids = set()
    if not os.path.isdir(folder):
        return ids
    for name in os.listdir(folder):
        if name.startswith("audio_"):
            stem = os.path.splitext(name)[0]
            rec_id = stem[len("audio_"):]
            if rec_id:
                ids.add(rec_id)
    return ids


# ──────────────────────────────────────────────────────────────────────────────
# FALLBACK DATE PICKER (when tkcalendar is not installed)
# ──────────────────────────────────────────────────────────────────────────────

class SimpleDateEntry(tk.Frame):
    """Minimal YYYY-MM-DD text entry used when tkcalendar is unavailable."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, bg=PANEL_BG)
        self._var = tk.StringVar(value=kwargs.get("default", datetime.now().strftime("%Y-%m-%d")))
        entry = tk.Entry(
            self, textvariable=self._var, width=12,
            bg="#252837", fg=TEXT_PRIMARY, insertbackground=TEXT_PRIMARY,
            relief="flat", font=FONT_UI,
        )
        entry.pack(padx=2, pady=2)

    def get_date(self) -> datetime:
        try:
            return datetime.strptime(self._var.get().strip(), "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: '{self._var.get()}'. Use YYYY-MM-DD.")


def _mark_done(manifest_rows: list[dict], call_id: str,
               rec_ids: list[str] | None = None) -> None:
    """
    Set downloaded='yes' for the given call_id in the in-memory manifest list.
    """
    for row in manifest_rows:
        if row.get("call_id") == call_id:
            row["downloaded"] = "yes"
            if rec_ids is not None:
                row["recording_ids"] = "|".join(rec_ids)
            return


# ──────────────────────────────────────────────────────────────────────────────
# MAIN APPLICATION
# ──────────────────────────────────────────────────────────────────────────────

class ExtractorApp(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("Talkdesk Recording Extractor")
        self.configure(bg=DARK_BG)
        self.resizable(True, True)
        self.minsize(720, 620)

        # Threading control
        self._thread: threading.Thread | None = None
        self._stop_event  = threading.Event()
        self._pause_event = threading.Event()

        # SSL certificate path — None means verification is disabled
        self._cert_path: str | None = None

        self._build_ui()
        self._set_button_state("idle")

    # ── UI CONSTRUCTION ──────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Header ──
        header = tk.Frame(self, bg=DARK_BG)
        header.pack(fill="x", padx=20, pady=(18, 4))
        tk.Label(
            header, text="TALKDESK  RECORDING  EXTRACTOR",
            bg=DARK_BG, fg=ACCENT, font=("Consolas", 13, "bold"), anchor="w",
        ).pack(side="left")
        tk.Label(
            header, text="v2.5",
            bg=DARK_BG, fg=TEXT_MUTED, font=FONT_MONO, anchor="e",
        ).pack(side="right", padx=4)

        sep = tk.Frame(self, bg=BORDER_COLOR, height=1)
        sep.pack(fill="x", padx=20, pady=(4, 14))

        # ── Config card ──
        card = self._card(self, "Configuration")
        card.pack(fill="x", padx=20, pady=(0, 12))

        # Company name
        row0 = tk.Frame(card, bg=PANEL_BG)
        row0.pack(fill="x", padx=16, pady=(0, 10))
        tk.Label(row0, text="Company Name", bg=PANEL_BG, fg=TEXT_MUTED,
                 font=FONT_LABEL, width=16, anchor="w").pack(side="left")
        self._company_var = tk.StringVar()
        company_entry = tk.Entry(
            row0, textvariable=self._company_var,
            bg="#252837", fg=TEXT_PRIMARY, insertbackground=TEXT_PRIMARY,
            relief="flat", font=FONT_UI, width=22,
        )
        company_entry.pack(side="left", padx=(0, 8))
        tk.Label(row0, text="→  token URL will use  https://<name>.talkdeskid.com/oauth/token",
                 bg=PANEL_BG, fg=TEXT_MUTED, font=FONT_LABEL).pack(side="left")

        # Credentials file
        row1 = tk.Frame(card, bg=PANEL_BG)
        row1.pack(fill="x", padx=16, pady=(0, 10))
        tk.Label(row1, text="Credentials File", bg=PANEL_BG, fg=TEXT_MUTED,
                 font=FONT_LABEL, width=16, anchor="w").pack(side="left")
        self._creds_var = tk.StringVar(value="No file selected")
        tk.Label(row1, textvariable=self._creds_var, bg=PANEL_BG, fg=TEXT_PRIMARY,
                 font=FONT_LABEL, anchor="w", width=38).pack(side="left")
        self._btn(row1, "Browse…", self._browse_credentials, width=9).pack(side="left", padx=(6, 0))

        # Date range
        row2 = tk.Frame(card, bg=PANEL_BG)
        row2.pack(fill="x", padx=16, pady=(0, 10))
        tk.Label(row2, text="Date Range", bg=PANEL_BG, fg=TEXT_MUTED,
                 font=FONT_LABEL, width=16, anchor="w").pack(side="left")

        now   = datetime.now()
        week_ago = now - timedelta(days=7)

        if HAS_TKCALENDAR:
            cal_opts = dict(
                background=PANEL_BG, foreground=TEXT_PRIMARY,
                headersbackground=ACCENT, headersforeground="#fff",
                selectbackground=ACCENT, selectforeground="#fff",
                normalbackground="#252837", normalforeground=TEXT_PRIMARY,
                weekendbackground="#252837", weekendforeground=TEXT_PRIMARY,
                othermonthbackground=DARK_BG, othermonthforeground=TEXT_MUTED,
                bordercolor=BORDER_COLOR, date_pattern="yyyy-mm-dd",
                font=FONT_LABEL,
            )
            self._start_picker = DateEntry(row2, **cal_opts)
            self._start_picker.set_date(week_ago)
            self._start_picker.pack(side="left", padx=(0, 6))
            tk.Label(row2, text="to", bg=PANEL_BG, fg=TEXT_MUTED, font=FONT_LABEL).pack(side="left", padx=4)
            self._end_picker = DateEntry(row2, **cal_opts)
            self._end_picker.set_date(now)
            self._end_picker.pack(side="left", padx=(0, 6))
        else:
            # Fallback plain text entries
            self._start_picker = SimpleDateEntry(row2, default=week_ago.strftime("%Y-%m-%d"))
            self._start_picker.pack(side="left", padx=(0, 6))
            tk.Label(row2, text="to", bg=PANEL_BG, fg=TEXT_MUTED, font=FONT_LABEL).pack(side="left", padx=4)
            self._end_picker = SimpleDateEntry(row2, default=now.strftime("%Y-%m-%d"))
            self._end_picker.pack(side="left", padx=(0, 6))
            tk.Label(row2, text="(YYYY-MM-DD)", bg=PANEL_BG, fg=TEXT_MUTED, font=FONT_LABEL).pack(side="left")

        # SSL certificate (optional) — disabled by default
        row4 = tk.Frame(card, bg=PANEL_BG)
        row4.pack(fill="x", padx=16, pady=(0, 12))
        tk.Label(row4, text="SSL Certificate", bg=PANEL_BG, fg=TEXT_MUTED,
                 font=FONT_LABEL, width=16, anchor="w").pack(side="left")

        self._ssl_enabled_var = tk.BooleanVar(value=False)
        ssl_chk = tk.Checkbutton(
            row4, text="Enable", variable=self._ssl_enabled_var,
            command=self._on_ssl_toggle,
            bg=PANEL_BG, fg=TEXT_PRIMARY, selectcolor="#252837",
            activebackground=PANEL_BG, activeforeground=TEXT_PRIMARY,
            font=FONT_LABEL, cursor="hand2",
        )
        ssl_chk.pack(side="left", padx=(0, 10))

        self._cert_label_var = tk.StringVar(value="No certificate selected")
        self._cert_label = tk.Label(
            row4, textvariable=self._cert_label_var,
            bg=PANEL_BG, fg=TEXT_MUTED, font=FONT_LABEL, anchor="w", width=36,
        )
        self._cert_label.pack(side="left")

        self._btn_browse_cert = self._btn(row4, "Browse…", self._browse_cert, width=9)
        self._btn_browse_cert.pack(side="left", padx=(6, 0))
        self._btn_browse_cert.config(state="disabled")   # disabled until checkbox is ticked

        prog_card = self._card(self, "Progress")
        prog_card.pack(fill="x", padx=20, pady=(0, 12))

        prog_inner = tk.Frame(prog_card, bg=PANEL_BG)
        prog_inner.pack(fill="x", padx=16, pady=(0, 10))

        self._status_var = tk.StringVar(value="Idle — configure settings and press Start.")
        tk.Label(prog_inner, textvariable=self._status_var, bg=PANEL_BG,
                 fg=TEXT_PRIMARY, font=FONT_LABEL, anchor="w").pack(fill="x", pady=(0, 8))

        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure(
            "Dark.Horizontal.TProgressbar",
            troughcolor=DARK_BG, background=ACCENT,
            lightcolor=ACCENT, darkcolor=ACCENT_DARK,
            bordercolor=BORDER_COLOR, thickness=14,
        )
        self._progress_var = tk.DoubleVar(value=0.0)
        self._progressbar = ttk.Progressbar(
            prog_inner, variable=self._progress_var,
            maximum=100.0, style="Dark.Horizontal.TProgressbar",
        )
        self._progressbar.pack(fill="x", pady=(0, 6))

        self._pct_var = tk.StringVar(value="0%")
        tk.Label(prog_inner, textvariable=self._pct_var, bg=PANEL_BG,
                 fg=TEXT_MUTED, font=FONT_LABEL).pack(anchor="e")

        # ── Control buttons ──
        ctrl = tk.Frame(self, bg=DARK_BG)
        ctrl.pack(fill="x", padx=20, pady=(0, 14))

        self._btn_start  = self._btn(ctrl, "▶  Start",  self._on_start,  bg=SUCCESS,  hover="#27ae60")
        self._btn_pause  = self._btn(ctrl, "⏸  Pause",  self._on_pause,  bg=WARNING,  hover="#e67e22")
        self._btn_stop   = self._btn(ctrl, "■  Stop",   self._on_stop,   bg=DANGER,   hover="#c0392b")
        self._btn_start.pack(side="left", padx=(0, 8))
        self._btn_pause.pack(side="left", padx=(0, 8))
        self._btn_stop.pack(side="left")

        # ── Log card ──
        log_card = self._card(self, "Activity Log")
        log_card.pack(fill="both", expand=True, padx=20, pady=(0, 18))

        log_frame = tk.Frame(log_card, bg=DARK_BG)
        log_frame.pack(fill="both", expand=True, padx=16, pady=(0, 12))

        scrollbar = tk.Scrollbar(log_frame, bg=BORDER_COLOR, troughcolor=DARK_BG,
                                 activebackground=ACCENT, relief="flat")
        scrollbar.pack(side="right", fill="y")

        self._log = tk.Text(
            log_frame,
            bg=DARK_BG, fg=TEXT_PRIMARY, font=FONT_MONO,
            relief="flat", wrap="word", state="disabled",
            yscrollcommand=scrollbar.set,
            selectbackground=ACCENT, insertbackground=TEXT_PRIMARY,
            padx=6, pady=4,
        )
        self._log.pack(fill="both", expand=True)
        scrollbar.config(command=self._log.yview)

        # Tag colours for log levels
        self._log.tag_config("info",    foreground=TEXT_PRIMARY)
        self._log.tag_config("success", foreground=SUCCESS)
        self._log.tag_config("warning", foreground=WARNING)
        self._log.tag_config("error",   foreground=DANGER)
        self._log.tag_config("muted",   foreground=TEXT_MUTED)
        self._log.tag_config("accent",  foreground=ACCENT)

        if not HAS_TKCALENDAR:
            self._log_msg(
                "⚠  tkcalendar not found. Using plain text date fields (YYYY-MM-DD). "
                "Install with:  pip install tkcalendar", "warning"
            )

    # ── WIDGET HELPERS ───────────────────────────────────────────────────────

    def _card(self, parent, title: str) -> tk.Frame:
        """Create a titled panel card."""
        outer = tk.Frame(parent, bg=PANEL_BG, bd=1, relief="flat",
                         highlightbackground=BORDER_COLOR, highlightthickness=1)
        tk.Label(outer, text=f"  {title.upper()}  ", bg=PANEL_BG, fg=TEXT_MUTED,
                 font=("Consolas", 8, "bold")).pack(anchor="w", padx=14, pady=(10, 6))
        sep = tk.Frame(outer, bg=BORDER_COLOR, height=1)
        sep.pack(fill="x", padx=14, pady=(0, 10))
        return outer

    def _btn(self, parent, text: str, command, width=10,
             bg=ACCENT, hover=ACCENT_DARK) -> tk.Button:
        """Create a styled button with hover effect."""
        b = tk.Button(
            parent, text=text, command=command,
            bg=bg, fg="#ffffff", activebackground=hover, activeforeground="#fff",
            relief="flat", font=("Segoe UI Semibold", 9),
            padx=12, pady=6, width=width, cursor="hand2",
        )
        b.bind("<Enter>", lambda e: b.config(bg=hover))
        b.bind("<Leave>", lambda e: b.config(bg=bg))
        return b

    # ── BROWSE ───────────────────────────────────────────────────────────────

    def _browse_credentials(self):
        path = filedialog.askopenfilename(
            title="Select Credentials File",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if path:
            self._creds_path = path
            self._creds_var.set(os.path.basename(path))

    def _browse_cert(self):
        """Open a file dialog to select an SSL CA bundle / certificate file."""
        path = filedialog.askopenfilename(
            title="Select SSL Certificate File",
            filetypes=[
                ("Certificate files", "*.pem *.crt *.cer"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self._cert_path = path
            self._cert_label_var.set(os.path.basename(path))
            self._cert_label.config(fg=TEXT_PRIMARY)

    def _on_ssl_toggle(self):
        """Enable or disable the SSL certificate Browse button based on the checkbox."""
        if self._ssl_enabled_var.get():
            self._btn_browse_cert.config(state="normal")
        else:
            # Checkbox unchecked — clear any previously selected certificate
            self._cert_path = None
            self._cert_label_var.set("No certificate selected")
            self._cert_label.config(fg=TEXT_MUTED)
            self._btn_browse_cert.config(state="disabled")

    # ── LOG ──────────────────────────────────────────────────────────────────

    def _log_msg(self, message: str, level: str = "info"):
        """Append a timestamped message to the log (thread-safe)."""
        def _append():
            ts  = datetime.now().strftime("%H:%M:%S")
            self._log.config(state="normal")
            self._log.insert("end", f"[{ts}] ", "muted")
            self._log.insert("end", message + "\n", level)
            self._log.see("end")
            self._log.config(state="disabled")
        self.after(0, _append)

    # ── PROGRESS ─────────────────────────────────────────────────────────────

    def _set_progress(self, pct: float, status: str = ""):
        def _update():
            self._progress_var.set(pct)
            self._pct_var.set(f"{pct:.0f}%")
            if status:
                self._status_var.set(status)
        self.after(0, _update)

    # ── BUTTON STATES ────────────────────────────────────────────────────────

    def _set_button_state(self, state: str):
        """Manage button enable/disable based on run state."""
        def _apply():
            states = {
                "idle":    dict(start="normal",   pause="disabled", stop="disabled"),
                "running": dict(start="disabled", pause="normal",   stop="normal"),
                "paused":  dict(start="disabled", pause="normal",   stop="normal"),
                "done":    dict(start="normal",   pause="disabled", stop="disabled"),
            }
            cfg = states.get(state, states["idle"])
            self._btn_start.config(state=cfg["start"])
            self._btn_pause.config(state=cfg["pause"])
            self._btn_stop.config(state=cfg["stop"])
            if state == "paused":
                self._btn_pause.config(text="▶  Resume")
            else:
                self._btn_pause.config(text="⏸  Pause")
        self.after(0, _apply)

    # ── CONTROL HANDLERS ─────────────────────────────────────────────────────

    def _on_start(self):
        # Validate inputs
        company = self._company_var.get().strip()
        if not company:
            messagebox.showerror("Missing Input", "Please enter a Company Name.")
            return

        if not hasattr(self, "_creds_path") or not self._creds_path:
            messagebox.showerror("Missing Input", "Please select a Credentials File.")
            return

        try:
            start_dt = self._get_start_date()
            end_dt   = self._get_end_date()
        except ValueError as e:
            messagebox.showerror("Date Error", str(e))
            return

        if end_dt <= start_dt:
            messagebox.showerror("Date Error", "End Date must be after Start Date.")
            return

        # Reset controls
        self._stop_event.clear()
        self._pause_event.clear()
        self._log.config(state="normal")
        self._log.delete("1.0", "end")
        self._log.config(state="disabled")
        self._set_progress(0, "Starting…")
        self._set_button_state("running")

        # Apply the SSL verification setting for this run.
        # If the checkbox is enabled and a cert was selected, use that path;
        # otherwise keep verification disabled (False) for corporate proxy compat.
        global _SSL_VERIFY
        if self._ssl_enabled_var.get() and self._cert_path:
            _SSL_VERIFY = self._cert_path
        else:
            _SSL_VERIFY = False

        self._thread = threading.Thread(
            target=self._run_extraction,
            args=(company, self._creds_path, start_dt, end_dt),
            daemon=True,
        )
        self._thread.start()

    def _on_pause(self):
        if self._pause_event.is_set():
            self._pause_event.clear()
            self._log_msg("Resumed.", "success")
            self._set_button_state("running")
            self._status_var.set("Running…")
        else:
            self._pause_event.set()
            self._log_msg("Paused — press Resume to continue.", "warning")
            self._set_button_state("paused")
            self._status_var.set("Paused.")

    def _on_stop(self):
        self._stop_event.set()
        self._pause_event.clear()   # unblock the pause loop so it can exit
        self._log_msg("Stop requested — finishing current operation…", "warning")
        self._status_var.set("Stopping…")

    # ── DATE HELPERS ─────────────────────────────────────────────────────────

    def _get_start_date(self) -> datetime:
        if HAS_TKCALENDAR:
            d = self._start_picker.get_date()
            return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
        else:
            d = self._start_picker.get_date()
            return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)

    def _get_end_date(self) -> datetime:
        if HAS_TKCALENDAR:
            d = self._end_picker.get_date()
            return datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)
        else:
            d = self._end_picker.get_date()
            return datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)

    # ── EXTRACTION THREAD ────────────────────────────────────────────────────

    def _run_extraction(self, company: str, creds_path: str,
                        start_dt: datetime, end_dt: datetime):
        try:
            folder  = "recordings"
            mf_path = manifest_path(start_dt, end_dt)

            # Log the active SSL verification mode so the user can confirm it.
            if _SSL_VERIFY is False:
                self._log_msg("SSL verification: disabled (no certificate).", "warning")
            else:
                self._log_msg(f"SSL certificate: {os.path.basename(_SSL_VERIFY)}", "success")

            # ── STEP 1: Early disk scan ────────────────────────────────────
            os.makedirs(folder, exist_ok=True)
            existing_rec_ids = scan_existing_recordings(folder)
            self._log_msg(
                f"Disk pre-scan: {len(existing_rec_ids)} audio file(s) in '{folder}/'.",
                "muted",
            )

            # ── STEP 2: Load existing manifest (if any) ───────────────────
            manifest_rows: list[dict] = []
            calls_to_process: list[str] = []
            need_report = True

            if os.path.exists(mf_path):
                self._log_msg(f"Found existing manifest: {mf_path}", "accent")
                done_map = load_manifest(mf_path)  # call_id → set(rec_ids)

                with open(mf_path, newline="") as f:
                    manifest_rows = list(csv.DictReader(f))

                for mrow in manifest_rows:
                    mrow.setdefault("recording_ids", "")

                skipped = 0
                requeued = 0
                for mrow in manifest_rows:
                    call_id = mrow.get("call_id", "").strip()
                    rec_url = mrow.get("recording_url", "").strip()
                    if not call_id or not rec_url:
                        continue

                    if call_id in done_map:
                        stored_ids = done_map[call_id]
                        if stored_ids and not stored_ids.issubset(existing_rec_ids):
                            missing = stored_ids - existing_rec_ids
                            self._log_msg(
                                f"  ⚠  Call {call_id}: {len(missing)} file(s) missing "
                                f"from disk — re-queuing.", "warning"
                            )
                            mrow["downloaded"] = "no"
                            calls_to_process.append(call_id)
                            requeued += 1
                        elif not stored_ids:
                            skipped += 1
                        else:
                            skipped += 1
                    else:
                        calls_to_process.append(call_id)

                self._log_msg(
                    f"Manifest: {skipped} confirmed done, "
                    f"{len(calls_to_process) - requeued} pending, "
                    f"{requeued} re-queued (missing files).", "info"
                )

                if calls_to_process:
                    need_report = False
                else:
                    self._set_progress(100, "Up-to-date — all recordings already on disk.")
                    self._log_msg(
                        "Nothing to download. Delete the manifest to force a full re-run.",
                        "success",
                    )
                    self._set_button_state("done")
                    return

            # ── STEP 3: Authenticate ───────────────────────────────────────
            self._log_msg(f"Company: {company}", "accent")
            token_url = build_token_url(company)
            self._log_msg(f"Token URL: {token_url}", "muted")
            self._set_progress(3, "Authenticating…")

            creds = load_credentials(creds_path)
            token = get_access_token(creds.get("id"), creds.get("secret"), token_url)
            api_headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            self._log_msg("Authentication successful.", "success")

            # ── STEP 4 (conditional): Chunked report submission ────────────
            if need_report:
                chunks = split_date_range(start_dt, end_dt, max_days=31)
                total_chunks = len(chunks)
                self._log_msg(
                    f"Date range split into {total_chunks} chunk(s) of ≤31 days.", "info"
                )

                all_rows: list[dict] = []
                seen_call_ids: set[str] = set()

                for ci, (cs, ce) in enumerate(chunks, start=1):
                    if self._stop_event.is_set():
                        raise Exception("Process stopped by user.")
                    while self._pause_event.is_set():
                        time.sleep(0.5)
                        if self._stop_event.is_set():
                            raise Exception("Process stopped by user.")

                    chunk_pct = 5 + (ci - 1) / total_chunks * 20
                    label = f"{cs.strftime('%Y-%m-%d')} → {ce.strftime('%Y-%m-%d')}"
                    self._set_progress(chunk_pct, f"[Chunk {ci}/{total_chunks}] Submitting report…")
                    self._log_msg(f"[Chunk {ci}/{total_chunks}] {label}", "accent")

                    job_id = execute_report(api_headers, cs, ce)
                    self._log_msg(f"  Job ID: {job_id}", "muted")

                    self._set_progress(chunk_pct + 5 / total_chunks,
                                       f"[Chunk {ci}/{total_chunks}] Waiting…")
                    wait_for_report(api_headers, job_id, self._log_msg,
                                    self._stop_event, self._pause_event)

                    rows = fetch_report_rows(api_headers, job_id)
                    self._log_msg(f"  {len(rows)} row(s) in this chunk.", "muted")

                    for row in rows:
                        cid = row.get("Call Id", "").strip()
                        if cid and cid not in seen_call_ids:
                            seen_call_ids.add(cid)
                            all_rows.append(row)

                self._log_msg(
                    f"All chunks done. Total unique calls: {len(all_rows)}.", "info"
                )

                manifest_rows = []
                calls_to_process = []
                for row in all_rows:
                    call_id = row.get("Call Id", "").strip()
                    rec_url = row.get("Recording Url", "").strip()
                    if not call_id:
                        continue
                    manifest_rows.append({
                        "call_id":       call_id,
                        "recording_url": rec_url,
                        "recording_ids": "",
                        "downloaded":    "no",
                    })
                    if rec_url:
                        calls_to_process.append(call_id)

                save_manifest(mf_path, manifest_rows)
                self._log_msg(
                    f"Manifest created: {mf_path}  "
                    f"({len(calls_to_process)} call(s) with recordings).", "muted"
                )

            # ── STEP 5: Download loop ──────────────────────────────────────
            total = len(calls_to_process)
            self._log_msg(f"Processing {total} call(s)…", "info")

            if total == 0:
                self._log_msg("No calls with recordings found in this date range.", "warning")
                self._set_progress(100, "Done — no recordings to download.")
                self._set_button_state("done")
                return

            for idx, call_id in enumerate(calls_to_process, start=1):
                if self._stop_event.is_set():
                    self._log_msg("Process stopped by user.", "warning")
                    break
                while self._pause_event.is_set():
                    time.sleep(0.5)
                    if self._stop_event.is_set():
                        break

                pct = 25 + (idx - 1) / total * 73
                self._set_progress(pct, f"Processing call {idx}/{total}…")
                self._log_msg(f"[{idx}/{total}] Call ID: {call_id}", "accent")

                try:
                    rec_ids = get_recording_ids(api_headers, call_id)
                except Exception as e:
                    self._log_msg(f"  Could not fetch recording list: {e}", "warning")
                    continue

                if not rec_ids:
                    self._log_msg("  No recordings returned for this call.", "muted")
                    _mark_done(manifest_rows, call_id, rec_ids=[])
                    save_manifest(mf_path, manifest_rows)
                    continue

                for rec_id in rec_ids:
                    if rec_id in existing_rec_ids:
                        self._log_msg(
                            f"  ↩  {rec_id} — already on disk, skipping.", "muted"
                        )
                        continue

                    try:
                        download_recording_metadata(api_headers, rec_id, folder)
                        fname = download_recording_media(api_headers, rec_id, folder)
                        existing_rec_ids.add(rec_id)
                        self._log_msg(f"  ✓  {fname}", "success")
                    except Exception as e:
                        self._log_msg(f"  ✗  Error for {rec_id}: {e}", "error")

                _mark_done(manifest_rows, call_id, rec_ids=rec_ids)
                save_manifest(mf_path, manifest_rows)

            if not self._stop_event.is_set():
                self._set_progress(100, "Extraction complete.")
                self._log_msg("All done! ✓", "success")
            else:
                self._set_progress(self._progress_var.get(), "Stopped by user.")

        except Exception as e:
            self._log_msg(f"Fatal error: {e}", "error")
            self.after(0, lambda: self._status_var.set(f"Error: {e}"))

        finally:
            self._set_button_state("done")


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = ExtractorApp()
    app.mainloop()