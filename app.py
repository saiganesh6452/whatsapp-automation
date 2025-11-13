#!/usr/bin/env python3
"""
app.py - updated: robust Valura auth attempts and Contacts writeback.

Key improvements:
 - Uses jobs envelope {"jobs":[...]} matching working Node code.
 - Default header: x-api-key; fallback: Basic auth attempts.
 - DRY_RUN env to skip actual sending.
 - Normalized name matching and Contacts sheet updates (Message, Send Status, Last Sent).
 - Detailed logging of Valura response body on failures (helps debug 401).
 - Accumulates multiple generated messages per BU/name and writes them into the same cell.
 - Normalizes phone numbers and prepends default country code when missing.
 - Safe sheet writes: backups + truncation + offload of very long cells to LongMessages_<tag>.
"""

import os, sys, json, time, base64, logging, random, threading, tempfile, re
from typing import List, Dict, Any, Optional
from functools import wraps
from datetime import datetime
import requests
from collections import defaultdict

# --- safe gspread call helper for 429s ----
import gspread

def safe_gspread_call(fn, *a, max_attempts=6, initial_delay=1.0, backoff=2.0, max_delay=60.0, **kw):
    """
    Call a gspread function (fn) and retry on APIError 429 (rate-limit) with backoff + jitter.
    Example: safe_gspread_call(ws.update, payload)
    """
    attempt = 0
    delay = initial_delay
    while True:
        attempt += 1
        try:
            return fn(*a, **kw)
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if '429' in msg or 'quota' in msg.lower() or 'RATE_LIMIT_EXCEEDED' in msg:
                if attempt >= max_attempts:
                    logger.exception("gspread APIError 429: giving up after %d attempts", attempt)
                    raise
                jitter = random.uniform(0.5, 1.5)
                sleep_for = min(max_delay, delay * jitter)
                logger.warning("Rate-limited by Google Sheets (attempt %d/%d). Sleeping %.1fs before retry.", attempt, max_attempts, sleep_for)
                time.sleep(sleep_for)
                delay *= backoff
                continue
            raise
        except Exception:
            raise


# auto-load .env if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# third-party
from google.oauth2.service_account import Credentials
try:
    import openai
except Exception:
    openai = None
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone as pytz_timezone

try:
    from tqdm import tqdm
except Exception:
    tqdm = lambda x, **k: x

# ---------- logging ----------
LOG_DIR = os.environ.get("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "auto_whatsapp.log")
logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s - %(message)s",
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("auto_whatsapp")

# ---------- env & config ----------
VALURA_BACKEND_URL = os.environ.get("VALURA_BACKEND_URL")
VALURA_API_KEY = os.environ.get("VALURA_API_KEY")
VALURA_AUTH_HEADER = os.environ.get("VALURA_AUTH_HEADER")
DRY_RUN = str(os.environ.get("DRY_RUN", "false")).lower() in ("1","true","yes")

GOOGLE_SA_BASE64 = os.environ.get("GOOGLE_SA_BASE64")
GOOGLE_SA_BASE64_FILE = os.environ.get("GOOGLE_SA_BASE64_FILE")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID")
CONTACTS_TAB = os.environ.get("CONTACTS_TAB", "Contacts")

TABS_CONFIG_ENV = os.environ.get("TABS_CONFIG")
TABS_CONFIG_FILE = os.environ.get("TABS_CONFIG_FILE")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
if OPENAI_API_KEY and openai:
    try:
        if hasattr(openai, "api_key"):
            openai.api_key = OPENAI_API_KEY
    except Exception:
        pass

# Default country code to use when phone does not start with '+'
DEFAULT_COUNTRY_CODE = os.environ.get("DEFAULT_COUNTRY_CODE", "+91")

def ms_to_sec(env_key: str, default_ms: int) -> float:
    val = os.environ.get(env_key)
    if val is None:
        return default_ms/1000.0
    try:
        return float(val)/1000.0
    except:
        return default_ms/1000.0

SHEET_WRITE_DELAY_SEC = ms_to_sec("SHEET_WRITE_DELAY_MS", 500)
SHEETS_BATCH_CHUNK_SIZE = int(os.environ.get("SHEETS_BATCH_CHUNK_SIZE", "0") or 0)
SHEETS_BATCH_DELAY_SEC = ms_to_sec("SHEETS_BATCH_DELAY_MS", 2500)
REPORT_CHUNK_SIZE = int(os.environ.get("REPORT_CHUNK_SIZE", "0") or 0)
REPORT_WRITE_DELAY_SEC = ms_to_sec("REPORT_WRITE_DELAY_MS", 3000)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "25"))
DELAY_BETWEEN_BATCH_SEC = ms_to_sec("DELAY_BETWEEN_BATCH_MS", 1000)

WHATSAPP_MIN_DELAY_SEC = ms_to_sec("WHATSAPP_MIN_DELAY_MS", 12000)
WHATSAPP_MAX_DELAY_SEC = ms_to_sec("WHATSAPP_MAX_DELAY_MS", 15000)
if WHATSAPP_MIN_DELAY_SEC > WHATSAPP_MAX_DELAY_SEC:
    WHATSAPP_MIN_DELAY_SEC, WHATSAPP_MAX_DELAY_SEC = WHATSAPP_MAX_DELAY_SEC, WHATSAPP_MIN_DELAY_SEC

TIMEZONE = os.environ.get("DEFAULT_TIMEZONE", "Asia/Kolkata")
TABS_CONFIG_FILE_POLL_SECONDS = int(os.environ.get("TABS_CONFIG_FILE_POLL_SECONDS", "30"))

MAX_CONCURRENT_LLM = int(os.environ.get("MAX_CONCURRENT_LLM", "3"))
LLM_REQUEST_DELAY_SEC = float(os.environ.get("LLM_REQUEST_DELAY_SEC", "0.2"))

RUN_IMMEDIATE = str(os.environ.get("RUN_IMMEDIATE", "false")).lower() in ("1","true","yes")

# ---------- sanity ----------
if not VALURA_BACKEND_URL or not VALURA_API_KEY:
    logger.error("VALURA_BACKEND_URL and VALURA_API_KEY required.")
    raise SystemExit(1)
if not SPREADSHEET_ID:
    logger.error("SPREADSHEET_ID required.")
    raise SystemExit(1)
if not (GOOGLE_SA_BASE64 or GOOGLE_SA_BASE64_FILE or GOOGLE_APPLICATION_CREDENTIALS):
    logger.error("Google creds needed (GOOGLE_SA_BASE64 or GOOGLE_SA_BASE64_FILE or GOOGLE_APPLICATION_CREDENTIALS).")
    raise SystemExit(1)

# ---------- retry decorator ----------
def retry_backoff(max_attempts=5, initial_delay=1.0, backoff_factor=2.0, allowed_exceptions=(Exception,)):
    def deco(fn):
        @wraps(fn)
        def wrapped(*a, **kw):
            attempt = 0
            delay = initial_delay
            while True:
                attempt += 1
                try:
                    return fn(*a, **kw)
                except allowed_exceptions as e:
                    if attempt >= max_attempts:
                        logger.exception("Function %s failed after %d attempts.", fn.__name__, attempt)
                        raise
                    logger.warning("Error in %s: %s. Retrying in %.1fs (%d/%d)", fn.__name__, e, delay, attempt, max_attempts)
                    time.sleep(delay)
                    delay *= backoff_factor
        return wrapped
    return deco

# ---------- Google Sheets helpers ----------
def _read_text_file(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return None

def load_gspread_client():
    """
    Robust loader for Google service account credentials.
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = None

    def try_parse_json(text: str):
        try:
            return json.loads(text)
        except Exception:
            return None

    def write_tmp(name_prefix: str, text: str):
        try:
            tf = tempfile.NamedTemporaryFile(delete=False, suffix=".json", prefix=name_prefix)
            tf.write(text.encode("utf-8", errors="ignore"))
            tf.close()
            logger.info("Wrote debug temp file: %s (first 300 chars: %s)", tf.name, (text[:300].replace("\n","\\n")))
            return tf.name
        except Exception:
            logger.exception("Failed to write temp debug file.")
            return None

    def candidates_from_string(s: str):
        cand = []
        if not s:
            return cand
        s = s.strip()
        if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
            unq = s[1:-1]
            cand.append(("stripped-surrounding-quotes", unq))
        if s.startswith("{") or s.startswith("["):
            cand.append(("raw-json", s))
        if "\\n" in s:
            replaced = s.replace("\\n", "\n")
            cand.append(("replaced-\\\\n-with-newlines", replaced))
            s_for_b64 = replaced
        else:
            s_for_b64 = s
        try:
            dec = base64.b64decode(s_for_b64, validate=True).decode("utf-8")
            cand.append(("base64-decoded-once", dec))
            try:
                if all(ch in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r" for ch in dec.strip()):
                    dec2 = base64.b64decode(dec, validate=True).decode("utf-8")
                    cand.append(("base64-decoded-twice", dec2))
            except Exception:
                pass
        except Exception:
            pass
        if not cand:
            cand.append(("original-string", s))
        return cand

    b64 = GOOGLE_SA_BASE64
    if not b64 and GOOGLE_SA_BASE64_FILE:
        b64 = _read_text_file(GOOGLE_SA_BASE64_FILE)

    if b64:
        tried_any = False
        for tag, candidate in candidates_from_string(b64):
            tried_any = True
            sa = try_parse_json(candidate)
            if sa:
                try:
                    creds = Credentials.from_service_account_info(sa, scopes=scopes)
                    logger.info("Loaded Google creds from candidate: %s", tag)
                    break
                except Exception:
                    logger.exception("Credentials.from_service_account_info failed for candidate: %s", tag)
                    write_tmp("gsa_candidate_failed_", candidate)
            else:
                logger.warning("Candidate %s not valid JSON; saving for inspection.", tag)
                write_tmp("gsa_candidate_invalidjson_", candidate)

        if not tried_any:
            logger.warning("GOOGLE_SA_BASE64 was provided but no parsing candidates were generated.")
    if not creds and GOOGLE_APPLICATION_CREDENTIALS:
        try:
            creds = Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS, scopes=scopes)
            logger.info("Loaded Google creds from GOOGLE_APPLICATION_CREDENTIALS file path.")
        except Exception:
            logger.exception("Failed to load GOOGLE_APPLICATION_CREDENTIALS file.")
    if not creds and GOOGLE_SA_BASE64_FILE and not b64:
        file_text = _read_text_file(GOOGLE_SA_BASE64_FILE)
        if file_text:
            for tag, candidate in candidates_from_string(file_text):
                sa = try_parse_json(candidate)
                if sa:
                    try:
                        creds = Credentials.from_service_account_info(sa, scopes=scopes)
                        logger.info("Loaded Google creds from file-candidate: %s", tag)
                        break
                    except Exception:
                        logger.exception("Credentials.from_service_account_info failed for file-candidate: %s", tag)
                        write_tmp("gsa_file_candidate_failed_", candidate)
                else:
                    write_tmp("gsa_file_candidate_invalidjson_", candidate)

    if not creds:
        logger.error("Unable to obtain Google credentials from env/file/path. See temp debug files for attempted contents.")
        raise RuntimeError("No Google credentials available or parsing failed. Check GOOGLE_SA_BASE64, GOOGLE_SA_BASE64_FILE, or GOOGLE_APPLICATION_CREDENTIALS.")

    try:
        client = gspread.authorize(creds)
        logger.info("Authorized gspread client.")
        return client
    except Exception:
        logger.exception("gspread.authorize failed.")
        raise

gc = load_gspread_client()

@retry_backoff()
def open_spreadsheet_by_id(sid: str):
    return gc.open_by_key(sid)

@retry_backoff()
def create_or_get_worksheet(spreadsheet, title: str, rows: int = 1000, cols: int = 50):
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))

@retry_backoff()
def read_worksheet_as_dicts(spreadsheet, worksheet_title: str) -> List[Dict[str, Any]]:
    """
    Robust replacement for gspread.get_all_records() that tolerates duplicate headers.
    """
    try:
        ws = spreadsheet.worksheet(worksheet_title)
    except gspread.exceptions.WorksheetNotFound:
        return []

    all_vals = ws.get_all_values()
    if not all_vals:
        return []

    raw_headers = [str(h).strip() for h in all_vals[0]]
    rows = all_vals[1:] if len(all_vals) > 1 else []

    # make headers unique
    seen = {}
    unique_headers = []
    blank_index = 0
    duplicates = []
    for i, h in enumerate(raw_headers):
        if not h:
            blank_index += 1
            base = f"Column_{blank_index}"
        else:
            base = h
        count = seen.get(base, 0) + 1
        seen[base] = count
        if count == 1:
            unique = base
        else:
            unique = f"{base}_{count}"
            duplicates.append((base, count, i))
        unique_headers.append(unique)

    if duplicates:
        logger.warning("Duplicate headers detected in worksheet '%s': %s", worksheet_title, duplicates)

    out = []
    for r in rows:
        d = {}
        if len(r) < len(unique_headers):
            r = r + [""] * (len(unique_headers) - len(r))
        for idx, key in enumerate(unique_headers):
            d[key] = r[idx] if idx < len(r) else ""
        out.append(d)

    return out

@retry_backoff()
def write_rows_to_worksheet(spreadsheet, worksheet_title: str, rows: List[Dict[str, Any]]):
    if not rows:
        create_or_get_worksheet(spreadsheet, worksheet_title)
        return
    all_keys=[]
    for r in rows:
        for k in r.keys():
            if k not in all_keys:
                all_keys.append(k)
    ws = create_or_get_worksheet(spreadsheet, worksheet_title, rows=max(100,len(rows)+5), cols=max(10,len(all_keys)))
    chunk = SHEETS_BATCH_CHUNK_SIZE or 0
    if chunk <= 0 or len(rows) <= chunk:
        payload = [all_keys] + [[r.get(k,"") for k in all_keys] for r in rows]
        time.sleep(SHEETS_BATCH_DELAY_SEC)
        ws.clear()
        ws.update(payload)
        time.sleep(SHEET_WRITE_DELAY_SEC)
        logger.info("Wrote %d rows to %s", len(rows), worksheet_title)
        return
    total = len(rows)
    for i in range(0,total,chunk):
        sub = rows[i:i+chunk]
        payload = [all_keys] + [[r.get(k,"") for k in all_keys] for r in sub]
        time.sleep(SHEETS_BATCH_DELAY_SEC)
        if i==0:
            ws.clear(); ws.update(payload)
        else:
            ws.append_rows(payload[1:], value_input_option='USER_ENTERED')
        logger.info("Wrote chunk %d..%d to %s", i+1, min(i+chunk,total), worksheet_title)
        time.sleep(SHEET_WRITE_DELAY_SEC)

from gspread.exceptions import APIError

MAX_SHEET_CELL_CHARS = 50000
TRUNCATE_TO = 49000  # leave some room for our truncation suffix

def backup_sheet_to_local(ws, prefix="backup"):
    """Save a JSON backup of worksheet to local file for recovery."""
    try:
        vals = safe_gspread_call(ws.get_all_values)
        ts = int(time.time())
        fname = f"{prefix}_{ws.title.replace(' ','_')}_{ts}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump({"title": ws.title, "values": vals}, f, ensure_ascii=False, indent=2)
        logger.info("Saved sheet backup to %s (rows=%d)", fname, len(vals))
        return fname
    except Exception:
        logger.exception("Failed to backup sheet to local file.")
        return None

def _find_and_truncate_long_cells(headers, rows, max_chars=TRUNCATE_TO, preserve_suffix="... (truncated)"):
    """
    Returns (new_rows, long_cells) where new_rows is copy with cells longer than max_chars replaced
    by a truncated version, and long_cells is list of (r_idx, c_idx, original_text).
    """
    long_cells = []
    new_rows = []
    for r_idx, r in enumerate(rows):
        row_copy = r[:]
        for c_idx in range(len(row_copy)):
            val = str(row_copy[c_idx] or "")
            if len(val) > max_chars:
                long_cells.append((r_idx, c_idx, val))
                truncated = val[:max_chars] + preserve_suffix
                row_copy[c_idx] = truncated
        new_rows.append(row_copy)
    return new_rows, long_cells

def _write_long_cells_to_sheet(spreadsheet, report_tag, long_cells, headers):
    """
    Writes each long cell into a separate LongMessages_<report_tag> sheet so the full text is preserved.
    """
    try:
        title = f"LongMessages_{_sanitize_tab_for_col(report_tag)}"
        ws_long = create_or_get_worksheet(spreadsheet, title, rows=max(100, len(long_cells)+5), cols=6)
        payload = [["contact_row", "col_index", "header_name", "truncated_cell_preview", "full_text"]]
        for (r_idx, c_idx, text) in long_cells:
            contact_row_number = r_idx + 2  # rows start at 2 for body
            header_name = headers[c_idx] if c_idx < len(headers) else f"col_{c_idx}"
            preview = (text[:200] + "…") if len(text) > 200 else text
            payload.append([contact_row_number, c_idx, header_name, preview, text])
        safe_gspread_call(ws_long.clear)
        safe_gspread_call(ws_long.update, payload)
        logger.info("Saved %d long cells to %s", len(long_cells), title)
        return title
    except Exception:
        logger.exception("Failed to write long cells to LongMessages sheet.")
        return None

def safe_update_sheet_with_backup_and_truncation(spreadsheet, ws, headers, rows, report_tag="run"):
    """
    Safe update helper:
     - Backups existing sheet to local JSON
     - Attempts update as-is (no ws.clear())
     - If APIError about 50000 chars occurs, truncates offending cells and retries
     - If still failing, offloads full long texts into LongMessages_<report_tag> and writes truncated placeholders
    """
    try:
        backup_file = backup_sheet_to_local(ws, prefix=f"backup_{_sanitize_tab_for_col(report_tag)}")
    except Exception:
        backup_file = None

    norm_rows = []
    for r in rows:
        rr = r[:]
        if len(rr) < len(headers):
            rr += [""] * (len(headers) - len(rr))
        norm_rows.append(rr)

    payload = [headers] + norm_rows
    try:
        safe_gspread_call(ws.update, payload)
        logger.info("safe_update: wrote %d rows to %s successfully.", len(norm_rows), ws.title)
        return {"ok": True, "backup": backup_file, "truncated": False}
    except APIError as e:
        msg = str(e)
        logger.warning("safe_update: initial update failed: %s", msg)

    try:
        new_rows, long_cells = _find_and_truncate_long_cells(headers, norm_rows, max_chars=TRUNCATE_TO)
        if not long_cells:
            raise
        try:
            safe_gspread_call(ws.update, [headers] + new_rows)
            logger.info("safe_update: wrote with truncation (shortened %d cells).", len(long_cells))
            title = _write_long_cells_to_sheet(spreadsheet, report_tag, long_cells, headers)
            return {"ok": True, "backup": backup_file, "truncated": True, "long_sheet": title}
        except APIError as e2:
            logger.warning("safe_update: truncation retry failed: %s", e2)
            offloaded_rows = new_rows[:]
            for (r_idx, c_idx, text) in long_cells:
                placeholder = f"[Long message moved to LongMessages_{_sanitize_tab_for_col(report_tag)}]"
                if c_idx < len(offloaded_rows[r_idx]):
                    offloaded_rows[r_idx][c_idx] = placeholder
            title = _write_long_cells_to_sheet(spreadsheet, report_tag, long_cells, headers)
            try:
                safe_gspread_call(ws.update, [headers] + offloaded_rows)
                logger.info("safe_update: wrote placeholders and offloaded full texts to %s", title)
                return {"ok": True, "backup": backup_file, "truncated": True, "long_sheet": title}
            except Exception as e3:
                logger.exception("safe_update: final fallback write failed.")
                raise
    except Exception:
        logger.exception("safe_update: truncation strategy failed; rethrowing.")
        raise


# ---------- OpenAI call ----------
llm_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_LLM)

def call_openai_chat(system_prompt: str, user_prompt: str, model: str = OPENAI_MODEL, max_tokens: int = 800) -> str:
    if not openai:
        raise RuntimeError("openai package not installed.")
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set in env.")
    try:
        client = None
        if hasattr(openai, "OpenAI"):
            try:
                client = openai.OpenAI()
            except Exception:
                client = None
        if client is not None:
            with llm_semaphore:
                time.sleep(LLM_REQUEST_DELAY_SEC)
                resp = client.chat.completions.create(
                    model=model,
                    messages=[{"role":"system","content":system_prompt},{"role":"user","content":user_prompt}],
                    temperature=0.3,
                    max_tokens=max_tokens
                )
                try:
                    text = resp.choices[0].message["content"]
                except Exception:
                    text = getattr(resp.choices[0].message, "content", None) or str(resp)
                logger.debug("OpenAI (new) resp preview: %s", str(text)[:400])
                return text.strip()
    except Exception as e:
        logger.debug("OpenAI new client failed: %s", e)
    try:
        with llm_semaphore:
            time.sleep(LLM_REQUEST_DELAY_SEC)
            resp = openai.ChatCompletion.create(
                model=model,
                messages=[{"role":"system","content":system_prompt},{"role":"user","content":user_prompt}],
                temperature=0.3,
                max_tokens=max_tokens
            )
            text = resp["choices"][0]["message"]["content"]
            logger.debug("OpenAI (legacy) resp preview: %s", str(text)[:400])
            return text.strip()
    except Exception as e:
        logger.exception("OpenAI call failed.")
        raise

def build_prompt_for_row(prompt_template: str, row: Dict[str, Any]) -> str:
    prompt = prompt_template
    for k,v in row.items():
        placeholder = "{{"+k+"}}"
        prompt = prompt.replace(placeholder, str(v))
    prompt += "\n\nRow JSON:\n" + json.dumps(row, ensure_ascii=False)
    return prompt

def ask_llm_for_row(system_prompt: str, prompt_template: str, row: Dict[str, Any]) -> Dict[str, Any]:
    raw = call_openai_chat(system_prompt, build_prompt_for_row(prompt_template, row))
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        s = raw.find("{"); e = raw.rfind("}")
        if s!=-1 and e!=-1 and e>s:
            try:
                return json.loads(raw[s:e+1])
            except Exception:
                pass
    logger.error("LLM output not JSON. Preview: %s", raw[:800])
    raise RuntimeError("LLM output not JSON")

# ---------- Valura: enqueue jobs ----------
def _preferred_valura_headers(api_key: str):
    return {"x-api-key": api_key, "Content-Type": "application/json"}

@retry_backoff()
def enqueue_to_valura_jobs(jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
    if DRY_RUN:
        logger.info("[DRY_RUN] Would enqueue %d jobs to Valura (preview first job): %s", len(jobs),
                    json.dumps(jobs[0], ensure_ascii=False)[:300] if jobs else "<none>")
        return {"status": "dry_run", "jobs": len(jobs)}

    if not VALURA_API_KEY:
        raise RuntimeError("VALURA_API_KEY not set")

    payload = {"jobs": jobs}
    headers = _preferred_valura_headers(VALURA_API_KEY)
    last_exc = None

    try:
        r = requests.post(VALURA_BACKEND_URL, headers=headers, json=payload, timeout=30)
        text = r.text or ""
        logger.debug("Valura attempt header=x-api-key response (trim): %s", text[:800])
        if r.ok:
            try:
                return r.json()
            except Exception:
                return {"status": "ok", "raw": r.text}
        else:
            logger.error("Valura send failed %s: %s", r.status_code, text)
            if r.status_code != 401:
                r.raise_for_status()
            last_exc = requests.HTTPError(f"{r.status_code} {r.text}")
    except Exception as e:
        last_exc = e
        logger.exception("Error calling Valura with x-api-key header.")

    for auth_user in ("api", ""):
        try:
            logger.info("Valura fallback attempt: basic auth user=%r", auth_user)
            r = requests.post(VALURA_BACKEND_URL, headers={"Content-Type": "application/json"},
                              json=payload, auth=(auth_user, VALURA_API_KEY), timeout=30)
            text = r.text or ""
            logger.debug("Valura basic-auth attempt user=%r response (trim): %s", auth_user, text[:800])
            if r.ok:
                try:
                    return r.json()
                except Exception:
                    return {"status": "ok", "raw": r.text}
            else:
                logger.error("Valura basic-auth failed %s (user=%r): %s", r.status_code, auth_user, text)
                if r.status_code != 401:
                    r.raise_for_status()
                last_exc = requests.HTTPError(f"{r.status_code} {r.text}")
        except Exception as e:
            last_exc = e
            logger.exception("Error calling Valura with basic auth user=%r", auth_user)

    logger.error("All Valura attempts failed. Last exception: %s", last_exc)
    raise last_exc

# ---------- helper: sanitize tab name for column ----------
def _sanitize_tab_for_col(tab: str) -> str:
    return "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in (tab or "")).strip("_")[:40] or "tab"

# ---------- Phone normalization helpers ----------
def normalize_phone_with_default(phone_raw: Optional[Any]) -> str:
    try:
        p = "" if phone_raw is None else str(phone_raw)
    except Exception:
        p = ""
    p = p.strip()
    if not p:
        return ""

    p = re.sub(r'[\s\-\(\)\.,]', '', p)

    if p.startswith("+"):
        digits = re.sub(r'[^\d]', '', p[1:])
        digits = digits.lstrip("0")
        if not digits:
            return ""
        return "+" + digits

    digits = re.sub(r'\D', '', p)
    digits = digits.lstrip("0")
    if not digits:
        return ""

    dcc = (DEFAULT_COUNTRY_CODE or "+91").strip()
    if not dcc.startswith("+"):
        dcc = "+" + dcc
    return dcc + digits

normalize_phone = normalize_phone_with_default

# ---------- send & record (overwrites per-tab Message_<sanitized_tab> column) ----------
def send_messages_with_valura_and_record(spreadsheet, contacts_tab: str, list_of_msgs: List[Dict[str, Any]], run_tag: Optional[str] = None):
    """
    Send messages through Valura and record results back to Contacts sheet.
    Uses a run-specific message column named Message_<sanitized_run_tag>.
    Performs batched sheet writes with safe retries.
    """
    logger.info("Preparing to send %d messages (run_tag=%s)", len(list_of_msgs), run_tag)
    ws = create_or_get_worksheet(spreadsheet, contacts_tab, rows=1000, cols=40)
    all_vals = safe_gspread_call(ws.get_all_values)
    headers = all_vals[0] if all_vals else []
    rows = all_vals[1:] if len(all_vals) > 1 else []

    tag = _sanitize_tab_for_col(run_tag or "")
    run_msg_col = f"Message_{tag}"

    # Required columns
    status_col = "Send Status"
    last_sent_col = "Last Sent"
    name_col_candidates = ["BU Name", "name", "Offshore Manager Name", "Name"]
    phone_col_candidates = ["phone", "phone_number", "mobile", "contact", "Phone", "PhoneNumber", "phone_no", "Whatsapp", "whatsapp"]

    # ensure run-specific message column and status columns exist (do not create a generic "Message" column)
    changed = False
    if run_msg_col not in headers:
        headers.append(run_msg_col); changed = True
    if status_col not in headers:
        headers.append(status_col); changed = True
    if last_sent_col not in headers:
        headers.append(last_sent_col); changed = True

    if changed:
        # use safe update which will backup and truncate if needed
        try:
            res = safe_update_sheet_with_backup_and_truncation(spreadsheet, ws, headers, rows, report_tag=run_tag or "run")
            logger.info("safe_update (ensure cols) result: %s", res)
        except Exception:
            logger.exception("safe_update failed for ensure headers; falling back to simple clear+update")
            safe_gspread_call(ws.clear)
            safe_gspread_call(ws.update, [headers] + rows)
        all_vals = safe_gspread_call(ws.get_all_values)
        headers = all_vals[0]
        rows = all_vals[1:] if len(all_vals) > 1 else []

    def index_of_any(cands, hdrs):
        for c in cands:
            if c in hdrs:
                return hdrs.index(c)
        return None

    name_idx = index_of_any(name_col_candidates, headers)
    phone_idx = index_of_any(phone_col_candidates, headers)

    def normalize_name(s): return str(s).strip().lower() if s else ""

    name_to_row = {}
    phone_to_row = {}
    for i, r in enumerate(rows, start=2):
        nm = r[name_idx] if name_idx is not None and name_idx < len(r) else ""
        name_to_row[normalize_name(nm)] = i
        if phone_idx is not None and phone_idx < len(r):
            ph = normalize_phone_with_default(r[phone_idx])
            if ph:
                phone_to_row[ph] = i

    run_msg_idx = headers.index(run_msg_col)
    status_idx = headers.index(status_col)
    last_sent_idx = headers.index(last_sent_col)

    row_msg_map = {}
    for it in list_of_msgs:
        nm = it.get("name") or it.get("BU Name") or it.get("Student Name")
        phone_raw = it.get("phone")
        msg = str(it.get("message") or "").strip()
        norm = normalize_name(nm)
        row_num = name_to_row.get(norm) or phone_to_row.get(normalize_phone_with_default(phone_raw))
        if not row_num:
            new_row = [""] * len(headers)
            if name_idx is not None:
                new_row[name_idx] = nm or ""
            if phone_idx is not None:
                new_row[phone_idx] = normalize_phone_with_default(phone_raw)
            rows.append(new_row)
            row_num = len(rows) + 1
            name_to_row[norm] = row_num

        existing = row_msg_map.get(row_num, "")
        if msg and msg not in existing:
            row_msg_map[row_num] = (existing + "\n\n" + msg).strip() if existing else msg

    for i, r in enumerate(rows, start=2):
        if len(r) < len(headers):
            r += [""] * (len(headers) - len(r))
        r[run_msg_idx] = row_msg_map.get(i, "")

    # safe write combined messages
    try:
        res = safe_update_sheet_with_backup_and_truncation(spreadsheet, ws, headers, rows, report_tag=run_tag or "run")
        logger.info("safe_update (write combined messages) result: %s", res)
    except Exception:
        logger.exception("safe_update failed for writing combined messages; falling back to clear+update")
        safe_gspread_call(ws.clear)
        safe_gspread_call(ws.update, [headers] + rows)
    time.sleep(SHEET_WRITE_DELAY_SEC)

    # send via Valura and update send status & timestamp in-memory
    for it in list_of_msgs:
        nm = it.get("name") or it.get("BU Name") or it.get("Student Name")
        phone_norm = normalize_phone_with_default(it.get("phone"))
        message = str(it.get("message") or "").strip()
        norm = normalize_name(nm)
        row_num = name_to_row.get(norm) or phone_to_row.get(phone_norm)
        if not row_num:
            continue

        send_status = "unknown"
        last_sent = ""
        try:
            if DRY_RUN:
                send_status = "dry_run"
                logger.info("[DRY_RUN] send to %s (%s)", nm, phone_norm)
            else:
                job_id = f"job-{int(time.time())}-{random.randint(1000,9999)}"
                job = {"id": job_id, "phone": phone_norm, "text": message, "meta": {"name": nm}}
                resp = enqueue_to_valura_jobs([job])
                send_status = "sent"
                last_sent = datetime.now().astimezone().isoformat()
                logger.info("Sent job %s to %s (%s) -> %s", job_id, nm, phone_norm, str(resp)[:300])
        except Exception as e:
            send_status = f"failed: {e}"
            logger.exception("Send failed for %s (%s)", nm, phone_norm)

        ridx = row_num - 2
        while len(rows[ridx]) < len(headers):
            rows[ridx].append("")
        rows[ridx][status_idx] = send_status
        rows[ridx][last_sent_idx] = last_sent

        time.sleep(random.uniform(WHATSAPP_MIN_DELAY_SEC, WHATSAPP_MAX_DELAY_SEC))

    # final safe write of statuses
    try:
        res = safe_update_sheet_with_backup_and_truncation(spreadsheet, ws, headers, rows, report_tag=run_tag or "run")
        logger.info("safe_update (final statuses) result: %s", res)
    except Exception:
        logger.exception("safe_update failed for final statuses; falling back to clear+update")
        safe_gspread_call(ws.clear)
        safe_gspread_call(ws.update, [headers] + rows)
    time.sleep(SHEET_WRITE_DELAY_SEC)
    logger.info("Completed send-and-record for %d messages (run_tag=%s).", len(list_of_msgs), run_tag)

# ---------- update_contacts_messages (improved) ----------
@retry_backoff()
def update_contacts_messages(spreadsheet, contacts_tab: str, name_col: str, message_col: str, name_to_message: Dict[str, List[str]]):
    """
    Writes messages into the Contacts tab (Message column) using normalized name matching.
    Ensures name_col exists (creates it if missing) so new appended rows have the BU/name filled.
    Combines multiple messages per BU/name into the same cell.
    """
    try:
        ws = spreadsheet.worksheet(contacts_tab)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=contacts_tab, rows=500, cols=10)
        safe_gspread_call(ws.append_row, [name_col, message_col, "phone"])
        logger.info("Created contacts tab %s with headers.", contacts_tab)
        return

    all_vals = safe_gspread_call(ws.get_all_values)
    if not all_vals:
        headers = [name_col, message_col, "phone"]
        try:
            res = safe_update_sheet_with_backup_and_truncation(spreadsheet, ws, headers, [], report_tag=contacts_tab)
            logger.info("safe_update(created headers) result: %s", res)
        except Exception:
            safe_gspread_call(ws.clear)
            safe_gspread_call(ws.update, [headers])
        all_vals = safe_gspread_call(ws.get_all_values)

    headers = [str(h).strip() for h in all_vals[0]] if all_vals else []
    rows = all_vals[1:] if len(all_vals) > 1 else []

    changed = False
    if name_col not in headers:
        headers.append(name_col); changed = True
    if message_col not in headers:
        headers.append(message_col); changed = True

    if changed:
        new_rows = []
        for r in rows:
            if len(r) < len(headers):
                r = r + [""] * (len(headers) - len(r))
            new_rows.append(r)
        try:
            res = safe_update_sheet_with_backup_and_truncation(spreadsheet, ws, headers, new_rows, report_tag=contacts_tab)
            logger.info("safe_update (ensure name/msg cols) result: %s", res)
        except Exception:
            logger.exception("safe_update failed for ensure name/msg cols; falling back")
            safe_gspread_call(ws.clear)
            safe_gspread_call(ws.update, [headers] + new_rows)
        all_vals = safe_gspread_call(ws.get_all_values)
        headers = [str(h).strip() for h in all_vals[0]] if all_vals else []
        rows = all_vals[1:] if len(all_vals) > 1 else []

    name_idx = headers.index(name_col)
    msg_idx = headers.index(message_col)

    def normalize_key(s):
        return str(s).strip().lower() if s is not None else ""

    existing_name_to_row = {}
    for i, r in enumerate(rows, start=2):
        val = r[name_idx] if name_idx < len(r) else ""
        existing_name_to_row[normalize_key(val)] = i

    for i in range(len(rows)):
        if len(rows[i]) < len(headers):
            rows[i] += [""] * (len(headers) - len(rows[i]))

    for nm_raw, msg_list in name_to_message.items():
        if isinstance(msg_list, (list, tuple)):
            incoming_combined = "\n\n".join([str(m).strip() for m in msg_list if str(m).strip()])
        else:
            incoming_combined = str(msg_list).strip()

        if not incoming_combined:
            continue

        norm = normalize_key(nm_raw)
        row_num = existing_name_to_row.get(norm)

        if row_num:
            r_idx = row_num - 2
            existing_val = str(rows[r_idx][msg_idx] or "").strip()
            if existing_val:
                if incoming_combined not in existing_val:
                    rows[r_idx][msg_idx] = existing_val + "\n\n" + incoming_combined
            else:
                rows[r_idx][msg_idx] = incoming_combined
        else:
            new_row = [""] * len(headers)
            new_row[name_idx] = nm_raw
            new_row[msg_idx] = incoming_combined
            rows.append(new_row)
            new_row_index = len(rows) + 1
            existing_name_to_row[norm] = new_row_index

    try:
        res = safe_update_sheet_with_backup_and_truncation(spreadsheet, ws, headers, rows, report_tag=contacts_tab)
        logger.info("update_contacts_messages safe_update result: %s", res)
        time.sleep(SHEET_WRITE_DELAY_SEC)
        logger.info("update_contacts_messages completed for %d entries. (wrote %d rows)", len(name_to_message), len(rows))
    except Exception:
        logger.exception("Failed batch-write contacts; falling back to per-row updates.")
        for norm_key, msg_list in name_to_message.items():
            incoming_combined = "\n\n".join([str(m).strip() for m in (msg_list if isinstance(msg_list, (list,tuple)) else [msg_list]) if str(m).strip()])
            norm = normalize_key(norm_key)
            row_num = existing_name_to_row.get(norm)
            if row_num:
                try:
                    existing = ws.row_values(row_num)
                    while len(existing) < len(headers):
                        existing.append("")
                    existing_msg = existing[msg_idx] if msg_idx < len(existing) else ""
                    if existing_msg and incoming_combined not in existing_msg:
                        new_val = existing_msg + "\n\n" + incoming_combined
                    else:
                        new_val = incoming_combined if not existing_msg else existing_msg
                    safe_gspread_call(ws.update_cell, row_num, msg_idx+1, new_val)
                except Exception:
                    logger.exception("Failed per-row update for existing contact row %s", row_num)
            else:
                new_row = [""] * len(headers)
                new_row[name_idx] = norm_key
                new_row[msg_idx] = incoming_combined
                try:
                    safe_gspread_call(ws.append_row, new_row)
                except Exception:
                    logger.exception("Failed per-row append for new contact %s", norm_key)
        logger.info("update_contacts_messages completed (fallback path).")

# ---------- main process (friend-style enqueue + status updates) ----------
def process_tab_once(spreadsheet_id: str, source_tab: str, prompt_template: str, report_tab: Optional[str] = None,
                     system_prompt: str = "You must return a single valid JSON object per row."):
    report_tab = report_tab or f"report_{source_tab}"
    logger.info("Processing tab '%s' -> report '%s'", source_tab, report_tab)
    sheet = open_spreadsheet_by_id(spreadsheet_id)
    rows = read_worksheet_as_dicts(sheet, source_tab)
    logger.info("Read %d rows from '%s'.", len(rows), source_tab)
    if not rows:
        create_or_get_worksheet(sheet, report_tab)
        logger.info("No rows; ensured report tab exists and skipped.")
        return

    generated = []
    name_to_message = defaultdict(list)
    def _norm_key(s): return str(s).strip().lower() if s is not None else ""

    for idx, row in enumerate(tqdm(rows, desc=f"LLM {source_tab}", unit="rows")):
        try:
            parsed = ask_llm_for_row(system_prompt, prompt_template, row)
            if not isinstance(parsed, dict):
                logger.warning("LLM returned non-dict for row %d: %r", idx, parsed)
                continue
        except Exception:
            logger.exception("LLM failed for a row in %s; skipping row %d.", source_tab, idx)
            continue
        generated.append(parsed)
        nm = parsed.get("name") or parsed.get("BU Name") or parsed.get("Student Name")
        msg = parsed.get("message") or parsed.get("Message")
        if nm and msg:
            name_to_message[_norm_key(nm)].append(str(msg).strip())

    logger.info("Generated %d outputs from %d input rows for tab '%s'.", len(generated), len(rows), source_tab)
    try:
        dbg_name = f"debug_generated_{source_tab.replace(' ','_')}_{int(time.time())}.json"
        with open(dbg_name, "w", encoding="utf-8") as f:
            json.dump(generated, f, ensure_ascii=False, indent=2)
        logger.info("Saved debug generated file: %s", dbg_name)
    except Exception:
        logger.exception("Failed to save debug generated file.")

    try:
        if generated:
            write_rows_to_worksheet(sheet, report_tab, generated)
            logger.info("Wrote %d rows to report tab '%s'.", len(generated), report_tab)
        else:
            ws = create_or_get_worksheet(sheet, report_tab)
            safe_gspread_call(ws.clear)
            safe_gspread_call(ws.update, [["Status","Info"], ["No generated rows", f"LLM returned 0 valid JSON outputs for {len(rows)} input rows. See debug file."]])
            logger.info("Wrote placeholder to report tab '%s' because no generated rows were available.", report_tab)
    except Exception:
        logger.exception("Failed writing to report tab '%s'.", report_tab)

    if not name_to_message:
        logger.info("No name->message mappings generated; skipping contacts update/send.")
        return

    contacts = read_worksheet_as_dicts(sheet, CONTACTS_TAB)
    if not contacts:
        logger.info("No contacts; skipping send.")
        return

    def normalize_for_lookup(s): return str(s).strip().lower()
    nm_map = {normalize_for_lookup(k): ("\n\n".join(v) if isinstance(v,(list,tuple)) else str(v)) for k,v in name_to_message.items()}

    gen_col = f"GeneratedMessage_{_sanitize_tab_for_col(report_tab)}"
    status_col = f"Status_{_sanitize_tab_for_col(report_tab)}"

    ws = create_or_get_worksheet(sheet, CONTACTS_TAB, rows=max(100, len(contacts)+5), cols=40)
    all_vals = safe_gspread_call(ws.get_all_values)
    headers = all_vals[0] if all_vals else []
    rows_vals = all_vals[1:] if len(all_vals) > 1 else []

    changed_headers = False
    if gen_col not in headers:
        headers.append(gen_col); changed_headers = True
    if status_col not in headers:
        headers.append(status_col); changed_headers = True
    if changed_headers:
        try:
            res = safe_update_sheet_with_backup_and_truncation(sheet, ws, headers, rows_vals, report_tag=report_tab)
            logger.info("safe_update (add gen/status cols) result: %s", res)
        except Exception:
            logger.exception("safe_update failed while adding headers; falling back to clear+update")
            safe_gspread_call(ws.clear)
            safe_gspread_call(ws.update, [headers] + rows_vals)
        all_vals = safe_gspread_call(ws.get_all_values)
        headers = all_vals[0]; rows_vals = all_vals[1:] if len(all_vals) > 1 else []

    name_idx = None
    for cand in ("BU Name","name","Offshore Manager Name","Name","Manager"):
        if cand in headers:
            name_idx = headers.index(cand); break

    phone_idx = None
    for cand in ["Phone","phone","phone_number","mobile","contact","PhoneNumber","phone_no","Whatsapp","whatsapp"]:
        if cand in headers:
            phone_idx = headers.index(cand); break
    if phone_idx is None and rows_vals:
        for j,h in enumerate(headers):
            if any((len(r) > j and re.search(r'\d', str(r[j]) or "")) for r in rows_vals):
                phone_idx = j; break

    def safe_norm(s): return str(s).strip().lower() if s is not None else ""
    row_entries = []
    for r in rows_vals:
        row = r[:]
        if len(row) < len(headers):
            row += [""] * (len(headers) - len(row))
        row_entries.append(row)

    jobs = []
    updates_count = 0
    for bu_key, combined_msg in nm_map.items():
        if not bu_key or not combined_msg:
            continue

        matched_indexes = []
        for idx, row in enumerate(row_entries):
            bu_val = ""
            if name_idx is not None and name_idx < len(row):
                bu_val = str(row[name_idx] or "").strip().lower()
            mgr_candidates = []
            for c in ("Offshore Manager Name","Manager","Name"):
                if c in headers:
                    mgr_candidates.append(str(row[headers.index(c)] or "").strip().lower())
            if (bu_val and bu_val == bu_key) or any((m and m == bu_key) for m in mgr_candidates):
                matched_indexes.append(idx)
        if not matched_indexes:
            for idx, row in enumerate(row_entries):
                bu_val = ""
                if name_idx is not None and name_idx < len(row):
                    bu_val = str(row[name_idx] or "").strip().lower()
                mgr_candidates = []
                for c in ("Offshore Manager Name","Manager","Name"):
                    if c in headers:
                        mgr_candidates.append(str(row[headers.index(c)] or "").strip().lower())
                if (bu_val and bu_key in bu_val) or any((m and bu_key in m) for m in mgr_candidates):
                    matched_indexes.append(idx)

        if not matched_indexes:
            logger.info("No contact matched for key '%s' (report %s)", bu_key, report_tab)
            continue

        for idx in matched_indexes:
            gen_idx = headers.index(gen_col)
            existing = str(row_entries[idx][gen_idx] or "")
            new_gen = (existing + "\n---\n" + combined_msg) if existing else combined_msg
            row_entries[idx][gen_idx] = new_gen

            st_idx = headers.index(status_col)
            queued_label = f"Queued ({datetime.utcnow().isoformat()}Z)"
            row_entries[idx][st_idx] = queued_label

            ph_val = ""
            if phone_idx is not None and phone_idx < len(row_entries[idx]):
                ph_val = row_entries[idx][phone_idx]
            phone_norm = normalize_phone_with_default(ph_val)
            if phone_norm:
                job_id = f"job-{int(time.time())}-{random.randint(0,99999)}"
                jobs.append({"id": job_id, "phone": phone_norm, "text": combined_msg, "meta": {"sourceTab": source_tab, "reportTab": report_tab, "contactRow": idx + 2}})
                updates_count += 1
            else:
                row_entries[idx][st_idx] = f"Missing phone ({datetime.utcnow().isoformat()}Z)"
                logger.info("Contact row %d missing/invalid phone; skipping enqueue", idx + 2)

    try:
        logger.info("Writing back %d contact rows (GeneratedMessage/Status) to Contacts sheet.", len(row_entries))
        try:
            res = safe_update_sheet_with_backup_and_truncation(sheet, ws, headers, row_entries, report_tag=report_tab)
            logger.info("safe_update (generated/status) result: %s", res)
        except Exception:
            logger.exception("safe_update failed for generated/status; falling back to clear+update")
            safe_gspread_call(ws.clear)
            safe_gspread_call(ws.update, [headers] + row_entries)
    except Exception:
        logger.exception("Failed to write GeneratedMessage/Status back to Contacts sheet (will continue to enqueue jobs).")

    queued = 0
    if jobs:
        logger.info("Enqueueing %d jobs to Valura for report %s", len(jobs), report_tab)
        for i, job in enumerate(jobs, start=1):
            try:
                if DRY_RUN:
                    logger.info("[DRY_RUN] Enqueue job %s -> %s", job["id"], job["phone"])
                    resp = {"status": "dry_run"}
                else:
                    resp = enqueue_to_valura_jobs([job])
                queued += 1
                logger.info("[%s] Enqueued %s -> OK (%d/%d)", report_tab, job["phone"], i, len(jobs))
                row_idx = job["meta"].get("contactRow", None)
                if row_idx:
                    ridx = row_idx - 2
                    st_idx = headers.index(status_col)
                    row_entries[ridx][st_idx] = f"Sent ({datetime.utcnow().isoformat()}Z)"
            except Exception as e:
                logger.exception("Failed to enqueue job %s: %s", job.get("id"), e)
                row_idx = job["meta"].get("contactRow", None)
                if row_idx:
                    ridx = row_idx - 2
                    st_idx = headers.index(status_col)
                    row_entries[ridx][st_idx] = f"Failed enqueue: {str(e)[:120]}"

            try:
                min_ms = int(os.environ.get("WHATSAPP_MIN_DELAY_MS", str(int(WHATSAPP_MIN_DELAY_SEC*1000))))
                max_ms = int(os.environ.get("WHATSAPP_MAX_DELAY_MS", str(int(WHATSAPP_MAX_DELAY_SEC*1000))))
                if min_ms < 0: min_ms = 0
                if max_ms < min_ms: max_ms = min_ms
                gap_ms = random.randint(min_ms, max_ms)
            except Exception:
                gap_ms = int((WHATSAPP_MIN_DELAY_SEC * 1000))
            logger.info("[%s] Waiting %d ms before next send...", report_tab, gap_ms)
            time.sleep(gap_ms / 1000.0)

        try:
            logger.info("Writing final statuses back to Contacts sheet (batch).")
            try:
                res = safe_update_sheet_with_backup_and_truncation(sheet, ws, headers, row_entries, report_tag=report_tab)
                logger.info("safe_update (final statuses) result: %s", res)
            except Exception:
                logger.exception("safe_update failed for final statuses; falling back to clear+update")
                safe_gspread_call(ws.clear)
                safe_gspread_call(ws.update, [headers] + row_entries)
        except Exception:
            logger.exception("Failed to write back final statuses to Contacts sheet.")
    else:
        logger.info("No jobs created for report %s", report_tab)

    logger.info("process_tab_once completed: generated=%d jobs_prepared=%d queued=%d", len(generated), updates_count, queued)

# ---------- TABS_CONFIG loader & scheduler ----------
def load_tabs_config() -> List[Dict[str,Any]]:
    if TABS_CONFIG_FILE and os.path.exists(TABS_CONFIG_FILE):
        try:
            with open(TABS_CONFIG_FILE,"r",encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                logger.info("Loaded %d tab configs from %s", len(data), TABS_CONFIG_FILE)
                return data
            logger.error("TABS_CONFIG_FILE is not a JSON array.")
            return []
        except Exception:
            logger.exception("Failed to read/parse TABS_CONFIG_FILE.")
            return []
    if TABS_CONFIG_ENV:
        try:
            data = json.loads(TABS_CONFIG_ENV)
            if isinstance(data,list):
                logger.info("Loaded %d tab configs from TABS_CONFIG env.", len(data))
                return data
            logger.error("TABS_CONFIG env is not a JSON array.")
            return []
        except Exception:
            logger.exception("Failed to parse TABS_CONFIG env.")
    logger.warning("No TABS_CONFIG found.")
    return []

def start_scheduler(spreadsheet_id: str):
    tabs = load_tabs_config()
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    scheduler.start()
    def schedule_one(cfg: Dict[str,Any]):
        src = cfg.get("sourceTab"); pt = cfg.get("promptTemplate"); rpt = cfg.get("reportTab") or f"report_{src}"
        enqueue = bool(cfg.get("enqueue", True)); cron_expr = cfg.get("schedule"); tz = cfg.get("timezone") or TIMEZONE
        if not src or not pt:
            logger.warning("Invalid tab config: missing sourceTab or promptTemplate: %s", cfg); return
        def job_func():
            try:
                process_tab_once(spreadsheet_id, src, pt, rpt)
            except Exception:
                logger.exception("Job failed for %s", src)
        if enqueue:
            if cron_expr:
                try:
                    trig = CronTrigger.from_crontab(cron_expr, timezone=pytz_timezone(tz))
                    job_id = f"job_{src}"
                    scheduler.add_job(job_func, trigger=trig, id=job_id, replace_existing=True)
                    logger.info("Scheduled %s cron='%s' tz=%s -> %s", src, cron_expr, tz, rpt)
                except Exception:
                    logger.exception("Failed to schedule %s; running once now.", src)
                    threading.Thread(target=job_func).start()
            else:
                logger.info("enqueue true but no schedule; running %s once now.", src)
                threading.Thread(target=job_func).start()
        else:
            logger.info("enqueue=false for %s; not scheduling.", src)
    for cfg in tabs:
        schedule_one(cfg)
    if TABS_CONFIG_FILE and os.path.exists(TABS_CONFIG_FILE):
        last_mtime = os.path.getmtime(TABS_CONFIG_FILE)
        def poll_loop():
            nonlocal last_mtime
            while True:
                try:
                    time.sleep(TABS_CONFIG_FILE_POLL_SECONDS)
                    m = os.path.getmtime(TABS_CONFIG_FILE)
                    if m != last_mtime:
                        last_mtime = m
                        logger.info("Detected change in TABS_CONFIG_FILE; rescheduling jobs.")
                        for job in scheduler.get_jobs():
                            try: scheduler.remove_job(job.id)
                            except Exception: pass
                        new_tabs = load_tabs_config()
                        for cfg in new_tabs: schedule_one(cfg)
                except Exception:
                    logger.exception("Hot-reload poll loop error.")
        threading.Thread(target=poll_loop, daemon=True).start()
    if RUN_IMMEDIATE:
        logger.info("RUN_IMMEDIATE=true -> running tabs once now.")
        for cfg in load_tabs_config():
            try:
                src = cfg.get("sourceTab"); pt = cfg.get("promptTemplate"); rpt = cfg.get("reportTab") or f"report_{src}"
                if src and pt:
                    process_tab_once(spreadsheet_id, src, pt, rpt)
            except Exception:
                logger.exception("Immediate run failed for %s", cfg.get("sourceTab"))
    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler.")
        scheduler.shutdown()

# ---------- entry ----------
if __name__ == "__main__":
    logger.info("Starting app.py (VALURA_BACKEND_URL=%s DRY_RUN=%s VALURA_AUTH_HEADER=%s DEFAULT_COUNTRY_CODE=%s)", VALURA_BACKEND_URL, DRY_RUN, VALURA_AUTH_HEADER, DEFAULT_COUNTRY_CODE)
    logger.info("SPREADSHEET_ID=%s CONTACTS_TAB=%s TABS_CONFIG_FILE=%s", SPREADSHEET_ID, CONTACTS_TAB, TABS_CONFIG_FILE)
    start_scheduler(SPREADSHEET_ID)
