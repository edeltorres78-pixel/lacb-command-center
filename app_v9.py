import os
import re
import sqlite3
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

try:
    import psycopg2
except ImportError:
    psycopg2 = None


# =========================================================
# CONFIG
# =========================================================

APP_TITLE = "LACB Customer Care Command Center V9"
DB_PATH = "lacb_command_center.db"

def _safe_secret_get(key, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


def _current_db_backend() -> str:
    backend = (
        os.getenv("DB_BACKEND")
        or _safe_secret_get("DB_BACKEND")
        or ("postgres" if (os.getenv("DATABASE_URL") or _safe_secret_get("DATABASE_URL")) else "sqlite")
    )
    return str(backend or "sqlite").strip().lower()


def _pg_conn_args() -> dict:
    # Prefer explicit PG* fields first so stale DATABASE_URL values don't hijack
    # the connection (common during Cloud secrets edits/migrations).
    host = os.getenv("PGHOST") or _safe_secret_get("PGHOST")
    if host:
        return {
            "host": str(host).strip(),
            "port": int(os.getenv("PGPORT") or _safe_secret_get("PGPORT") or 5432),
            "dbname": os.getenv("PGDATABASE") or _safe_secret_get("PGDATABASE"),
            "user": os.getenv("PGUSER") or _safe_secret_get("PGUSER"),
            "password": os.getenv("PGPASSWORD") or _safe_secret_get("PGPASSWORD"),
            "sslmode": os.getenv("PGSSLMODE") or _safe_secret_get("PGSSLMODE") or "require",
        }

    db_url = os.getenv("DATABASE_URL") or _safe_secret_get("DATABASE_URL")
    db_url = str(db_url or "").strip()
    # Ignore template/placeholder URLs that cause DNS failures in production.
    if db_url and "<your-project-ref>" not in db_url and "[YOUR-PASSWORD]" not in db_url:
        return {"dsn": db_url}

    return {
        "host": None,
        "port": 5432,
        "dbname": None,
        "user": None,
        "password": None,
        "sslmode": "require",
    }

def _adapt_sql(sql: str, backend: str) -> str:
    if backend == "postgres":
        return sql.replace("?", "%s")
    return sql


class _DBCursor:
    def __init__(self, cursor, backend: str):
        self._cursor = cursor
        self._backend = backend

    def execute(self, sql, params=None):
        adapted = _adapt_sql(sql, self._backend)
        if params is None:
            return self._cursor.execute(adapted)
        return self._cursor.execute(adapted, params)

    def executemany(self, sql, seq_of_params):
        adapted = _adapt_sql(sql, self._backend)
        return self._cursor.executemany(adapted, seq_of_params)

    def __getattr__(self, item):
        return getattr(self._cursor, item)


class _DBConn:
    def __init__(self, conn, backend: str):
        self._conn = conn
        self.backend = backend

    def cursor(self):
        return _DBCursor(self._conn.cursor(), self.backend)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()

    def __getattr__(self, item):
        return getattr(self._conn, item)


def _db_read_sql_query(query: str, conn, params=None) -> pd.DataFrame:
    backend = getattr(conn, "backend", _current_db_backend())
    raw_conn = conn._conn if hasattr(conn, "_conn") else conn
    adapted = _adapt_sql(query, backend)
    if params is None:
        return pd.read_sql_query(adapted, raw_conn)
    return pd.read_sql_query(adapted, raw_conn, params=params)

REQUEST_TYPES = [
    "Service",
    "Scheduling",
    "Customer Follow-Up",
    "Vendor Follow-Up",
    "Internal Request",
    "General Support",
]

ISSUE_TYPES = [
    "Motor Issue",
    "Fabric Issue",
    "Guidewire Issue",
    "Anchor Issue",
    "Programming / Demonstrate Programming Steps",
    "Charger Reconnection",
    "Remote / Programming Issue",
    "Install Concern",
    "Shutter Issue",
    "Scheduling Request",
    "Order Status/ETA Update",
    "General Question",
    "Other",
]

HUBSPOT_STAGES = [
    "New",
    "Waiting on Order Readiness",
    "Scheduling Pending",
    "Awaiting Customer Response",
    "Awaiting Internal Response",
    "Closed",
]

INTERNAL_STATUSES = [
    "Open",
    "Pending Scheduling",
    "Pending Customer",
    "Pending Internal",
    "Waiting on Customer",
    "Waiting on Vendor",
    "Waiting on DC",
    "Waiting on Internal Dept",
    "Scheduled",
    "Ready to Close",
    "Closed",
]

TICKET_SOURCES = [
    "Phone",
    "SMS",
    "Email",
    "Internal",
    "Chat",
    "Walk-In",
]

OWNERS = [
    "Ed Torres",
    "Erika Sagasta",
]

KPI_AGENTS = [
    "Ed Torres",
    "Erika Sagasta",
    "User 3",
    "User 4",
    "User 5",
]

MANUAL_TALLY_TYPES = [
    "Inbound RingCX",
    "Inbound RingCentral",
    "Outbound RingCX",
    "Outbound RingCentral",
    "SMS",
    "Email",
]

IO_CHANNELS = [
    "Inbound RingCX",
    "Outbound RingCX",
    "Inbound RC (MVP)",
    "Outbound RC (MVP)",
    "SMS",
    "Email",
    "Internal Chat Other",
    "Internal Chat Scheduling",
    "Internal Chat DC",
    "New HubSpot Ticket Assigned",
    "HubSpot Ticket Update/Follow-Up",
    "Task Review",
    "Direct Request DC",
    "Direct Request Leadership",
]

IO_TASK_ACTIONS = [
    "Ticket Updated/Notes",
    "Ticket Closed",
    "New Ticket Created",
    "New Ticket Assigned - Receptionist",
    "New Ticket Assigned - Supervisor",
    "New Ticket Assigned - Sales Team",
    "New Ticket Assigned - By Other",
    "Order Status/ETA Review and update",
    "Service Order Created",
    "Scheduled/Booked Service Appt",
    "Transferred call Sales",
    "Transferred call Agent",
    "Transferred call Scheduling",
    "Transferred call Other",
    "Task Updated",
    "Task Marked Complete",
    "Submitted Scheduling Request",
    "Internal Email Sent",
    "Called Vendor",
    "Emailed Vendor",
    "RC Chat w/ Team Member",
    "Internal Call w/ Team Member",
    "Call w/ DC",
    "SMS w/ DC",
    "SMS w/ Customer",
    "Call w/ JC Scheduling",
    "Called Installer",
    "SMS Installer",
]

IO_OUTCOMES = [
    "Closed/Resolved",
    "Escalated",
    "Waiting on Customer",
    "Waiting on DC",
    "Waiting on Internal Dpt",
    "Waiting on Scheduling Team",
    "Waiting on Vendor",
    "Transferred call",
    "Follow up",
    "Scheduled",
]

TERMINAL_FOLLOWUP_OUTCOMES = {
    "closed/resolved",
    "closed",
    "resolved",
    "escalated",
    "transferred call",
    "scheduled",
}
QUICK_LOG_ACTIONS = [
    "Outbound Call",
    "Inbound Call",
    "SMS Sent",
    "Email Sent",
    "Internal Chat",
    "HubSpot Note Added",
    "QuoteRite Note Added",
    "Service Order Created",
    "Appointment Scheduled",
    "Scheduling Request Sent",
    "Vendor Follow-Up",
    "DC Follow-Up",
    "Internal Dept Follow-Up",
    "Ticket Closed",
]

QUICK_LOG_RESULTS = [
    "Spoke with customer",
    "No answer",
    "Left voicemail",
    "Waiting on customer",
    "Waiting on vendor",
    "Waiting on DC",
    "Waiting on internal department",
    "Resolved",
]

ASSIGNED_BY_OPTIONS = [
    "Receptionist",
    "Ed Torres",
    "Erika Sagasta",
    "Supervisor",
    "Scheduling",
    "Installer",
    "Customer",
    "Vendor",
    "Internal Team",
]

TICKET_IMPORT_FIELDS = [
    "ticket_title",
    "customer_name",
    "order_no",
    "phone",
    "issue_type",
    "request_type",
    "ticket_source",
    "assigned_by",
    "assigned_agent",
    "hubspot_stage",
    "internal_status",
    "attempt_no",
    "last_activity_date",
    "next_followup_date",
    "scheduling_flags",
    "notes_summary",
]

TICKET_IMPORT_ALIASES = {
    "ticket_title": ["ticket_title", "hubspot ticket title", "hubspot_title", "title", "ticket name"],
    "customer_name": ["customer_name", "customer", "customer name", "name"],
    "order_no": ["order_no", "order #", "order", "order number", "service order", "service order #"],
    "phone": ["phone", "phone number", "customer phone"],
    "issue_type": ["issue_type", "issue", "issue type"],
    "request_type": ["request_type", "request", "request type"],
    "ticket_source": ["ticket_source", "ticket source", "source"],
    "assigned_by": ["assigned_by", "assigned by"],
    "assigned_agent": ["assigned_agent", "assigned agent", "owner", "assigned owner"],
    "hubspot_stage": ["hubspot_stage", "hubspot stage", "stage"],
    "internal_status": ["internal_status", "internal status", "status"],
    "attempt_no": ["attempt_no", "attempt", "attempt #"],
    "last_activity_date": ["last_activity_date", "last activity", "last activity date"],
    "next_followup_date": ["next_followup_date", "next followup", "next follow-up", "next follow-up date"],
    "scheduling_flags": ["scheduling_flags", "scheduling flags", "flags"],
    "notes_summary": ["notes_summary", "notes", "notes summary", "summary"],
}

REGION_MAP_CANDIDATES = ["Region_Map.csv", "region_map.csv"]
SCHEDULER_CANDIDATES = ["Scheduling Tool_2_20.xlsx", "Scheduling Tool.xlsx"]

SCHEDULING_FLAGS = [
    "Palm Springs",
    "Shutters",
    "Arizona",
    "2-Man Job",
    "Measure Needed",
    "Reinstall",
    "Installer Dispute",
    "Warranty",
    "Parts ETA",
]

REGION_ALIASES = {
    "LA": ["LA", "LOS ANGELES"],
    "OC": ["OC", "ORANGE COUNTY"],
    "IE": ["IE", "IE/RIVERSIDE", "INLAND EMPIRE", "RIVERSIDE"],
    "SD": ["SD", "SAN DIEGO"],
    "PS": ["PS", "PALM SPRINGS"],
    "ARIZONA": ["ARIZONA", "AZ"],
    "LAS VEGAS": ["LAS VEGAS", "VEGAS", "LV"],
    "VALLEY": ["VALLEY"],
    "VENTURA": ["VENTURA"],
}
ETA_REFERENCE = {
    "Roller Shades": [
        {"vendor": "LA Custom Blinds", "turnaround": "3 DAYS-1 WEEK", "preschedule": "1-2 WEEKS"},
    ],
    "Zebra Shades": [
        {"vendor": "Albright", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
        {"vendor": "LA Custom Blinds", "turnaround": "2-3 WEEKS", "preschedule": "3-4 WEEKS"},
    ],
    "Cell Shades": [
        {"vendor": "Blind Express", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
        {"vendor": "Lantex", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
    ],
    "Zipper Shades": [
        {"vendor": "LA Custom Blinds", "turnaround": "1-2 WEEKS", "preschedule": "3-4 WEEKS"},
        {"vendor": "Superior Blinds MF", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
    ],
    "Exterior Shades": [
        {"vendor": "LA Custom Blinds", "turnaround": "2-3 WEEKS", "preschedule": "3-4 WEEKS"},
    ],
    "Woven Wood": [
        {"vendor": "Lantex", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
    ],
    "Sheer Shades": [
        {"vendor": "Lantex", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
    ],
    "Shutters": [
        {"vendor": "A Custom Shutters", "turnaround": "4-5 WEEKS", "preschedule": "5-6 WEEKS"},
    ],
    "Faux Wood": [
        {"vendor": "Superior Blinds MF", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
    ],
    "Woodblinds": [
        {"vendor": "Superior Blinds MF", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
    ],
    "Aluminum": [
        {"vendor": "Blind Express", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
        {"vendor": "Superior Blinds MF", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
    ],
    "Vertical Blinds": [
        {"vendor": "Blind Express", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
        {"vendor": "Superior Blinds MF", "turnaround": "3-4 WEEKS", "preschedule": "5-6 WEEKS"},
    ],
    "Roman Shades": [
        {"vendor": "Richard Williams", "turnaround": "6-8 WEEKS", "preschedule": "9-10 WEEKS"},
        {"vendor": "Drapemax", "turnaround": "4-5 WEEKS", "preschedule": ""},
    ],
    "Drapery": [
        {"vendor": "Richard Williams", "turnaround": "6-8 WEEKS", "preschedule": "9-10 WEEKS"},
        {"vendor": "Drapemax", "turnaround": "4-5 WEEKS", "preschedule": ""},
    ],
}
ASSISTANT_PROMPT_TEMPLATES = {
    "HubSpot Ticket Draft": "Create a HubSpot ticket for customer [Name], order [Order #], phone [Phone], issue [Issue]. Include internal summary + CRM note + next follow-up recommendation.",
    "Customer SMS Update": "Write a customer SMS update for [Name], order [Order #], explaining we are waiting on scheduling confirmation and will follow up within 24-48 business hours.",
    "Customer Email Update": "Write a customer email in LACB style for [Name], order [Order #], confirming we received the concern and are coordinating next steps.",
    "PROBLEM/RESOLUTION/EXPECTATION": "Convert these raw notes into PROBLEM / RESOLUTION / EXPECTATION format: [paste notes]",
    "Vendor Follow-Up": "Draft a vendor follow-up note for Superior Blinds for customer [Name], order [Order #], requesting status on parts/repair timeline.",
    "Call Script": "Create a call script for outbound follow-up attempt #[1/2/3/4] for customer [Name], order [Order #], issue [Issue].",
    "Transcript Summary": "Summarize this RingCX call transcript into a concise HubSpot summary + CRM note + customer-friendly SMS: [paste transcript]",
    "Scheduling Request": "Create a scheduling request block for JC-SCHEDULING CHAT with customer [Name], order [Order #], phone [Phone], availability [Availability], notes [Notes].",
    "Final No-Response Closure": "Create a final no-response closure message for customer [Name], order [Order #], after 4 follow-up attempts, professional and polite.",
    "Close/Escalate Recommendation": "Review this ticket history and recommend: keep open, schedule follow-up, escalate vendor, or ready to close. Explain why briefly: [paste timeline/notes]",
}
LACB_ASSISTANT_INSTRUCTIONS = """
You are Ed Torres' personal CRM and communications assistant for LA Custom Blinds.

Always write in first person when generating internal notes.
Maintain a professional, clear, courteous, and solution-oriented tone.
Create HubSpot tickets, customer SMS/emails, CRM notes, and scheduling requests.

Always include order numbers, contact details, and next steps when available.
Customer-facing messages should sound warm, concise, and natural.
Internal notes should follow this structure:

PROBLEM:
[Short description]

RESOLUTION:
[Action taken]

EXPECTATION:
[Next step]

For internal emails, prefix the subject with: LACB |

Use LA Custom Blinds style consistently.
"""


# =========================================================
# PAGE CONFIG
# =========================================================

st.set_page_config(
    page_title=APP_TITLE,
    layout="wide",
)


# =========================================================
# DATABASE
# =========================================================

def get_conn():
    backend = _current_db_backend()
    if backend == "postgres":
        if psycopg2 is None:
            raise RuntimeError("Postgres backend selected but psycopg2 is not installed.")
        raw = psycopg2.connect(**_pg_conn_args())
        return _DBConn(raw, "postgres")

    raw = sqlite3.connect(DB_PATH, check_same_thread=False)
    return _DBConn(raw, "sqlite")


def _parse_cc_sequence(cc_id: str) -> int:
    m = re.match(r"^CC-(\d+)$", str(cc_id or "").strip(), flags=re.I)
    return int(m.group(1)) if m else 0


def _next_cc_id_from_seq(seq: int) -> str:
    return f"CC-{seq:04d}"


def get_next_cc_id(conn) -> str:
    cur = conn.cursor()
    cur.execute("SELECT cc_id FROM tickets WHERE cc_id IS NOT NULL")
    max_seq = 0
    for (cid,) in cur.fetchall():
        max_seq = max(max_seq, _parse_cc_sequence(cid))
    return _next_cc_id_from_seq(max_seq + 1)


def _table_columns(conn, table_name: str) -> list[str]:
    cur = conn.cursor()
    backend = getattr(conn, "backend", "sqlite")
    if backend == "postgres":
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ?
            """,
            (table_name,),
        )
        return [r[0] for r in cur.fetchall()]

    cur.execute(f"PRAGMA table_info({table_name})")
    return [r[1] for r in cur.fetchall()]


def ensure_cc_id_schema_and_backfill(conn):
    cur = conn.cursor()
    cols = _table_columns(conn, "tickets")
    if "cc_id" not in cols:
        cur.execute("ALTER TABLE tickets ADD COLUMN cc_id TEXT")

    cur.execute("UPDATE tickets SET cc_id = NULL WHERE TRIM(COALESCE(cc_id, '')) = ''")

    cur.execute("SELECT id, cc_id FROM tickets")
    used = set()
    max_seq = 0
    rows = cur.fetchall()
    for _id, cid in rows:
        seq = _parse_cc_sequence(cid)
        if seq > 0:
            used.add(seq)
            max_seq = max(max_seq, seq)

    cur.execute("SELECT id FROM tickets WHERE cc_id IS NULL ORDER BY id")
    missing_ids = [r[0] for r in cur.fetchall()]
    next_seq = max_seq + 1
    for ticket_id in missing_ids:
        while next_seq in used:
            next_seq += 1
        cur.execute("UPDATE tickets SET cc_id = ? WHERE id = ?", (_next_cc_id_from_seq(next_seq), ticket_id))
        used.add(next_seq)
        next_seq += 1

    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tickets_cc_id_unique ON tickets(cc_id)")


def ensure_activity_io_schema(conn):
    cur = conn.cursor()
    cols = _table_columns(conn, "activities")
    for column_name in [
        "io_channel",
        "customer_name",
        "order_no",
        "phone",
        "email",
        "ticket_update_name",
        "follow_up_date",
        "follow_up_case_id",
    ]:
        if column_name not in cols:
            cur.execute(f"ALTER TABLE activities ADD COLUMN {column_name} TEXT")


def _parse_follow_up_case_seq(case_id: str) -> int:
    m = re.match(r"^FUCASE-(\d+)$", clean_text(case_id), flags=re.I)
    return int(m.group(1)) if m else 0


def _next_follow_up_case_id_from_seq(seq: int) -> str:
    return f"FUCASE-{seq:05d}"


def _activity_case_identifiers(customer_name: str = "", order_no: str = "", phone: str = "", ticket_id=None) -> list[str]:
    identifiers = []
    ticket_token = _safe_optional_int(ticket_id)
    if ticket_token is not None:
        identifiers.append(f"ticket:{ticket_token}")
    order_token = _normalize_followup_order(order_no)
    phone_token = _normalize_followup_phone(phone)
    name_token = _normalize_followup_name(customer_name)
    if order_token:
        identifiers.append(f"order:{order_token}")
    if phone_token:
        identifiers.append(f"phone:{phone_token}")
    if name_token:
        identifiers.append(f"name:{name_token}")
    return identifiers


def get_or_create_follow_up_case_id(conn, customer_name: str = "", order_no: str = "", phone: str = "", ticket_id=None) -> str:
    cur = conn.cursor()
    identifiers = _activity_case_identifiers(customer_name, order_no, phone, ticket_id)

    for identifier in identifiers:
        prefix, value = identifier.split(":", 1)
        if prefix == "ticket":
            cur.execute(
                """
                SELECT follow_up_case_id
                FROM activities
                WHERE follow_up_case_id IS NOT NULL AND ticket_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (_safe_optional_int(value),),
            )
        elif prefix == "order":
            cur.execute(
                """
                SELECT follow_up_case_id
                FROM activities
                WHERE follow_up_case_id IS NOT NULL AND LOWER(REPLACE(COALESCE(order_no, ''), ' ', '')) = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (value,),
            )
        elif prefix == "phone":
            cur.execute(
                """
                SELECT follow_up_case_id, phone
                FROM activities
                WHERE follow_up_case_id IS NOT NULL AND COALESCE(phone, '') <> ''
                ORDER BY id ASC
                """
            )
            for case_id, stored_phone in cur.fetchall():
                if _normalize_followup_phone(stored_phone) == value:
                    return clean_text(case_id)
            continue
        else:
            cur.execute(
                """
                SELECT follow_up_case_id
                FROM activities
                WHERE follow_up_case_id IS NOT NULL AND LOWER(TRIM(COALESCE(customer_name, ''))) = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (value,),
            )

        row = cur.fetchone()
        if row and clean_text(row[0]):
            return clean_text(row[0])

    cur.execute("SELECT follow_up_case_id FROM activities WHERE follow_up_case_id IS NOT NULL")
    max_seq = 0
    for (case_id,) in cur.fetchall():
        max_seq = max(max_seq, _parse_follow_up_case_seq(case_id))
    return _next_follow_up_case_id_from_seq(max_seq + 1)


def ensure_follow_up_case_ids(conn):
    cur = conn.cursor()
    cols = _table_columns(conn, "activities")
    if "follow_up_case_id" not in cols:
        return

    cur.execute(
        """
        SELECT id, ticket_id, customer_name, order_no, phone, follow_up_case_id
        FROM activities
        ORDER BY id ASC
        """
    )
    rows = cur.fetchall()
    if not rows:
        return

    identifier_to_case = {}
    max_seq = 0
    updates = []

    for row_id, ticket_id, customer_name, order_no, phone, existing_case_id in rows:
        case_id = clean_text(existing_case_id)
        if case_id:
            max_seq = max(max_seq, _parse_follow_up_case_seq(case_id))

    next_seq = max_seq + 1

    for row_id, ticket_id, customer_name, order_no, phone, existing_case_id in rows:
        identifiers = _activity_case_identifiers(customer_name, order_no, phone, ticket_id)
        case_id = clean_text(existing_case_id)

        if not case_id:
            for identifier in identifiers:
                if identifier in identifier_to_case:
                    case_id = identifier_to_case[identifier]
                    break

        if not case_id:
            case_id = _next_follow_up_case_id_from_seq(next_seq)
            next_seq += 1

        for identifier in identifiers:
            identifier_to_case[identifier] = case_id

        if clean_text(existing_case_id) != case_id:
            updates.append((case_id, int(row_id)))

    if updates:
        cur.executemany("UPDATE activities SET follow_up_case_id = ? WHERE id = ?", updates)


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    backend = getattr(conn, "backend", "sqlite")

    if backend == "postgres":
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                id BIGSERIAL PRIMARY KEY,
                ticket_title TEXT,
                customer_name TEXT,
                order_no TEXT,
                phone TEXT,
                issue_type TEXT,
                request_type TEXT,
                ticket_source TEXT,
                assigned_by TEXT,
                assigned_agent TEXT,
                hubspot_stage TEXT,
                internal_status TEXT,
                attempt_no INTEGER DEFAULT 1,
                last_activity_date TEXT,
                next_followup_date TEXT,
                scheduling_flags TEXT,
                notes_summary TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS activities (
                id BIGSERIAL PRIMARY KEY,
                ticket_id BIGINT,
                activity_date TEXT,
                logging_agent TEXT,
                assigned_owner TEXT,
                io_channel TEXT,
                customer_name TEXT,
                order_no TEXT,
                phone TEXT,
                email TEXT,
                ticket_update_name TEXT,
                follow_up_date TEXT,
                follow_up_case_id TEXT,
                action_type TEXT,
                result_type TEXT,
                notes TEXT,
                created_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_tallies (
                id BIGSERIAL PRIMARY KEY,
                tally_date TEXT,
                owner TEXT,
                metric_type TEXT,
                quantity INTEGER DEFAULT 0,
                notes TEXT,
                created_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS assistant_prompt_templates (
                id BIGSERIAL PRIMARY KEY,
                template_name TEXT UNIQUE,
                prompt_text TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_title TEXT,
                customer_name TEXT,
                order_no TEXT,
                phone TEXT,
                issue_type TEXT,
                request_type TEXT,
                ticket_source TEXT,
                assigned_by TEXT,
                assigned_agent TEXT,
                hubspot_stage TEXT,
                internal_status TEXT,
                attempt_no INTEGER DEFAULT 1,
                last_activity_date TEXT,
                next_followup_date TEXT,
                scheduling_flags TEXT,
                notes_summary TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER,
                activity_date TEXT,
                logging_agent TEXT,
                assigned_owner TEXT,
                io_channel TEXT,
                customer_name TEXT,
                order_no TEXT,
                phone TEXT,
                email TEXT,
                ticket_update_name TEXT,
                follow_up_date TEXT,
                follow_up_case_id TEXT,
                action_type TEXT,
                result_type TEXT,
                notes TEXT,
                created_at TEXT,
                FOREIGN KEY(ticket_id) REFERENCES tickets(id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_tallies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tally_date TEXT,
                owner TEXT,
                metric_type TEXT,
                quantity INTEGER DEFAULT 0,
                notes TEXT,
                created_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS assistant_prompt_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_name TEXT UNIQUE,
                prompt_text TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )

    ensure_cc_id_schema_and_backfill(conn)
    ensure_activity_io_schema(conn)
    ensure_follow_up_case_ids(conn)
    conn.commit()
    conn.close()


# =========================================================
# SESSION STATE
# =========================================================

def init_v9_state():
    defaults = {
        "selected_page": "HubSpot Ticket Builder V9",

        # Builder inputs
        "builder_customer_name": "",
        "builder_order_no": "",
        "builder_phone": "",
        "builder_ticket_source": "Phone",
        "builder_assigned_by": "Receptionist",
        "builder_assigned_agent": "Ed Torres",
        "builder_issue_type": "Other",
        "builder_request_type": "Service",
        "builder_hubspot_stage": "New",
        "builder_internal_status": "Open",
        "builder_attempt_no": 1,
        "builder_raw_details": "",

        # Builder helpers
        "builder_pending_autofill": None,
        "builder_generated_ticket_name": "",
        "builder_generated_summary": "",
        "builder_generated_crm_note": "",
        "builder_generated_sms": "",
        "builder_generated_email": "",
        "builder_multi_ticket_drafts": [],

        # Assistant
        "assistant_chat_history": [],
        "assistant_last_output": "",
        "assistant_last_no_api_package": None,
        "assistant_mode": "General",
        "assistant_no_api_mode": True,
        "assistant_use_ticket_context": False,
        "assistant_user_prompt": "",
        "assistant_reset_requested": False,

        # Ticket selection
        "selected_ticket_id": None,

        # Quick log
        "quick_log_ticket_id": None,
        "quick_log_actions": ["Outbound Call"],
        "quick_log_result_type": "No answer",
        "quick_log_notes": "",
        "quick_log_logging_agent": "Ed Torres",
        "quick_log_assigned_owner": "Ed Torres",
        "quick_log_reset_requested": False,

        # Inbound / outbound log
        "io_logging_agent": "Ed Torres",
        "io_channels": [IO_CHANNELS[0]],
        "io_actions": [],
        "io_result_types": [],
        "io_customer_name": "",
        "io_order_no": "",
        "io_phone": "",
        "io_email": "",
        "io_ticket_update_name": "",
        "io_has_follow_up_date": False,
        "io_follow_up_date_input": add_business_days(date.today(), 1),
        "io_notes": "",
        "io_reset_requested": False,
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# =========================================================
# HELPERS
# =========================================================

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def now_pacific_ts() -> str:
    return datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M:%S %Z")


def today_str() -> str:
    return date.today().isoformat()


def default_internal_status(request_type: str) -> str:
    mapping = {
        "Service": "Pending Scheduling",
        "Scheduling": "Pending Scheduling",
        "Customer Follow-Up": "Pending Customer",
        "Vendor Follow-Up": "Pending Internal",
        "Internal Request": "Pending Internal",
        "General Support": "Open",
    }
    return mapping.get(request_type, "Open")


def request_type_to_source(request_type: str) -> str:
    mapping = {
        "Service": "Phone",
        "Scheduling": "Phone",
        "Customer Follow-Up": "Phone",
        "Vendor Follow-Up": "Internal",
        "Internal Request": "Internal",
        "General Support": "Phone",
    }
    return mapping.get(request_type, "Phone")


def clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()

def add_business_days(start_date: date, days: int) -> date:
    current = start_date
    added = 0
    while added < max(0, days):
        current = current.fromordinal(current.toordinal() + 1)
        if current.weekday() < 5:
            added += 1
    return current


def next_customer_followup(attempt_no: int) -> str:
    return add_business_days(date.today(), 2 if int(attempt_no or 0) >= 4 else 1).isoformat()


def compute_scheduling_route(flags: list[str]):
    selected = set(flags or [])
    if "Palm Springs" in selected:
        return "Moses Email", "Palm Springs"
    if "Shutters" in selected:
        return "Chat Routing", "Shutters"
    if "Arizona" in selected:
        return "Chat Routing", "Arizona"
    return "Direct Scheduling", "Standard"


@st.cache_data(show_spinner=False)
def load_region_map():
    for name in REGION_MAP_CANDIDATES:
        if os.path.exists(name):
            try:
                df = pd.read_csv(name, dtype=str)
                df.columns = [str(c).strip() for c in df.columns]

                if "ZIP_CODE" not in df.columns:
                    for candidate in ["ZIP", "ZIP CODE", "Zip", "Zip Code", "postal_code", "Postal Code"]:
                        if candidate in df.columns:
                            df = df.rename(columns={candidate: "ZIP_CODE"})
                            break

                if "REGION" not in df.columns:
                    for candidate in ["Region", "REGION_NAME", "Installer Region", "REGION NAME"]:
                        if candidate in df.columns:
                            df = df.rename(columns={candidate: "REGION"})
                            break

                if "ZIP_CODE" in df.columns:
                    df["ZIP_CODE"] = (
                        df["ZIP_CODE"].astype(str)
                        .str.extract(r"(\d{5})", expand=False)
                        .fillna(df["ZIP_CODE"].astype(str))
                    )

                if "REGION" in df.columns:
                    df["REGION"] = df["REGION"].astype(str).str.strip()

                return df
            except Exception:
                return pd.DataFrame()
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_scheduler_dashboard():
    # 1) Try explicit known file names first.
    for name in SCHEDULER_CANDIDATES:
        if os.path.exists(name):
            df = _load_scheduler_sheet_from_file(name)
            if not df.empty:
                return df

    # 2) Fallback: scan local xlsx/xlsm files for a compatible dashboard sheet.
    for p in sorted(Path(".").glob("*.xls*")):
        df = _load_scheduler_sheet_from_file(str(p))
        if not df.empty:
            return df

    return pd.DataFrame()


def normalize_region_for_dashboard(region_value):
    val = str(region_value or "").strip().upper()
    norm_val = _norm_col_name(val)
    for canonical, aliases in REGION_ALIASES.items():
        alias_upper = [a.upper() for a in aliases]
        alias_norm = [_norm_col_name(a) for a in aliases]
        if val in alias_upper or norm_val in alias_norm:
            return canonical
        if any(alias and alias in norm_val for alias in alias_norm):
            return canonical
    return val


def _norm_col_name(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _load_scheduler_sheet_from_file(path: str) -> pd.DataFrame:
    sheet_candidates = ["Regional Dashboard", "Estimator"]
    header_candidates = [1, 0, 2, 3]

    for sheet_name in sheet_candidates:
        for header_row in header_candidates:
            try:
                df = pd.read_excel(path, sheet_name=sheet_name, header=header_row, dtype=str)
            except Exception:
                continue

            df.columns = [str(c).strip() for c in df.columns]
            norm_map = {_norm_col_name(c): c for c in df.columns}

            name_col = norm_map.get("NAME")
            role_col = norm_map.get("ROLE")
            if not name_col or not role_col:
                continue

            norm_cols = [_norm_col_name(c) for c in df.columns]
            has_region = any(
                any(token in col for col in norm_cols)
                for token in [
                    "LALOSANGELES",
                    "OC",
                    "IERIVERSIDE",
                    "SANDIEGO",
                    "PALMSPRINGS",
                    "ARIZONA",
                    "LASVEGAS",
                    "VALLEY",
                    "VENTURA",
                ]
            )
            if not has_region:
                continue

            if name_col != "NAME":
                df = df.rename(columns={name_col: "NAME"})
            if role_col != "ROLE":
                df = df.rename(columns={role_col: "ROLE"})
            return df

    return pd.DataFrame()


def find_dashboard_region_column(df, region_value):
    target = normalize_region_for_dashboard(region_value)
    for col in df.columns:
        u = str(col).upper()
        nu = _norm_col_name(col)
        if target == "LA" and ("LA(" in u or "LALOSANGELES" in nu):
            return col
        if target == "OC" and (u.startswith("OC(") or nu == "OC" or nu.startswith("OC") or "ORANGECOUNTY" in nu):
            return col
        if target == "IE" and ("IE/RIVERSIDE" in u or "IERIVERSIDE" in nu):
            return col
        if target == "SD" and ("SAN DIEGO" in u or "SANDIEGO" in nu):
            return col
        if target == "PS" and ("PALM SPRINGS" in u or "PALMSPRINGS" in nu):
            return col
        if target == "ARIZONA" and "ARIZONA" in nu:
            return col
        if target == "LAS VEGAS" and ("LAS VEGAS" in u or "LASVEGAS" in nu):
            return col
        if target == "VALLEY" and "VALLEY" in nu:
            return col
        if target == "VENTURA" and "VENTURA" in nu:
            return col
    return None


def get_installer_priority_for_region(region_value):
    df = load_scheduler_dashboard()
    if df.empty or "NAME" not in df.columns or "ROLE" not in df.columns:
        return []

    col = find_dashboard_region_column(df, region_value)
    if not col:
        return []

    installers = []
    for row_order, (_, row) in enumerate(df.iterrows()):
        role_value = str(row.get("ROLE", "")).strip().upper()
        if "INSTALLER" not in role_value:
            continue

        marker_raw = str(row.get(col, "")).strip().upper()
        marker_match = re.search(r"\bP([123])\b", marker_raw)
        marker = f"P{marker_match.group(1)}" if marker_match else ""
        if marker in {"P1", "P2", "P3"}:
            installers.append(
                {
                    "installer": str(row.get("NAME", "")).strip(),
                    "priority": marker,
                    "row_order": row_order,
                }
            )

    priority_rank = {"P1": 1, "P2": 2, "P3": 3}
    return sorted(installers, key=lambda x: (priority_rank.get(x["priority"], 99), x["row_order"]))


def group_installers_by_priority(installers: list[dict]) -> dict[str, list[dict]]:
    grouped = {"P1": [], "P2": [], "P3": []}
    for item in installers:
        priority = clean_text(item.get("priority", ""))
        if priority in grouped:
            grouped[priority].append(item)
    for priority in grouped:
        grouped[priority] = sorted(grouped[priority], key=lambda x: x.get("row_order", 9999))
    return grouped


def build_scheduling_priority_copy_block(region_value: str, installers: list[dict], target_date: str) -> str:
    grouped = group_installers_by_priority(installers)
    lines = [
        "DIRECT SCHEDULING PRIORITY",
        f"Region: {clean_text(region_value)}",
        f"Suggested target date: {clean_text(target_date)}",
        "",
    ]
    for priority in ["P1", "P2", "P3"]:
        names = [
            clean_text(item.get("installer", ""))
            for item in grouped[priority]
            if clean_text(item.get("installer", ""))
        ]
        if names:
            lines.append(f"{priority}: {', '.join(names)}")
    return "\n".join(lines).strip()


def lookup_region_by_zip(zip_code):
    df = load_region_map()
    if df.empty or "ZIP_CODE" not in df.columns or "REGION" not in df.columns:
        return ""

    clean_zip = re.sub(r"\D", "", str(zip_code).strip())[:5]
    match = df[df["ZIP_CODE"].astype(str).str.strip() == clean_zip]
    if match.empty:
        return ""

    return str(match.iloc[0]["REGION"]).strip()



def ticket_exists(order_no: str, ticket_title: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) FROM tickets
        WHERE COALESCE(order_no, '') = COALESCE(?, '')
          AND COALESCE(ticket_title, '') = COALESCE(?, '')
        """,
        (order_no, ticket_title),
    )
    count = cur.fetchone()[0]
    conn.close()
    return count > 0

def normalize_import_colname(name: str) -> str:
    return re.sub(r"\s+", " ", str(name or "").strip().lower())


def suggest_ticket_import_map(columns: list[str]) -> dict:
    normalized = {normalize_import_colname(c): c for c in columns}
    mapping = {}
    for field in TICKET_IMPORT_FIELDS:
        mapping[field] = ""
        for alias in TICKET_IMPORT_ALIASES.get(field, []):
            k = normalize_import_colname(alias)
            if k in normalized:
                mapping[field] = normalized[k]
                break
    return mapping


def _safe_int(value, default=1) -> int:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return default
        parsed = int(float(str(value).strip()))
        return parsed if parsed >= 1 else default
    except Exception:
        return default


def bulk_import_tickets_df(df_raw: pd.DataFrame, column_map: dict) -> dict:
    inserted = 0
    duplicates = 0
    errors = []

    conn = get_conn()
    cur = conn.cursor()
    ts = now_ts()

    try:
        for row_idx, row in df_raw.iterrows():
            row_num = int(row_idx) + 2

            def pick(field, default=""):
                col = column_map.get(field, "")
                if not col:
                    return default
                val = row.get(col, default)
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return default
                return str(val).strip()

            ticket_title = pick("ticket_title")
            customer_name = pick("customer_name")
            order_no = pick("order_no")
            phone = pick("phone")
            issue_type = pick("issue_type", "Other") or "Other"
            request_type = pick("request_type", "Service") or "Service"
            ticket_source = pick("ticket_source", "Phone") or "Phone"
            assigned_by = pick("assigned_by", "Receptionist") or "Receptionist"
            assigned_agent = pick("assigned_agent", "Ed Torres") or "Ed Torres"
            hubspot_stage = pick("hubspot_stage", "New") or "New"
            internal_status = pick("internal_status", "Open") or "Open"
            attempt_no = _safe_int(pick("attempt_no", "1"), default=1)
            last_activity_date = pick("last_activity_date", today_str()) or today_str()
            next_followup_date = pick("next_followup_date", "")
            scheduling_flags = pick("scheduling_flags", "")
            notes_summary = pick("notes_summary", "")

            if not ticket_title:
                ticket_title = build_ticket_name(request_type, issue_type, customer_name, order_no)

            if not customer_name and not order_no and not ticket_title:
                errors.append({"row": row_num, "reason": "Missing key fields", "ticket_title": ""})
                continue

            if issue_type not in ISSUE_TYPES:
                issue_type = "Other"
            if request_type not in REQUEST_TYPES:
                request_type = "Service"
            if ticket_source not in TICKET_SOURCES:
                ticket_source = "Phone"
            if assigned_agent not in OWNERS:
                assigned_agent = "Ed Torres"
            if hubspot_stage not in HUBSPOT_STAGES:
                hubspot_stage = "New"
            if internal_status not in INTERNAL_STATUSES:
                internal_status = "Open"

            cur.execute(
                """
                SELECT COUNT(*) FROM tickets
                WHERE COALESCE(order_no, '') = COALESCE(?, '')
                  AND COALESCE(ticket_title, '') = COALESCE(?, '')
                """,
                (order_no, ticket_title),
            )
            exists = cur.fetchone()[0] > 0
            if exists:
                duplicates += 1
                continue

            cur.execute(
                """
                INSERT INTO tickets (
                    cc_id,
                    ticket_title, customer_name, order_no, phone,
                    issue_type, request_type, ticket_source,
                    assigned_by, assigned_agent, hubspot_stage, internal_status,
                    attempt_no, last_activity_date, next_followup_date,
                    scheduling_flags, notes_summary, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    get_next_cc_id(conn),
                    ticket_title,
                    customer_name,
                    order_no,
                    phone,
                    issue_type,
                    request_type,
                    ticket_source,
                    assigned_by,
                    assigned_agent,
                    hubspot_stage,
                    internal_status,
                    attempt_no,
                    last_activity_date,
                    next_followup_date,
                    scheduling_flags,
                    notes_summary,
                    ts,
                    ts,
                ),
            )
            inserted += 1

        conn.commit()
    finally:
        conn.close()

    return {
        "inserted": inserted,
        "duplicates": duplicates,
        "errors": errors,
    }


def render_ticket_bulk_import_section():
    st.subheader("Bulk Import Tickets (Excel/CSV)")
    st.caption("Upload your manual tracker and map columns before importing.")

    uploaded = st.file_uploader(
        "Upload Ticket File",
        type=["xlsx", "csv"],
        key="ticket_bulk_upload_file",
        help="Supported: .xlsx or .csv",
    )

    if not uploaded:
        return

    try:
        if uploaded.name.lower().endswith(".csv"):
            df_raw = pd.read_csv(uploaded)
        else:
            df_raw = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"Could not read file: {e}")
        return

    if df_raw.empty:
        st.warning("Uploaded file is empty.")
        return

    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    st.write("Detected columns:")
    st.dataframe(df_raw.head(20), use_container_width=True)

    suggested = suggest_ticket_import_map(list(df_raw.columns))
    options = [""] + list(df_raw.columns)

    st.markdown("### Column Mapping")
    c1, c2, c3 = st.columns(3)
    mapped = {}

    for i, field in enumerate(TICKET_IMPORT_FIELDS):
        col = [c1, c2, c3][i % 3]
        with col:
            default_col = suggested.get(field, "")
            default_idx = options.index(default_col) if default_col in options else 0
            mapped[field] = st.selectbox(
                field,
                options,
                index=default_idx,
                key=f"import_map_{field}",
            )

    if st.button("Run Bulk Import", key="run_ticket_bulk_import"):
        result = bulk_import_tickets_df(df_raw, mapped)
        st.success(
            f"Import complete. Inserted: {result['inserted']} | "
            f"Duplicates skipped: {result['duplicates']} | "
            f"Errors: {len(result['errors'])}"
        )

        if result["errors"]:
            err_df = pd.DataFrame(result["errors"])
            st.dataframe(err_df, use_container_width=True)
            st.download_button(
                "Download Import Errors CSV",
                data=err_df.to_csv(index=False).encode("utf-8"),
                file_name="ticket_import_errors.csv",
                mime="text/csv",
                key="download_ticket_import_errors",
            )

        st.rerun()


# =========================================================
# PARSING
# =========================================================

def extract_prefill(raw_text: str, default_source: str = "Phone") -> dict:
    text = clean_text(raw_text)
    lower = text.lower()

    customer_name = ""
    order_no = ""
    phone = ""
    issue_type = "Other"
    request_type = "Service"
    ticket_source = default_source

    order_patterns = [
        r"(?:service order|order\s*#?|order number|original order#?|original order number)\s*[:#]?\s*([A-Za-z0-9\-]+)",
        r"\b(O-\d{4,})\b",
    ]
    for pattern in order_patterns:
        m = re.search(pattern, text, re.I)
        if m:
            order_no = m.group(1).strip()
            break

    phone_patterns = [
        r"phone\s*[:#]?\s*(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})",
        r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})",
    ]
    for pattern in phone_patterns:
        m = re.search(pattern, text, re.I)
        if m:
            phone = m.group(1).strip()
            break

    name_patterns = [
        r"customer\s*[:\-]\s*([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)+)",
        r"cx\s*[:\-]\s*([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)+)",
        r"name\s*[:\-]\s*([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)+)",
        r"\bfor\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)+)\b",
    ]
    for pattern in name_patterns:
        m = re.search(pattern, text)
        if m:
            customer_name = m.group(1).strip()
            break

    if not customer_name:
        possible_names = re.findall(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b", text)
        skip_words = {
            "LA Custom",
            "Custom Blinds",
            "Superior Blinds",
            "Project Coordinator",
            "Palm Springs",
        }
        for nm in possible_names:
            if nm not in skip_words and len(nm.split()) >= 2:
                customer_name = nm.strip()
                break

    if "vendor" in lower:
        request_type = "Vendor Follow-Up"
        ticket_source = "Internal"
    elif "schedule" in lower or "appointment" in lower:
        request_type = "Scheduling"
    elif "follow-up" in lower or "follow up" in lower:
        request_type = "Customer Follow-Up"
    elif "internal" in lower or "supervisor" in lower:
        request_type = "Internal Request"

    if "motor" in lower and ("not working" in lower or "stuck" in lower or "not going" in lower):
        issue_type = "Motor Issue"
    elif "fabric" in lower or "fraying" in lower or "hole" in lower:
        issue_type = "Fabric Issue"
    elif "guidewire" in lower:
        issue_type = "Guidewire Issue"
    elif "anchor" in lower:
        issue_type = "Anchor Issue"
    elif "charger" in lower:
        issue_type = "Charger Reconnection"
    elif "remote" in lower or "pairing" in lower or "programming" in lower:
        issue_type = "Remote / Programming Issue"
    elif "hold down bracket" in lower or "hold-down bracket" in lower or "bracket" in lower:
        issue_type = "Install Concern"
    elif "schedule" in lower or "appointment" in lower:
        issue_type = "Scheduling Request"
    elif "shutter" in lower:
        issue_type = "Shutter Issue"

    return {
        "customer_name": customer_name,
        "order_no": order_no,
        "phone": phone,
        "issue_type": issue_type,
        "request_type": request_type,
        "ticket_source": ticket_source,
    }
def split_possible_multi_ticket_block(raw_text: str) -> list[str]:
    text = clean_text(raw_text)
    if not text:
        return []

    blocks = re.split(r"\n\s*\n+", text)
    cleaned = [b.strip() for b in blocks if b.strip()]
    if len(cleaned) > 1:
        return cleaned

    line_blocks = []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    current = []

    for line in lines:
        looks_like_new_case = (
            bool(re.search(r"order\s*#?|order number|customer\s*:|cx\s*:|name\s*:", line, re.I))
            and len(current) >= 1
        )
        if looks_like_new_case:
            line_blocks.append("\n".join(current).strip())
            current = [line]
        else:
            current.append(line)

    if current:
        line_blocks.append("\n".join(current).strip())

    return [b for b in line_blocks if b.strip()]


# =========================================================
# BUILDER AUTOFILL
# =========================================================

def apply_builder_pending_autofill():
    pending = st.session_state.pop("builder_pending_autofill", None)
    if not pending:
        return

    if pending.get("customer_name"):
        st.session_state["builder_customer_name"] = pending["customer_name"]

    if pending.get("order_no"):
        st.session_state["builder_order_no"] = pending["order_no"]

    if pending.get("phone"):
        st.session_state["builder_phone"] = pending["phone"]

    if pending.get("ticket_source") in TICKET_SOURCES:
        st.session_state["builder_ticket_source"] = pending["ticket_source"]

    if pending.get("issue_type") in ISSUE_TYPES:
        st.session_state["builder_issue_type"] = pending["issue_type"]

    if pending.get("request_type") in REQUEST_TYPES:
        request_type = pending["request_type"]
        st.session_state["builder_request_type"] = request_type
        st.session_state["builder_internal_status"] = default_internal_status(request_type)
        st.session_state["builder_ticket_source"] = request_type_to_source(request_type)

        if request_type == "Vendor Follow-Up":
            st.session_state["builder_hubspot_stage"] = "Waiting on Order Readiness"
            st.session_state["builder_internal_status"] = "Pending Internal"
        elif request_type == "Scheduling":
            st.session_state["builder_hubspot_stage"] = "Scheduling Pending"
            st.session_state["builder_internal_status"] = "Pending Scheduling"
        elif request_type == "Customer Follow-Up":
            st.session_state["builder_hubspot_stage"] = "Awaiting Customer Response"
            st.session_state["builder_internal_status"] = "Pending Customer"
# =========================================================
# CONTENT BUILDERS
# =========================================================

def build_ticket_name(request_type, issue_type, customer_name, order_no):
    parts = [request_type, issue_type]
    if order_no:
        parts.append(str(order_no))
    if customer_name:
        parts.append(customer_name)
    return " | ".join(parts)


def build_crm_note(raw_details, request_type, issue_type):
    details = clean_text(raw_details)
    if details:
        paragraphs = [p.strip() for p in details.split("\n\n") if p.strip()]
        if len(paragraphs) >= 3:
            problem = paragraphs[0]
            resolution = paragraphs[1]
            expectation = paragraphs[2]
        else:
            problem = details
            resolution = (
                f"I reviewed the details and prepared the appropriate next step for this "
                f"{request_type.lower()} request."
            )
            expectation = "I will monitor the account and follow up based on the current status."
    else:
        problem = f"Customer reported a {issue_type.lower()} related to a {request_type.lower()} request."
        resolution = "I reviewed the details and prepared the appropriate next step."
        expectation = "I will monitor the account and follow up based on the current status."

    return (
        f"PROBLEM: {problem}\n\n"
        f"RESOLUTION: {resolution}\n\n"
        f"EXPECTATION: {expectation}"
    )

def build_hubspot_summary(
    customer_name,
    order_no,
    phone,
    request_type,
    issue_type,
    assigned_by,
    assigned_agent,
    hubspot_stage,
    internal_status,
    attempt_no,
    raw_details,
):
    details = clean_text(raw_details)
    if details:
        return (
            f"PROBLEM: {details}\n\n"
            f"RESOLUTION: I created/reviewed this {request_type.lower()} request. "
            f"Assigned by: {assigned_by or 'N/A'}. Assigned agent: {assigned_agent or 'N/A'}. "
            f"Attempt #: {attempt_no}.\n\n"
            f"EXPECTATION: Current HubSpot stage is {hubspot_stage}. Internal status is {internal_status}. "
            f"I will continue follow-up as needed."
        )

    return (
        f"PROBLEM: {customer_name or 'Customer'} reported a {issue_type.lower()} issue. "
        f"Order #{order_no or 'N/A'}. Phone: {phone or 'N/A'}.\n\n"
        f"RESOLUTION: I created/reviewed this {request_type.lower()} request. "
        f"Assigned by: {assigned_by or 'N/A'}. Assigned agent: {assigned_agent or 'N/A'}. "
        f"Attempt #: {attempt_no}.\n\n"
        f"EXPECTATION: Current HubSpot stage is {hubspot_stage}. Internal status is {internal_status}. "
        f"I will continue follow-up as needed."
    )

def build_customer_sms(customer_name, order_no, request_type, issue_type):
    name = customer_name or "there"
    order_text = f" #{order_no}" if order_no else ""

    if request_type == "Vendor Follow-Up":
        context_line = "I'm following up on the parts/vendor request related to your order."
    elif issue_type == "Install Concern":
        context_line = "I'm reaching out regarding the installation-related items noted on your order."
    else:
        context_line = f"I'm reaching out regarding your {issue_type.lower()}."

    return (
        f"Hi {name}, this is Ed Torres. I'm the Project Coordinator for your order{order_text}. "
        f"{context_line} "
        f"I'm currently reviewing the next steps for this {request_type.lower()} request. "
        f"You can text me here or call (800) 533-7957 ext. 1111 if you have any questions. "
        f"Thanks for choosing LA Custom Blinds!"
    )

def build_customer_email(customer_name, order_no, issue_type, raw_details):
    name = customer_name or "Customer"
    detail_line = clean_text(raw_details) if clean_text(raw_details) else f"I'm writing regarding the {issue_type.lower()} concern."
    order_text = f" #{order_no}" if order_no else ""

    subject = f"LACB | {name}"
    body = (
        f"Hi {name},\n\n"
        f"{detail_line}\n\n"
        f"I'm reviewing the next steps for your order{order_text} and will continue to monitor the account.\n\n"
        f"Best regards,\n"
        f"Ed Torres\n"
        f"Project Coordinator | LA Custom Blinds\n"
        f"1-800-533-7957 ext. 1111 | (949) 617-1941\n"
        f"etorres@lacustomblinds.com\n\n"
        f"Any further questions or concerns, you can reply to this email,\n"
        f"text me at (949) 617-1941 or call 1-800-533-7957 ext. 1111."
    )
    return subject, body


# =========================================================
# TICKET SAVE / LOAD
# =========================================================

def save_generated_ticket_to_tracker(
    ticket_name,
    customer_name,
    order_no,
    phone,
    issue_type,
    request_type,
    ticket_source,
    assigned_by,
    assigned_agent,
    hubspot_stage,
    internal_status,
    attempt_no,
    notes_summary,
):
    if ticket_exists(order_no, ticket_name):
        return False, "Duplicate ticket detected. Ticket was not saved."

    conn = get_conn()
    cur = conn.cursor()

    ts = now_ts()

    cur.execute(
        """
        INSERT INTO tickets (
            cc_id,
            ticket_title,
            customer_name,
            order_no,
            phone,
            issue_type,
            request_type,
            ticket_source,
            assigned_by,
            assigned_agent,
            hubspot_stage,
            internal_status,
            attempt_no,
            last_activity_date,
            next_followup_date,
            scheduling_flags,
            notes_summary,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            get_next_cc_id(conn),
            ticket_name,
            customer_name,
            order_no,
            phone,
            issue_type,
            request_type,
            ticket_source,
            assigned_by,
            assigned_agent,
            hubspot_stage,
            internal_status,
            attempt_no,
            today_str(),
            "",
            "",
            notes_summary,
            ts,
            ts,
        ),
    )

    conn.commit()
    conn.close()
    return True, "Ticket draft generated and saved to tracker."


def get_all_tickets_df() -> pd.DataFrame:
    conn = get_conn()
    df = _db_read_sql_query(
        """
        SELECT *
        FROM tickets
        ORDER BY id DESC
        """,
        conn,
    )
    conn.close()
    return df


def get_ticket_by_id(ticket_id: int) -> dict | None:
    if not ticket_id:
        return None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,))
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description] if cur.description else []
    conn.close()

    if not row:
        return None

    return dict(zip(columns, row))


def update_ticket_basic(ticket_id: int, values: dict):
    if not ticket_id:
        return

    keys = list(values.keys())
    assignments = ", ".join([f"{k} = ?" for k in keys]) + ", updated_at = ?"
    params = [values[k] for k in keys] + [now_ts(), ticket_id]

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE tickets SET {assignments} WHERE id = ?", params)
    conn.commit()
    conn.close()


def delete_ticket(ticket_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM activities WHERE ticket_id = ?", (ticket_id,))
    cur.execute("DELETE FROM tickets WHERE id = ?", (ticket_id,))
    conn.commit()
    conn.close()


# =========================================================
# ACTIVITY LOG
# =========================================================

def add_activity(ticket_id, logging_agent, assigned_owner, action_type, result_type, notes):
    ticket = get_ticket_by_id(ticket_id)
    if not ticket:
        return

    if isinstance(action_type, list):
        action_labels = [clean_text(x) for x in action_type if clean_text(x)]
    else:
        single_action = clean_text(action_type)
        action_labels = [single_action] if single_action else []

    action_value = ", ".join(action_labels)
    action_set = set(action_labels)

    result_type = clean_text(result_type)
    notes = clean_text(notes)

    ts = now_ts()
    today = today_str()

    attempt_no = int(ticket.get("attempt_no") or 0)
    current_status = str(ticket.get("internal_status") or "Open")
    hubspot_stage = str(ticket.get("hubspot_stage") or "New")
    new_status = current_status
    next_followup_date = clean_text(ticket.get("next_followup_date") or add_business_days(date.today(), 1).isoformat())

    if current_status in {"Waiting on Customer", "Pending Customer", "Open"} and "Outbound Call" in action_set:
        attempt_no += 1
        next_followup_date = next_customer_followup(attempt_no)
        new_status = "Ready to Close" if attempt_no >= 4 else "Waiting on Customer"

    result_map = {
        "Waiting on customer": "Waiting on Customer",
        "Waiting on vendor": "Waiting on Vendor",
        "Waiting on DC": "Waiting on DC",
        "Waiting on internal department": "Waiting on Internal Dept",
        "Resolved": "Closed",
    }
    if result_type in result_map:
        new_status = result_map[result_type]

    if "Appointment Scheduled" in action_set:
        new_status = "Scheduled"

    if "Ticket Closed" in action_set or result_type == "Resolved":
        new_status = "Closed"
        hubspot_stage = "Closed"
        next_followup_date = ""

    if new_status in {"Waiting on Vendor", "Waiting on DC", "Waiting on Internal Dept", "Pending Internal"}:
        next_followup_date = add_business_days(date.today(), 1).isoformat()

    if new_status in {"Waiting on Customer", "Pending Customer"}:
        hubspot_stage = "Awaiting Customer Response"
    elif new_status in {"Waiting on Vendor", "Waiting on DC", "Waiting on Internal Dept", "Pending Internal"}:
        hubspot_stage = "Awaiting Internal Response"
    elif new_status in {"Scheduled", "Pending Scheduling"}:
        hubspot_stage = "Scheduling Pending"

    conn = get_conn()
    cur = conn.cursor()
    # Protect against accidental double-submit from rapid reruns/clicks.
    cur.execute(
        """
        SELECT logging_agent, assigned_owner, action_type, result_type, notes, created_at
        FROM activities
        WHERE ticket_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (ticket_id,),
    )
    last = cur.fetchone()
    if last:
        same_payload = (
            clean_text(last[0]) == clean_text(logging_agent)
            and clean_text(last[1]) == clean_text(assigned_owner)
            and clean_text(last[2]) == clean_text(action_value)
            and clean_text(last[3]) == clean_text(result_type)
            and clean_text(last[4]) == clean_text(notes)
        )
        if same_payload and clean_text(last[5]):
            try:
                prev_ts = datetime.strptime(clean_text(last[5]), "%Y-%m-%d %H:%M:%S")
                curr_ts = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                if (curr_ts - prev_ts).total_seconds() <= 8:
                    conn.close()
                    return False
            except Exception:
                pass

    cur.execute(
        """
        INSERT INTO activities (
            ticket_id,
            activity_date,
            logging_agent,
            assigned_owner,
            io_channel,
            customer_name,
            order_no,
            phone,
            email,
            ticket_update_name,
            follow_up_case_id,
            action_type,
            result_type,
            notes,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticket_id,
            ts,
            logging_agent,
            assigned_owner,
            "",
            clean_text(ticket.get("customer_name", "")),
            clean_text(ticket.get("order_no", "")),
            clean_text(ticket.get("phone", "")),
            "",
            clean_text(ticket.get("ticket_title", "")),
            get_or_create_follow_up_case_id(
                conn,
                customer_name=clean_text(ticket.get("customer_name", "")),
                order_no=clean_text(ticket.get("order_no", "")),
                phone=clean_text(ticket.get("phone", "")),
                ticket_id=ticket_id,
            ),
            action_value,
            result_type,
            notes,
            ts,
        ),
    )

    cur.execute(
        """
        UPDATE tickets
        SET
            assigned_agent = ?,
            attempt_no = ?,
            last_activity_date = ?,
            next_followup_date = ?,
            internal_status = ?,
            hubspot_stage = ?,
            notes_summary = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            assigned_owner,
            max(1, attempt_no),
            today,
            next_followup_date,
            new_status,
            hubspot_stage,
            notes,
            ts,
            ticket_id,
        ),
    )

    conn.commit()
    conn.close()


def save_inbound_outbound_activity_entry(
    logging_agent: str,
    io_channels: list[str],
    action_types: list[str],
    result_types: list[str],
    customer_name: str,
    order_no: str,
    phone: str,
    email: str,
    ticket_update_name: str,
    follow_up_date: str,
    notes: str,
):
    channel_value = ", ".join([clean_text(x) for x in io_channels if clean_text(x)])
    action_value = ", ".join([clean_text(x) for x in action_types if clean_text(x)])
    result_value = ", ".join([clean_text(x) for x in result_types if clean_text(x)])
    if not clean_text(channel_value):
        return False, "Channel is required."
    if not any([action_value, clean_text(result_value), clean_text(notes), clean_text(customer_name), clean_text(order_no)]):
        return False, "Add at least an action, outcome, note, customer, or order number."

    conn = get_conn()
    cur = conn.cursor()
    ts = now_pacific_ts()

    cur.execute(
        """
        INSERT INTO activities (
            ticket_id,
            activity_date,
            logging_agent,
            assigned_owner,
            io_channel,
            customer_name,
            order_no,
            phone,
            email,
            ticket_update_name,
            follow_up_date,
            follow_up_case_id,
            action_type,
            result_type,
            notes,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            None,
            ts,
            clean_text(logging_agent),
            clean_text(logging_agent),
            clean_text(channel_value),
            clean_text(customer_name),
            clean_text(order_no),
            clean_text(phone),
            clean_text(email),
            clean_text(ticket_update_name),
            clean_text(follow_up_date),
            get_or_create_follow_up_case_id(
                conn,
                customer_name=clean_text(customer_name),
                order_no=clean_text(order_no),
                phone=clean_text(phone),
            ),
            action_value,
            clean_text(result_value),
            clean_text(notes),
            ts,
        ),
    )
    conn.commit()
    conn.close()
    return True, "Inbound / outbound activity saved."


def update_follow_up_activity_entry(activity_id: int, result_types: list[str], follow_up_date: str, ticket_id=None):
    result_value = ", ".join([clean_text(x) for x in result_types if clean_text(x)])
    if not activity_id:
        return False, "Activity entry not found."

    conn = get_conn()
    cur = conn.cursor()
    ts = now_ts()
    cur.execute(
        "UPDATE activities SET result_type = ?, follow_up_date = ? WHERE id = ?",
        (clean_text(result_value), clean_text(follow_up_date), int(activity_id)),
    )

    if ticket_id:
        next_followup = "" if _is_terminal_outcome(result_value) else clean_text(follow_up_date)
        cur.execute(
            "UPDATE tickets SET next_followup_date = ?, updated_at = ? WHERE id = ?",
            (next_followup, ts, int(ticket_id)),
        )

    conn.commit()
    conn.close()
    return True, "Follow-up entry updated."


def update_follow_up_group_entries(group_key: str, result_types: list[str], follow_up_date: str):
    result_value = ", ".join([clean_text(x) for x in result_types if clean_text(x)])
    group_key = clean_text(group_key)
    if not group_key:
        return False, "Follow-up group not found."

    conn = get_conn()
    cur = conn.cursor()

    if group_key.startswith("row:"):
        row_id = _safe_optional_int(group_key.split(":", 1)[1])
        if not row_id:
            conn.close()
            return False, "Follow-up group not found."
        cur.execute("SELECT id, ticket_id FROM activities WHERE id = ?", (int(row_id),))
    else:
        cur.execute(
            "SELECT id, ticket_id FROM activities WHERE follow_up_case_id = ?",
            (group_key,),
        )

    rows = cur.fetchall()
    if not rows:
        conn.close()
        return False, "No linked activity entries found for this group."

    activity_ids = sorted({_safe_optional_int(row["id"]) for row in rows if _safe_optional_int(row["id"])})
    ticket_ids = sorted({_safe_optional_int(row["ticket_id"]) for row in rows if _safe_optional_int(row["ticket_id"])})
    cleaned_follow_up_date = "" if _is_terminal_outcome(result_value) else clean_text(follow_up_date)

    cur.execute(
        f"UPDATE activities SET result_type = ?, follow_up_date = ? WHERE id IN ({','.join(['?'] * len(activity_ids))})",
        [clean_text(result_value), cleaned_follow_up_date, *activity_ids],
    )

    if ticket_ids:
        cur.execute(
            f"UPDATE tickets SET next_followup_date = ?, updated_at = ? WHERE id IN ({','.join(['?'] * len(ticket_ids))})",
            [cleaned_follow_up_date, now_ts(), *ticket_ids],
        )

    conn.commit()
    conn.close()
    return True, f"Updated {len(activity_ids)} linked activity entr{'y' if len(activity_ids) == 1 else 'ies'}."


def update_activity_entry_fields(
    activity_id: int,
    io_channels: list[str],
    action_types: list[str],
    result_types: list[str],
    follow_up_date: str,
    notes: str,
    ticket_id=None,
):
    if not activity_id:
        return False, "Select an activity entry first."

    channel_value = ", ".join([clean_text(x) for x in io_channels if clean_text(x)])
    action_value = ", ".join([clean_text(x) for x in action_types if clean_text(x)])
    result_value = ", ".join([clean_text(x) for x in result_types if clean_text(x)])

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE activities
        SET io_channel = ?, action_type = ?, result_type = ?, follow_up_date = ?, notes = ?
        WHERE id = ?
        """,
        (
            clean_text(channel_value),
            clean_text(action_value),
            clean_text(result_value),
            clean_text(follow_up_date),
            clean_text(notes),
            int(activity_id),
        ),
    )

    if ticket_id:
        next_followup = "" if _is_terminal_outcome(result_value) else clean_text(follow_up_date)
        cur.execute(
            "UPDATE tickets SET next_followup_date = ?, updated_at = ? WHERE id = ?",
            (next_followup, now_ts(), int(ticket_id)),
        )

    conn.commit()
    conn.close()
    return True, "Activity entry updated."


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31] or "Sheet1")
    return output.getvalue()

def get_activities_for_ticket(ticket_id: int) -> pd.DataFrame:
    conn = get_conn()
    df = _db_read_sql_query(
        """
        SELECT id, activity_date, logging_agent, assigned_owner, action_type, result_type, notes
        FROM activities
        WHERE ticket_id = ?
        ORDER BY id DESC
        """,
        conn,
        params=(ticket_id,),
    )
    conn.close()
    return df


def delete_activity_by_id(activity_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM activities WHERE id = ?", (int(activity_id),))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def cleanup_duplicate_activities_for_ticket(ticket_id: int, window_seconds: int = 8) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, logging_agent, assigned_owner, action_type, result_type, notes, created_at
        FROM activities
        WHERE ticket_id = ?
        ORDER BY id ASC
        """,
        (ticket_id,),
    )
    rows = cur.fetchall()
    if not rows:
        conn.close()
        return 0

    seen = {}
    to_delete = []

    for row in rows:
        rid = int(row[0])
        key = (
            clean_text(row[1]),
            clean_text(row[2]),
            clean_text(row[3]),
            clean_text(row[4]),
            clean_text(row[5]),
        )
        ts_str = clean_text(row[6])
        ts = None
        if ts_str:
            try:
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                ts = None

        if key not in seen:
            seen[key] = (rid, ts)
            continue

        prev_id, prev_ts = seen[key]
        if ts is not None and prev_ts is not None:
            if (ts - prev_ts).total_seconds() <= window_seconds:
                to_delete.append(rid)
            else:
                seen[key] = (rid, ts)
        else:
            to_delete.append(rid)

    deleted = 0
    if to_delete:
        qmarks = ",".join(["?"] * len(to_delete))
        cur.execute(f"DELETE FROM activities WHERE id IN ({qmarks})", tuple(to_delete))
        deleted = cur.rowcount
        conn.commit()
    conn.close()
    return int(deleted)


# =========================================================
# FOLLOW-UP LOGIC
# =========================================================


def add_manual_tally(tally_date: str, owner: str, metric_type: str, quantity: int, notes: str = ""):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO manual_tallies (tally_date, owner, metric_type, quantity, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (tally_date, owner, metric_type, int(quantity), notes, now_ts()),
    )
    conn.commit()
    conn.close()


def get_manual_tallies_df() -> pd.DataFrame:
    conn = get_conn()
    try:
        df = _db_read_sql_query("SELECT * FROM manual_tallies ORDER BY id DESC", conn)
    except Exception:
        df = pd.DataFrame(columns=["id", "tally_date", "owner", "metric_type", "quantity", "notes", "created_at"])
    conn.close()
    return df


def get_manual_tally_total(tally_date: str, owner: str, metric_type: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(quantity), 0)
        FROM manual_tallies
        WHERE tally_date = ? AND owner = ? AND metric_type = ?
        """,
        (tally_date, owner, metric_type),
    )
    total = cur.fetchone()[0] or 0
    conn.close()
    return int(total)


def apply_manual_tally_adjustment(tally_date: str, owner: str, metric_type: str, delta: int, note: str):
    current_total = get_manual_tally_total(tally_date, owner, metric_type)
    if delta < 0 and current_total + delta < 0:
        return False, f"Cannot reduce below 0 for {metric_type} on {tally_date} ({owner})."

    add_manual_tally(tally_date, owner, metric_type, delta, note)
    return True, "Manual tally updated."


def get_custom_prompt_templates() -> dict:
    conn = get_conn()
    try:
        df = _db_read_sql_query(
            """
            SELECT template_name, prompt_text
            FROM assistant_prompt_templates
            ORDER BY template_name ASC
            """,
            conn,
        )
    except Exception:
        df = pd.DataFrame(columns=["template_name", "prompt_text"])
    conn.close()

    result = {}
    for _, row in df.iterrows():
        result[str(row["template_name"])] = str(row["prompt_text"])
    return result


def save_custom_prompt_template(template_name: str, prompt_text: str) -> tuple[bool, str]:
    name = clean_text(template_name)
    prompt = clean_text(prompt_text)
    if not name:
        return False, "Template name is required."
    if not prompt:
        return False, "Prompt text is empty. Enter prompt text first."

    if name in ASSISTANT_PROMPT_TEMPLATES:
        return False, "That name matches a built-in template. Use a different name."

    conn = get_conn()
    cur = conn.cursor()
    ts = now_ts()
    cur.execute(
        """
        INSERT INTO assistant_prompt_templates (template_name, prompt_text, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(template_name) DO UPDATE SET
            prompt_text = excluded.prompt_text,
            updated_at = excluded.updated_at
        """,
        (name, prompt, ts, ts),
    )
    conn.commit()
    conn.close()
    return True, f"Template '{name}' saved."


def delete_custom_prompt_template(template_name: str) -> tuple[bool, str]:
    name = clean_text(template_name)
    if not name:
        return False, "Select a custom template to delete."
    if name in ASSISTANT_PROMPT_TEMPLATES:
        return False, "Built-in templates cannot be deleted."

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM assistant_prompt_templates WHERE template_name = ?", (name,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()

    if deleted == 0:
        return False, "Template not found."
    return True, f"Template '{name}' deleted."


def get_all_prompt_templates() -> dict:
    combined = {}
    for name, prompt in ASSISTANT_PROMPT_TEMPLATES.items():
        combined[f"Built-in: {name}"] = prompt

    custom = get_custom_prompt_templates()
    for name, prompt in custom.items():
        combined[f"Custom: {name}"] = prompt
    return combined


def get_kpi_date_range(time_filter: str) -> tuple[str, str]:
    today = date.today()
    if time_filter == "Daily":
        start = today
        end = today
    elif time_filter == "Weekly":
        start = today.fromordinal(today.toordinal() - today.weekday())
        end = start.fromordinal(start.toordinal() + 6)
    else:
        start = today.replace(day=1)
        if today.month == 12:
            next_month = today.replace(year=today.year + 1, month=1, day=1)
        else:
            next_month = today.replace(month=today.month + 1, day=1)
        end = next_month.fromordinal(next_month.toordinal() - 1)
    return start.isoformat(), end.isoformat()
def recommended_next_action(attempt_no: int, status: str) -> str:
    status = str(status or "").strip()
    attempt_no = int(attempt_no or 0)

    if status in {"Waiting on Customer", "Pending Customer", "Open"}:
        if attempt_no <= 2:
            return "Call + SMS"
        if attempt_no == 3:
            return "Call + SMS + Email"
        return "Call + SMS + Final Email / Review closure"
    if status in {"Waiting on Vendor"}:
        return "Vendor follow-up"
    if status in {"Waiting on DC"}:
        return "DC follow-up"
    if status in {"Waiting on Internal Dept", "Pending Internal"}:
        return "Internal dept follow-up"
    if status in {"Ready to Close"}:
        return "Review and close"
    if status in {"Scheduled", "Pending Scheduling"}:
        return "Monitor appointment"

    if attempt_no <= 1:
        return "Call + SMS"
    if attempt_no == 2:
        return "Call + SMS"
    if attempt_no == 3:
        return "Call + SMS + Email"
    return "Final review / close"


def _parse_app_timestamp(value: str) -> datetime | None:
    text = clean_text(value)
    if not text:
        return None
    text = re.sub(r"\s+[A-Z]{3}$", "", text)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _safe_optional_int(value):
    text = clean_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _normalize_followup_name(value: str) -> str:
    return re.sub(r"\s+", " ", clean_text(value).lower()).strip()


def _normalize_followup_phone(value: str) -> str:
    digits = re.sub(r"\D", "", clean_text(value))
    return digits[-10:] if len(digits) >= 10 else digits


def _normalize_followup_order(value: str) -> str:
    return re.sub(r"\s+", "", clean_text(value).lower())


def _split_multi_value(value: str) -> list[str]:
    return [clean_text(part) for part in str(value or "").split(",") if clean_text(part)]


def _is_terminal_outcome(value: str) -> bool:
    tokens = [token.lower() for token in _split_multi_value(value)]
    return any(token in TERMINAL_FOLLOWUP_OUTCOMES for token in tokens)


def _is_followup_outcome(value: str) -> bool:
    tokens = [token.lower() for token in _split_multi_value(value)]
    return any(token and token not in TERMINAL_FOLLOWUP_OUTCOMES for token in tokens)


def _is_resolved_activity(action_type: str, result_type: str, internal_status: str = "") -> bool:
    action = clean_text(action_type).lower()
    status = clean_text(internal_status).lower()
    return (
        _is_terminal_outcome(result_type)
        or "ticket closed" in action
        or status == "closed"
    )


def _build_followup_activity_groups():
    conn = get_conn()
    df = _db_read_sql_query(
        """
        SELECT
            a.id,
            a.ticket_id,
            a.follow_up_case_id,
            COALESCE(a.ticket_update_name, t.ticket_title) AS ticket_title,
            COALESCE(a.customer_name, t.customer_name) AS customer_name,
            COALESCE(a.order_no, t.order_no) AS order_no,
            COALESCE(a.phone, t.phone) AS phone,
            a.email,
            a.io_channel,
            a.activity_date,
            a.logging_agent,
            a.assigned_owner,
            a.action_type,
            a.result_type,
            a.follow_up_date,
            a.notes,
            t.next_followup_date,
            t.internal_status
        FROM activities a
        LEFT JOIN tickets t ON t.id = a.ticket_id
        ORDER BY a.id ASC
        """,
        conn,
    )
    conn.close()

    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    grouped_rows = {}
    for _, row in df.iterrows():
        case_id = clean_text(row.get("follow_up_case_id", ""))
        if not case_id:
            identifiers = _activity_case_identifiers(
                customer_name=row.get("customer_name", ""),
                order_no=row.get("order_no", ""),
                phone=row.get("phone", ""),
                ticket_id=row.get("ticket_id", ""),
            )
            case_id = identifiers[0] if identifiers else f"row:{_safe_optional_int(row.get('id', 0)) or 0}"
        grouped_rows.setdefault(case_id, []).append(row.to_dict())

    open_groups = []
    resolved_groups = []
    timelines = {}

    for case_id, rows in grouped_rows.items():
        rows = sorted(
            rows,
            key=lambda r: (_parse_app_timestamp(r.get("activity_date", "")) or datetime.min, int(r.get("id", 0))),
        )
        followup_rows = [r for r in rows if _is_followup_outcome(r.get("result_type", ""))]
        if not followup_rows:
            continue

        latest_followup_dt = max(
            (_parse_app_timestamp(r.get("activity_date", "")) or datetime.min) for r in followup_rows
        )
        resolved_rows = [
            r for r in rows
            if _is_resolved_activity(r.get("action_type", ""), r.get("result_type", ""), r.get("internal_status", ""))
            and (_parse_app_timestamp(r.get("activity_date", "")) or datetime.min) >= latest_followup_dt
        ]
        is_resolved = bool(resolved_rows)

        latest_row = rows[-1]
        first_dt = _parse_app_timestamp(rows[0].get("activity_date", ""))
        last_dt = _parse_app_timestamp(latest_row.get("activity_date", ""))
        resolution_dt = max(
            (_parse_app_timestamp(r.get("activity_date", "")) or datetime.min) for r in resolved_rows
        ) if resolved_rows else None

        def latest_nonempty(field_name: str) -> str:
            for row in reversed(rows):
                value = clean_text(row.get(field_name, ""))
                if value:
                    return value
            return ""

        next_followup_candidates = [clean_text(r.get("next_followup_date", "")) for r in rows if clean_text(r.get("next_followup_date", ""))]
        activity_followup_candidates = [clean_text(r.get("follow_up_date", "")) for r in rows if clean_text(r.get("follow_up_date", ""))]
        if activity_followup_candidates:
            followup_date = activity_followup_candidates[-1]
        elif next_followup_candidates:
            followup_date = min(next_followup_candidates)
        elif latest_followup_dt and latest_followup_dt != datetime.min:
            followup_date = add_business_days(latest_followup_dt.date(), 1).isoformat()
        else:
            followup_date = ""

        group_key = clean_text(case_id)
        summary = {
            "group_key": group_key,
            "ticket_title": latest_nonempty("ticket_title"),
            "customer_name": latest_nonempty("customer_name"),
            "order_no": latest_nonempty("order_no"),
            "phone": latest_nonempty("phone"),
            "email": latest_nonempty("email"),
            "follow_up_date": followup_date,
            "last_activity": clean_text(latest_row.get("activity_date", "")),
            "latest_action": clean_text(latest_row.get("action_type", "")),
            "latest_outcome": clean_text(latest_row.get("result_type", "")),
            "latest_activity_id": _safe_optional_int(latest_row.get("id", 0)) or 0,
            "latest_ticket_id": _safe_optional_int(latest_row.get("ticket_id", "")),
            "manual_agents": ", ".join(sorted({clean_text(r.get("logging_agent", "")) for r in rows if clean_text(r.get("logging_agent", ""))})),
            "entries": len(rows),
            "status": "Resolved" if is_resolved else "Open Follow-Up",
            "first_activity": first_dt.strftime("%Y-%m-%d %H:%M:%S") if first_dt else "",
            "resolved_at": resolution_dt.strftime("%Y-%m-%d %H:%M:%S") if resolution_dt else "",
            "lifetime_days": ((resolution_dt or last_dt or first_dt or datetime.min) - (first_dt or datetime.min)).days if first_dt else 0,
        }

        timeline_df = pd.DataFrame(rows)
        if not timeline_df.empty:
            keep_cols = [
                "activity_date",
                "io_channel",
                "action_type",
                "result_type",
                "customer_name",
                "order_no",
                "phone",
                "email",
                "ticket_title",
                "follow_up_date",
                "logging_agent",
                "assigned_owner",
                "notes",
            ]
            keep_cols = [c for c in keep_cols if c in timeline_df.columns]
            timelines[group_key] = timeline_df[keep_cols].sort_values(by="activity_date", ascending=False)

        if is_resolved:
            resolved_groups.append(summary)
        else:
            open_groups.append(summary)

    open_df = pd.DataFrame(open_groups)
    resolved_df = pd.DataFrame(resolved_groups)
    if not open_df.empty:
        open_df["_follow_up_sort"] = open_df["follow_up_date"].replace("", "9999-12-31")
        open_df = open_df.sort_values(by=["_follow_up_sort", "last_activity"], ascending=[True, False]).drop(columns=["_follow_up_sort"])
    if not resolved_df.empty:
        resolved_df = resolved_df.sort_values(by="resolved_at", ascending=False)
    return open_df, resolved_df, timelines


# =========================================================
# OPENAI
# =========================================================

def get_openai_client():
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key or OpenAI is None:
        return None
    return OpenAI(api_key=api_key)


def get_assistant_health_status() -> tuple[bool, str, str]:
    has_openai_pkg = OpenAI is not None
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    has_api_key = bool(api_key)

    if has_openai_pkg and has_api_key:
        return True, "ready", "LACB Assistant is ready."

    missing = []
    if not has_openai_pkg:
        missing.append("`openai` package")
    if not has_api_key:
        missing.append("`OPENAI_API_KEY`")

    details = ", ".join(missing)
    msg = (
        f"LACB Assistant not ready. Missing: {details}. "
        "Install package with `pip install openai` and set env var with "
        "`$env:OPENAI_API_KEY=\"your_key_here\"` in PowerShell."
    )
    return False, "missing", msg

def get_masked_api_key_preview() -> str:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return "Not set"
    if len(api_key) <= 8:
        return f"{api_key[:2]}..."
    return f"{api_key[:7]}...{api_key[-4:]}"


def test_openai_connection() -> tuple[bool, str]:
    client = get_openai_client()
    if client is None:
        return False, "OPENAI_API_KEY is missing or openai package is not installed."
    try:
        resp = client.responses.create(model="gpt-5", input="ping")
        text = (resp.output_text or "").strip()
        if text:
            return True, "OpenAI connection successful."
        return True, "OpenAI connection successful (empty response text)."
    except Exception as e:
        msg = str(e)
        if "invalid_api_key" in msg or "Incorrect API key provided" in msg:
            return False, "Invalid API key. Set a valid OPENAI_API_KEY and restart Streamlit."
        return False, f"Connection failed: {msg}"


def generate_no_api_assistant_package(user_prompt: str, mode: str = "General", ticket_context: str = "") -> dict:
    text = clean_text(user_prompt)
    if not text:
        return {"output_text": "Enter prompt text to generate a draft."}

    merged = f"{ticket_context}\n\n{text}" if ticket_context else text
    pref = extract_prefill(merged, default_source="Phone")

    customer_name = pref.get("customer_name", "") or "Customer"
    order_no = pref.get("order_no", "") or ""
    phone = pref.get("phone", "") or ""
    request_type = pref.get("request_type", "Service")
    issue_type = pref.get("issue_type", "Other")

    package = {
        "raw_details": text,
        "customer_name": customer_name if customer_name != "Customer" else "",
        "order_no": order_no,
        "phone": phone,
        "request_type": request_type,
        "issue_type": issue_type,
        "ticket_source": request_type_to_source(request_type),
        "hubspot_stage": "New",
        "internal_status": default_internal_status(request_type),
        "attempt_no": 1,
        "ticket_name": "",
        "summary": "",
        "crm_note": "",
        "sms": "",
        "email": "",
        "output_text": "",
    }

    if mode == "Create CRM Note":
        package["crm_note"] = build_crm_note(text, request_type, issue_type)
        package["output_text"] = package["crm_note"]
        return package

    if mode == "Draft SMS":
        package["sms"] = build_customer_sms(customer_name, order_no, request_type, issue_type)
        package["output_text"] = package["sms"]
        return package

    if mode == "Draft Email":
        subject, body = build_customer_email(customer_name, order_no, issue_type, text)
        package["email"] = f"Subject: {subject}\n\n{body}"
        package["output_text"] = package["email"]
        return package

    if mode == "Scheduling Request":
        order_display = order_no or "N/A"
        phone_display = phone or "N/A"
        schedule_text = (
            "SCHEDULING REQUEST:\n"
            f"🔢 Active Order Number: {order_display}\n"
            f"👤 Customer Name: {customer_name}\n"
            f"📞☎️ Phone: {phone_display}\n"
            "🚚 Installer/Region (if applicable): [Add installer/region]\n"
            "📅 Requested Date & Time or Customer Availability: [Add availability]\n"
            "📝 Any Special Notes: " + text + "\n\n"
            "Internal Note:\n"
            f"I created a scheduling request for {customer_name} (Order {order_display}). "
            "Please contact the customer to confirm availability and book the visit. "
            "I will monitor and follow up as needed."
        )
        package["output_text"] = schedule_text
        return package

    if mode == "Summarize Notes":
        compact = " ".join(text.split())
        if len(compact) > 700:
            compact = compact[:700].rstrip() + "..."
        order_display = order_no or "N/A"
        phone_display = phone or "N/A"
        package["output_text"] = (
            f"Summary:\n{compact}\n\n"
            f"Detected:\n- Customer: {customer_name}\n- Order #: {order_display}\n- Phone: {phone_display}\n"
            f"- Request Type: {request_type}\n- Issue Type: {issue_type}"
        )
        return package

    package["ticket_name"] = build_ticket_name(request_type, issue_type, customer_name, order_no)
    package["summary"] = build_hubspot_summary(
        customer_name=customer_name,
        order_no=order_no,
        phone=phone,
        request_type=request_type,
        issue_type=issue_type,
        assigned_by="Receptionist",
        assigned_agent="Ed Torres",
        hubspot_stage="New",
        internal_status=default_internal_status(request_type),
        attempt_no=1,
        raw_details=text,
    )
    package["crm_note"] = build_crm_note(text, request_type, issue_type)
    package["sms"] = build_customer_sms(customer_name, order_no, request_type, issue_type)
    email_subject, email_body = build_customer_email(customer_name, order_no, issue_type, text)
    package["email"] = f"Subject: {email_subject}\n\n{email_body}"
    package["output_text"] = (
        "NO-API MODE (Local Rules)\n\n"
        f"HubSpot Ticket Name:\n{package['ticket_name']}\n\n"
        f"HubSpot Summary:\n{package['summary']}\n\n"
        f"CRM Note:\n{package['crm_note']}\n\n"
        f"Customer SMS:\n{package['sms']}\n\n"
        f"Customer Email:\n{package['email']}"
    )
    return package


def generate_no_api_assistant_output(user_prompt: str, mode: str = "General", ticket_context: str = "") -> str:
    return generate_no_api_assistant_package(user_prompt, mode, ticket_context).get("output_text", "")


def send_no_api_package_to_builder(pkg: dict) -> tuple[bool, str]:
    if not pkg or not isinstance(pkg, dict):
        return False, "No No-API output available yet. Run Assistant first in No-API mode."

    pending = {
        "customer_name": clean_text(pkg.get("customer_name", "")),
        "order_no": clean_text(pkg.get("order_no", "")),
        "phone": clean_text(pkg.get("phone", "")),
        "issue_type": pkg.get("issue_type", "Other"),
        "request_type": pkg.get("request_type", "Service"),
        "ticket_source": pkg.get("ticket_source", "Phone"),
    }

    st.session_state["builder_pending_autofill"] = pending
    st.session_state["builder_raw_details"] = clean_text(pkg.get("raw_details", ""))

    if pkg.get("ticket_name"):
        st.session_state["builder_generated_ticket_name"] = pkg.get("ticket_name", "")
    if pkg.get("summary"):
        st.session_state["builder_generated_summary"] = pkg.get("summary", "")
    if pkg.get("crm_note"):
        st.session_state["builder_generated_crm_note"] = pkg.get("crm_note", "")
    if pkg.get("sms"):
        st.session_state["builder_generated_sms"] = pkg.get("sms", "")
    if pkg.get("email"):
        st.session_state["builder_generated_email"] = pkg.get("email", "")

    st.session_state["selected_page"] = "HubSpot Ticket Builder V9"
    return True, "Sent to HubSpot Ticket Builder V9."


def ask_lacb_assistant(user_prompt: str, ticket_context: str = "", chat_history=None) -> str:
    client = get_openai_client()
    if client is None:
        return (
            "OpenAI API key not found or openai package is not installed.\n\n"
            "Install with: pip install openai\n"
            "Then set OPENAI_API_KEY in your environment."
        )

    history_text = ""
    if chat_history:
        for item in chat_history[-6:]:
            role = item.get("role", "user")
            content = item.get("content", "")
            history_text += f"{role.upper()}: {content}\n"

    full_input = f"""
Ticket Context:
{ticket_context or 'None'}

Conversation History:
{history_text or 'None'}

User Request:
{user_prompt}
"""
    try:
        response = client.responses.create(
            model="gpt-5",
            instructions=LACB_ASSISTANT_INSTRUCTIONS,
            input=full_input,
        )
        return response.output_text.strip()
    except Exception as e:
        msg = str(e)
        if "invalid_api_key" in msg or "Incorrect API key provided" in msg:
            return "OpenAI API key is invalid. Please set a valid OPENAI_API_KEY and rerun the app."
        return f"Assistant request failed: {msg}"


# =========================================================
# BUILDER GENERATION
# =========================================================

def do_generate_v9(save_to_tracker=False):
    customer_name = clean_text(st.session_state.get("builder_customer_name", ""))
    order_no = clean_text(st.session_state.get("builder_order_no", ""))
    phone = clean_text(st.session_state.get("builder_phone", ""))
    request_type = st.session_state.get("builder_request_type", "Service")
    issue_type = st.session_state.get("builder_issue_type", "Other")
    ticket_source = clean_text(st.session_state.get("builder_ticket_source", "Phone"))
    assigned_by = clean_text(st.session_state.get("builder_assigned_by", ""))
    assigned_agent = clean_text(st.session_state.get("builder_assigned_agent", "Ed Torres"))
    hubspot_stage = st.session_state.get("builder_hubspot_stage", "New")
    internal_status = st.session_state.get("builder_internal_status", "Open")
    attempt_no = st.session_state.get("builder_attempt_no", 1)
    raw_details = clean_text(st.session_state.get("builder_raw_details", ""))

    pref = extract_prefill(raw_details, default_source=ticket_source)

    final_customer_name = customer_name or pref.get("customer_name", "")
    final_order_no = order_no or pref.get("order_no", "")
    final_phone = phone or pref.get("phone", "")
    final_request_type = (
        pref["request_type"] if pref.get("request_type") in REQUEST_TYPES else request_type
    )
    final_issue_type = (
        pref["issue_type"] if pref.get("issue_type") in ISSUE_TYPES else issue_type
    )

    ticket_name = build_ticket_name(
        final_request_type,
        final_issue_type,
        final_customer_name,
        final_order_no,
    )

    summary = build_hubspot_summary(
        final_customer_name,
        final_order_no,
        final_phone,
        final_request_type,
        final_issue_type,
        assigned_by,
        assigned_agent,
        hubspot_stage,
        internal_status,
        attempt_no,
        raw_details,
    )

    crm_note = build_crm_note(
        raw_details,
        final_request_type,
        final_issue_type,
    )

    sms = build_customer_sms(
        final_customer_name,
        final_order_no,
        final_request_type,
        final_issue_type,
    )

    email_subject, email_body = build_customer_email(
        final_customer_name,
        final_order_no,
        final_issue_type,
        raw_details,
    )

    st.session_state["builder_generated_ticket_name"] = ticket_name
    st.session_state["builder_generated_summary"] = summary
    st.session_state["builder_generated_crm_note"] = crm_note
    st.session_state["builder_generated_sms"] = sms
    st.session_state["builder_generated_email"] = f"Subject: {email_subject}\n\n{email_body}"

    if save_to_tracker:
        ok, msg = save_generated_ticket_to_tracker(
            ticket_name=ticket_name,
            customer_name=final_customer_name,
            order_no=final_order_no,
            phone=final_phone,
            issue_type=final_issue_type,
            request_type=final_request_type,
            ticket_source=ticket_source,
            assigned_by=assigned_by,
            assigned_agent=assigned_agent,
            hubspot_stage=hubspot_stage,
            internal_status=internal_status,
            attempt_no=attempt_no,
            notes_summary=summary,
        )
        if ok:
            st.success(msg)
        else:
            st.warning(msg)
    else:
        st.success("Ticket draft generated.")


# =========================================================
# PAGES
# =========================================================

def follow_up_assistant_page():
    st.title("Follow-Up Assistant")

    grouped_tab, tickets_tab = st.tabs(["Grouped Activity Follow-Ups", "Ticket Queue"])

    with grouped_tab:
        open_groups_df, resolved_groups_df, timelines = _build_followup_activity_groups()

        m1, m2 = st.columns(2)
        m1.metric("Open Follow-Up Groups", 0 if open_groups_df.empty else len(open_groups_df))
        m2.metric("Resolved Groups", 0 if resolved_groups_df.empty else len(resolved_groups_df))

        st.subheader("Open Follow-Up Queue")
        if open_groups_df.empty:
            st.info("No grouped follow-up entries are currently open.")
        else:
            st.dataframe(
                open_groups_df[
                    [
                        "group_key",
                        "ticket_title",
                        "customer_name",
                        "order_no",
                        "phone",
                        "follow_up_date",
                        "latest_outcome",
                        "latest_action",
                        "manual_agents",
                        "entries",
                        "lifetime_days",
                    ]
                ],
                use_container_width=True,
            )

            group_options = {
                f"{row['group_key']} | {row['customer_name'] or 'N/A'} | {row['order_no'] or 'N/A'} | {row['phone'] or 'N/A'}": row["group_key"]
                for _, row in open_groups_df.iterrows()
            }
            selected_group = st.selectbox(
                "Select Follow-Up Group for Timeline",
                list(group_options.keys()),
                key="follow_up_group_select",
            )
            selected_group_key = group_options[selected_group]
            selected_group_row = open_groups_df[open_groups_df["group_key"] == selected_group_key].iloc[0]
            st.subheader("Follow-Up Timeline")
            st.dataframe(timelines[selected_group_key], use_container_width=True)

            current_outcomes = [
                token for token in _split_multi_value(selected_group_row["latest_outcome"]) if token in IO_OUTCOMES
            ]
            current_follow_up_value = clean_text(selected_group_row["follow_up_date"])
            default_followup_date = _parse_app_timestamp(current_follow_up_value)
            default_followup_date = (
                default_followup_date.date()
                if default_followup_date is not None
                else add_business_days(date.today(), 1)
            )
            if st.session_state.get("follow_up_group_edit_target_key") != selected_group_key:
                st.session_state["follow_up_group_edit_target_key"] = selected_group_key
                st.session_state["follow_up_group_edit_has_date"] = bool(current_follow_up_value)

            st.checkbox(
                "Set Follow-Up Date",
                key="follow_up_group_edit_has_date",
            )

            with st.form("follow_up_group_edit_form"):
                st.markdown("**Edit Grouped Follow-Up Status**")
                edited_outcomes = st.multiselect(
                    "Outcome",
                    IO_OUTCOMES,
                    default=current_outcomes,
                    key="follow_up_group_edit_outcomes",
                )
                edited_follow_up_date = None
                if st.session_state.get("follow_up_group_edit_has_date", False):
                    edited_follow_up_date = st.date_input(
                        "Follow-Up Date",
                        value=default_followup_date,
                        key="follow_up_group_edit_date",
                    )
                else:
                    st.caption("Follow-Up Date not set for this group.")
                update_group_btn = st.form_submit_button("Update Group")

            if update_group_btn:
                ok, msg = update_follow_up_group_entries(
                    group_key=selected_group_key,
                    result_types=edited_outcomes,
                    follow_up_date=edited_follow_up_date.isoformat() if edited_follow_up_date else "",
                )
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.warning(msg)

        if not resolved_groups_df.empty:
            with st.expander("Resolved Follow-Up Groups"):
                st.dataframe(
                    resolved_groups_df[
                        [
                            "group_key",
                            "ticket_title",
                            "customer_name",
                            "order_no",
                            "phone",
                            "resolved_at",
                            "latest_outcome",
                            "latest_action",
                            "manual_agents",
                            "entries",
                            "lifetime_days",
                        ]
                    ],
                    use_container_width=True,
                )

    with tickets_tab:
        df = get_all_tickets_df()
        if df.empty:
            st.info("No tickets found.")
            return

        df["attempt_no"] = pd.to_numeric(df["attempt_no"], errors="coerce").fillna(1).astype(int)
        df["recommended_action"] = df.apply(
            lambda row: recommended_next_action(row["attempt_no"], str(row["internal_status"])),
            axis=1,
        )

        st.subheader("Ticket Follow-Up Queue")
        view_df = df[
            [
                "id",
                "ticket_title",
                "customer_name",
                "order_no",
                "assigned_agent",
                "attempt_no",
                "hubspot_stage",
                "internal_status",
                "next_followup_date",
                "last_activity_date",
                "recommended_action",
            ]
        ]
        st.dataframe(view_df, use_container_width=True)


def _kpi_pick_period(prefix: str):
    time_view = st.selectbox(
        "KPI View",
        ["Daily", "Weekly", "Monthly", "Custom Range"],
        key=f"{prefix}_time_view",
    )
    today_dt = date.today()
    if time_view == "Daily":
        ref_date = st.date_input("Reference Date", value=today_dt, key=f"{prefix}_day")
        return time_view, ref_date, ref_date
    if time_view == "Weekly":
        ref_date = st.date_input("Reference Date", value=today_dt, key=f"{prefix}_week")
        start_date = ref_date - timedelta(days=ref_date.weekday())
        end_date = start_date + timedelta(days=6)
        return time_view, start_date, end_date
    if time_view == "Monthly":
        ref_date = st.date_input("Reference Date", value=today_dt, key=f"{prefix}_month")
        start_date = ref_date.replace(day=1)
        if ref_date.month == 12:
            next_month = ref_date.replace(year=ref_date.year + 1, month=1, day=1)
        else:
            next_month = ref_date.replace(month=ref_date.month + 1, day=1)
        end_date = next_month - timedelta(days=1)
        return time_view, start_date, end_date

    c1, c2 = st.columns(2)
    start_date = c1.date_input(
        "Start Date", value=today_dt - timedelta(days=7), key=f"{prefix}_custom_start"
    )
    end_date = c2.date_input("End Date", value=today_dt, key=f"{prefix}_custom_end")
    if end_date < start_date:
        st.warning("End date cannot be before start date.")
        return time_view, start_date, start_date
    return time_view, start_date, end_date


def _kpi_token_count(df: pd.DataFrame, column: str, token: str) -> int:
    if df.empty or column not in df.columns:
        return 0
    return int(df[column].fillna("").astype(str).str.contains(re.escape(token), case=False).sum())


def _kpi_load_data(owner_filter: str, start_date: date, end_date: date):
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()

    conn = get_conn()
    df_tickets = _db_read_sql_query("SELECT * FROM tickets ORDER BY id DESC", conn)
    df_activities = _db_read_sql_query("SELECT * FROM activities ORDER BY id DESC", conn)
    conn.close()

    if not df_tickets.empty:
        if "created_at" in df_tickets.columns:
            df_tickets = df_tickets[
                df_tickets["created_at"].fillna("").astype(str).str[:10].between(start_iso, end_iso)
            ]
        if owner_filter != "All Agents" and "assigned_agent" in df_tickets.columns:
            df_tickets = df_tickets[df_tickets["assigned_agent"] == owner_filter]

    if not df_activities.empty:
        if "activity_date" in df_activities.columns:
            act_dates = df_activities["activity_date"].fillna("").astype(str).str[:10]
            df_activities = df_activities[act_dates.between(start_iso, end_iso)]
        if owner_filter != "All Agents":
            if "logging_agent" in df_activities.columns:
                df_activities = df_activities[df_activities["logging_agent"] == owner_filter]
            elif "assigned_owner" in df_activities.columns:
                df_activities = df_activities[df_activities["assigned_owner"] == owner_filter]
    return df_tickets, df_activities


def kpi_dashboard_page():
    st.title("LACB Inbound-Outbound KPI Dashboard")

    c1, c2 = st.columns([1, 1])
    with c1:
        owner_filter = st.selectbox("Agent Filter", ["All Agents"] + KPI_AGENTS, key="kpi_owner_view")
    with c2:
        time_view, start_date, end_date = _kpi_pick_period("kpi")

    period_label = f"{time_view} | {start_date.isoformat()} to {end_date.isoformat()}"

    df_tickets, df_activities = _kpi_load_data(owner_filter, start_date, end_date)

    total_activities = len(df_activities) if not df_activities.empty else 0
    total_ticket_actions = (
        _kpi_token_count(df_activities, "action_type", "new ticket")
        + _kpi_token_count(df_activities, "action_type", "ticket updated")
        + _kpi_token_count(df_activities, "action_type", "ticket closed")
    )

    ref_cols = st.columns([1.4, 1, 1, 1])
    ref_cols[0].markdown(
        f"**Selected Period**\n\n{period_label}\n\n**Agent Filter**\n\n{owner_filter}"
    )
    ref_cols[1].metric("Total Activities", total_activities)
    ref_cols[2].metric("Total Ticket Actions", total_ticket_actions)
    ref_cols[3].metric("Tickets in Scope", len(df_tickets) if not df_tickets.empty else 0)

    summary_rows = [
        ("Total Activities", total_activities),
        ("Inbound RingCX", _kpi_token_count(df_activities, "io_channel", "Inbound RingCX")),
        ("Outbound RingCX", _kpi_token_count(df_activities, "io_channel", "Outbound RingCX")),
        ("Inbound RC", _kpi_token_count(df_activities, "io_channel", "Inbound RC (MVP)")),
        ("Outbound RC", _kpi_token_count(df_activities, "io_channel", "Outbound RC (MVP)")),
        ("SMS", _kpi_token_count(df_activities, "io_channel", "SMS")),
        ("Email", _kpi_token_count(df_activities, "io_channel", "Email")),
        ("Service Orders Created", _kpi_token_count(df_activities, "action_type", "Service Order Created")),
        (
            "Tickets Assigned",
            _kpi_token_count(df_activities, "action_type", "New Ticket Assigned - Receptionist")
            + _kpi_token_count(df_activities, "action_type", "New Ticket Assigned - Supervisor")
            + _kpi_token_count(df_activities, "action_type", "New Ticket Assigned - Sales Team")
            + _kpi_token_count(df_activities, "action_type", "New Ticket Assigned - By Other"),
        ),
        ("Tickets Created", _kpi_token_count(df_activities, "action_type", "New Ticket Created")),
        ("Tickets Updated", _kpi_token_count(df_activities, "action_type", "Ticket Updated/Notes")),
        ("Tickets Closed", _kpi_token_count(df_activities, "action_type", "Ticket Closed")),
        ("Tasks Updated", _kpi_token_count(df_activities, "action_type", "Task Updated")),
        ("Tasks Completed", _kpi_token_count(df_activities, "action_type", "Task Marked Complete")),
    ]

    channel_rows = [(ch, _kpi_token_count(df_activities, "io_channel", ch)) for ch in IO_CHANNELS]
    action_rows = [(act, _kpi_token_count(df_activities, "action_type", act)) for act in IO_TASK_ACTIONS]
    outcome_rows = [(out, _kpi_token_count(df_activities, "result_type", out)) for out in IO_OUTCOMES]

    t1, t2, t3, t4 = st.columns(4)
    with t1:
        st.markdown("**KPI Summary**")
        st.dataframe(
            pd.DataFrame(summary_rows, columns=["Metric", "Count"]),
            use_container_width=True,
            hide_index=True,
        )
    with t2:
        st.markdown("**Channel Breakdown**")
        st.dataframe(
            pd.DataFrame(channel_rows, columns=["Channel", "Count"]),
            use_container_width=True,
            hide_index=True,
        )
    with t3:
        st.markdown("**All Actions Tally**")
        st.dataframe(
            pd.DataFrame(action_rows, columns=["Action", "Count"]),
            use_container_width=True,
            hide_index=True,
        )
    with t4:
        st.markdown("**Outcome Tally**")
        st.dataframe(
            pd.DataFrame(outcome_rows, columns=["Outcome", "Count"]),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Filtered Activity Log")
    if df_activities.empty:
        st.info("No activity logs for the selected period/filter.")
    else:
        view_cols = [
            c
            for c in [
                "activity_date",
                "io_channel",
                "action_type",
                "phone",
                "customer_name",
                "order_no",
                "ticket_update_name",
                "email",
                "result_type",
                "logging_agent",
                "notes",
            ]
            if c in df_activities.columns
        ]
        st.dataframe(df_activities[view_cols], use_container_width=True)


def kpi_graph_dashboard_page():
    st.title("LACB KPI Graph Dashboard")
    c1, c2 = st.columns([1, 1])
    with c1:
        owner_filter = st.selectbox(
            "Agent Filter",
            ["All Agents"] + KPI_AGENTS,
            key="kpi_graph_owner_view",
        )
    with c2:
        time_view, start_date, end_date = _kpi_pick_period("kpi_graph")

    st.caption(
        f"Charts follow selected filters: {time_view} | "
        f"{start_date.isoformat()} to {end_date.isoformat()} | {owner_filter}"
    )

    _, df_activities = _kpi_load_data(owner_filter, start_date, end_date)

    channel_df = pd.DataFrame(
        [{"Channel": ch, "Count": _kpi_token_count(df_activities, "io_channel", ch)} for ch in IO_CHANNELS]
    )
    action_df = pd.DataFrame(
        [{"Action": act, "Count": _kpi_token_count(df_activities, "action_type", act)} for act in IO_TASK_ACTIONS]
    )
    outcome_df = pd.DataFrame(
        [{"Outcome": out, "Count": _kpi_token_count(df_activities, "result_type", out)} for out in IO_OUTCOMES]
    )

    g1, g2 = st.columns(2)
    with g1:
        st.markdown("**Channel Breakdown**")
        st.bar_chart(channel_df.set_index("Channel"))
    with g2:
        st.markdown("**Action Breakdown**")
        st.bar_chart(action_df.set_index("Action"))

    g3, g4 = st.columns(2)
    with g3:
        st.markdown("**Outcome Distribution**")
        st.bar_chart(outcome_df.set_index("Outcome"))
    with g4:
        st.markdown("**KPI Summary Snapshot**")
        kpi_df = pd.DataFrame(
            [
                {"Metric": "Inbound RingCX", "Count": _kpi_token_count(df_activities, "io_channel", "Inbound RingCX")},
                {"Metric": "Outbound RingCX", "Count": _kpi_token_count(df_activities, "io_channel", "Outbound RingCX")},
                {"Metric": "Inbound RC", "Count": _kpi_token_count(df_activities, "io_channel", "Inbound RC (MVP)")},
                {"Metric": "Outbound RC", "Count": _kpi_token_count(df_activities, "io_channel", "Outbound RC (MVP)")},
                {"Metric": "SMS", "Count": _kpi_token_count(df_activities, "io_channel", "SMS")},
                {"Metric": "Email", "Count": _kpi_token_count(df_activities, "io_channel", "Email")},
                {"Metric": "Service Orders Created", "Count": _kpi_token_count(df_activities, "action_type", "Service Order Created")},
                {"Metric": "Tickets Updated", "Count": _kpi_token_count(df_activities, "action_type", "Ticket Updated/Notes")},
            ]
        )
        st.bar_chart(kpi_df.set_index("Metric"))


def inbound_outbound_activity_log_page(embedded: bool = False):
    if embedded:
        st.subheader("Inbound / Outbound Activity Log")
    else:
        st.title("Inbound / Outbound Activity Log")
    st.caption("Log channel activity entries that should feed the KPI dashboard.")
    st.caption(f"Auto Timestamp (Pacific): {now_pacific_ts()}")

    if st.session_state.pop("io_reset_requested", False):
        for key, value in {
            "io_channels": [IO_CHANNELS[0]],
            "io_actions": [],
            "io_result_types": [],
            "io_customer_name": "",
            "io_order_no": "",
            "io_phone": "",
            "io_email": "",
            "io_ticket_update_name": "",
            "io_has_follow_up_date": False,
            "io_follow_up_date_input": add_business_days(date.today(), 1),
            "io_notes": "",
        }.items():
            st.session_state[key] = value

    if not isinstance(st.session_state.get("io_channels"), list):
        st.session_state["io_channels"] = [clean_text(st.session_state.get("io_channels", ""))] if clean_text(st.session_state.get("io_channels", "")) else []
    if not isinstance(st.session_state.get("io_result_types"), list):
        st.session_state["io_result_types"] = _split_multi_value(st.session_state.get("io_result_types", ""))
    st.session_state.setdefault("io_has_follow_up_date", False)
    st.checkbox("Set Follow-Up Date", key="io_has_follow_up_date")

    with st.form("io_activity_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            logging_agent = st.selectbox(
                "Logging Agent",
                KPI_AGENTS,
                index=KPI_AGENTS.index(st.session_state.get("io_logging_agent", KPI_AGENTS[0])),
                key="io_logging_agent",
            )
            io_channels = st.multiselect("Channel", IO_CHANNELS, key="io_channels")
            action_types = st.multiselect("Action Type", IO_TASK_ACTIONS, key="io_actions")
        with c2:
            result_types = st.multiselect("Outcome", IO_OUTCOMES, key="io_result_types")
            customer_name = st.text_input("Customer Name", key="io_customer_name")
            order_no = st.text_input("Order #", key="io_order_no")
            phone = st.text_input("Phone", key="io_phone")
        with c3:
            email = st.text_input("Email", key="io_email")
            ticket_update_name = st.text_input("Ticket / Task Update Name", key="io_ticket_update_name")
            follow_up_date = None
            if st.session_state.get("io_has_follow_up_date", False):
                follow_up_date = st.date_input(
                    "Follow-Up Date",
                    value=st.session_state.get("io_follow_up_date_input", add_business_days(date.today(), 1)),
                    key="io_follow_up_date_input",
                )
            else:
                st.caption("Follow-Up Date not set for this entry.")
            notes = st.text_area("Notes", key="io_notes", height=150)

        b1, b2 = st.columns(2)
        save_btn = b1.form_submit_button("Save Activity")
        save_new_btn = b2.form_submit_button("Save + Start New")

    if save_btn or save_new_btn:
        ok, msg = save_inbound_outbound_activity_entry(
            logging_agent=logging_agent,
            io_channels=io_channels,
            action_types=action_types,
            result_types=result_types,
            customer_name=customer_name,
            order_no=order_no,
            phone=phone,
            email=email,
            ticket_update_name=ticket_update_name,
            follow_up_date=follow_up_date.isoformat() if follow_up_date else "",
            notes=notes,
        )
        if ok:
            if save_new_btn:
                st.session_state["io_reset_requested"] = True
            st.success(msg)
            st.rerun()
        else:
            st.warning(msg)

    st.subheader("Recent Inbound / Outbound Entries")
    conn = get_conn()
    recent_df = _db_read_sql_query(
        """
        SELECT
            id,
            activity_date AS timestamp_pacific,
            logging_agent,
            io_channel,
            follow_up_date,
            action_type,
            result_type,
            customer_name,
            order_no,
            phone,
            email,
            ticket_update_name,
            notes
        FROM activities
        WHERE COALESCE(io_channel, '') <> ''
        ORDER BY id DESC
        LIMIT 100
        """,
        conn,
    )
    conn.close()

    if recent_df.empty:
        st.info("No inbound / outbound entries logged yet.")
    else:
        st.dataframe(recent_df, use_container_width=True)


def ticket_tracker_page():
    st.title("Ticket Tracker")
    inbound_outbound_activity_log_page(embedded=True)
    st.divider()

    if st.session_state.pop("tracker_new_reset_requested", False):
        for k, v in {
            "tracker_new_ticket_title": "",
            "tracker_new_customer_name": "",
            "tracker_new_order_no": "",
            "tracker_new_phone": "",
            "tracker_new_issue_type": "Other",
            "tracker_new_request_type": "Service",
            "tracker_new_ticket_source": "Phone",
            "tracker_new_assigned_by": "Receptionist",
            "tracker_new_assigned_agent": "Ed Torres",
            "tracker_new_hubspot_stage": "New",
            "tracker_new_internal_status": "Open",
            "tracker_new_attempt_no": 1,
            "tracker_new_notes_summary": "",
        }.items():
            st.session_state[k] = v

    # Bulk import preserved
    render_ticket_bulk_import_section()
    st.divider()

    # Manual ticket entry (restored classic flow)
    st.subheader("Create / Update Ticket")
    st.session_state.setdefault("tracker_new_ticket_title", "")
    st.session_state.setdefault("tracker_new_customer_name", "")
    st.session_state.setdefault("tracker_new_order_no", "")
    st.session_state.setdefault("tracker_new_phone", "")
    st.session_state.setdefault("tracker_new_issue_type", "Other")
    st.session_state.setdefault("tracker_new_request_type", "Service")
    st.session_state.setdefault("tracker_new_ticket_source", "Phone")
    st.session_state.setdefault("tracker_new_assigned_by", "Receptionist")
    st.session_state.setdefault("tracker_new_assigned_agent", "Ed Torres")
    st.session_state.setdefault("tracker_new_hubspot_stage", "New")
    st.session_state.setdefault("tracker_new_internal_status", "Open")
    st.session_state.setdefault("tracker_new_attempt_no", 1)
    st.session_state.setdefault("tracker_new_notes_summary", "")

    with st.form("tracker_create_form_v9"):
        c1, c2, c3 = st.columns(3)
        with c1:
            ticket_title = st.text_input("HubSpot Ticket Title", key="tracker_new_ticket_title")
            customer_name = st.text_input("Customer Name", key="tracker_new_customer_name")
            order_no = st.text_input("Order #", key="tracker_new_order_no")
            phone = st.text_input("Phone", key="tracker_new_phone")

        with c2:
            issue_type = st.selectbox("Issue Type", ISSUE_TYPES, key="tracker_new_issue_type")
            request_type = st.selectbox("Request Type", REQUEST_TYPES, key="tracker_new_request_type")
            ticket_source = st.selectbox("Ticket Source", TICKET_SOURCES, key="tracker_new_ticket_source")
            assigned_by = st.selectbox("Assigned By", ASSIGNED_BY_OPTIONS, key="tracker_new_assigned_by")

        with c3:
            assigned_agent = st.selectbox("Assigned Agent", OWNERS, key="tracker_new_assigned_agent")
            hubspot_stage = st.selectbox("HubSpot Stage", HUBSPOT_STAGES, key="tracker_new_hubspot_stage")
            internal_status = st.selectbox("Internal Status", INTERNAL_STATUSES, key="tracker_new_internal_status")
            attempt_no = st.number_input("Attempt #", min_value=1, step=1, key="tracker_new_attempt_no")

        notes_summary = st.text_area("Notes Summary", key="tracker_new_notes_summary", height=120)

        b1, b2, b3 = st.columns(3)
        save_ticket_btn = b1.form_submit_button("Save Ticket")
        save_start_new_btn = b2.form_submit_button("Save + Start New")
        clear_form_btn = b3.form_submit_button("Clear Form")

    if clear_form_btn:
        st.session_state["tracker_new_reset_requested"] = True
        st.rerun()

    if save_ticket_btn or save_start_new_btn:
        if not ticket_title.strip():
            ticket_title = build_ticket_name(request_type, issue_type, customer_name.strip(), order_no.strip())

        if ticket_exists(order_no.strip(), ticket_title.strip()):
            st.warning("Possible duplicate ticket detected. Save was blocked.")
        else:
            ok, msg = save_generated_ticket_to_tracker(
                ticket_name=ticket_title.strip(),
                customer_name=customer_name.strip(),
                order_no=order_no.strip(),
                phone=phone.strip(),
                issue_type=issue_type,
                request_type=request_type,
                ticket_source=ticket_source,
                assigned_by=assigned_by,
                assigned_agent=assigned_agent,
                hubspot_stage=hubspot_stage,
                internal_status=internal_status,
                attempt_no=int(attempt_no),
                notes_summary=notes_summary.strip(),
            )
            if ok:
                st.success("Ticket saved.")
                if save_start_new_btn:
                    st.session_state["tracker_new_reset_requested"] = True
                st.rerun()
            else:
                st.warning(msg)

    st.divider()
    st.subheader("Ticket List")
    df = get_all_tickets_df()

    if df.empty:
        st.info("No tickets found yet.")
        return

    st.dataframe(df, use_container_width=True)

    ticket_options = {
        f"{row.get('cc_id', '') or '-'} | {int(row['id'])} | {row.get('ticket_title', '')}": int(row["id"])
        for _, row in df.iterrows()
    }
    selected_label = st.selectbox("Select Ticket", list(ticket_options.keys()), index=0)
    selected_id = ticket_options[selected_label]
    st.session_state["selected_ticket_id"] = selected_id

    ticket = get_ticket_by_id(selected_id)
    if not ticket:
        st.warning("Ticket not found.")
        return

    st.subheader("Edit Selected Ticket")
    c1, c2, c3 = st.columns(3)

    with c1:
        edit_customer = st.text_input("Customer Name", value=ticket["customer_name"] or "", key="edit_customer_name")
        edit_order = st.text_input("Order #", value=ticket["order_no"] or "", key="edit_order_no")
        edit_phone = st.text_input("Phone", value=ticket["phone"] or "", key="edit_phone")

    with c2:
        edit_issue = st.selectbox(
            "Issue Type",
            ISSUE_TYPES,
            index=ISSUE_TYPES.index(ticket["issue_type"]) if ticket["issue_type"] in ISSUE_TYPES else len(ISSUE_TYPES) - 1,
            key="edit_issue_type",
        )
        edit_request = st.selectbox(
            "Request Type",
            REQUEST_TYPES,
            index=REQUEST_TYPES.index(ticket["request_type"]) if ticket["request_type"] in REQUEST_TYPES else 0,
            key="edit_request_type",
        )
        edit_owner = st.selectbox(
            "Assigned Agent",
            OWNERS,
            index=OWNERS.index(ticket["assigned_agent"]) if ticket["assigned_agent"] in OWNERS else 0,
            key="edit_assigned_agent",
        )

    with c3:
        edit_stage = st.selectbox(
            "HubSpot Stage",
            HUBSPOT_STAGES,
            index=HUBSPOT_STAGES.index(ticket["hubspot_stage"]) if ticket["hubspot_stage"] in HUBSPOT_STAGES else 0,
            key="edit_hubspot_stage",
        )
        edit_status = st.selectbox(
            "Internal Status",
            INTERNAL_STATUSES,
            index=INTERNAL_STATUSES.index(ticket["internal_status"]) if ticket["internal_status"] in INTERNAL_STATUSES else 0,
            key="edit_internal_status",
        )
        edit_attempt = st.number_input("Attempt #", min_value=1, step=1, value=int(ticket["attempt_no"] or 1), key="edit_attempt_no")

    edit_notes = st.text_area("Notes Summary", value=ticket["notes_summary"] or "", height=180, key="edit_notes_summary")

    csave, cdelete = st.columns(2)
    if csave.button("Save Ticket Changes"):
        update_ticket_basic(
            selected_id,
            {
                "customer_name": edit_customer,
                "order_no": edit_order,
                "phone": edit_phone,
                "issue_type": edit_issue,
                "request_type": edit_request,
                "assigned_agent": edit_owner,
                "hubspot_stage": edit_stage,
                "internal_status": edit_status,
                "attempt_no": edit_attempt,
                "notes_summary": edit_notes,
            },
        )
        st.success("Ticket updated.")
        st.rerun()

    if cdelete.button("Delete Ticket"):
        delete_ticket(selected_id)
        st.success("Ticket deleted.")
        st.rerun()

def quick_activity_log_page():
    st.title("Quick Activity Log")

    if st.session_state.pop("quick_log_reset_requested", False):
        st.session_state["quick_log_actions"] = []
        st.session_state["quick_log_notes"] = ""

    df = get_all_tickets_df()
    if df.empty:
        st.info("No tickets found.")
        return

    customer_search = st.text_input(
        "Search Customer Name",
        key="quick_log_customer_search",
        placeholder="Type customer name to filter tickets...",
    ).strip()

    filtered_df = df
    if customer_search:
        filtered_df = df[
            df["customer_name"].fillna("").astype(str).str.contains(customer_search, case=False)
        ]

    if filtered_df.empty:
        st.warning("No tickets found for that customer search. Clear search to see all tickets.")
        return

    ticket_options = {
        f"{row.get('cc_id', '') or '-'} | {int(row['id'])} | {row['customer_name'] or 'N/A'} | {row['order_no'] or 'N/A'} | {row['ticket_title']}": int(row["id"])
        for _, row in filtered_df.iterrows()
    }

    option_values = list(ticket_options.values())
    default_index = 0
    current_ticket_id = st.session_state.get("quick_log_ticket_id")
    if current_ticket_id in option_values:
        default_index = option_values.index(current_ticket_id)

    selected_label = st.selectbox("Select Ticket", list(ticket_options.keys()), index=default_index)
    ticket_id = ticket_options[selected_label]
    st.session_state["quick_log_ticket_id"] = ticket_id
    ticket = get_ticket_by_id(ticket_id)
    if ticket:
        st.write(f"**Customer:** {ticket.get('customer_name', '') or 'N/A'}")
        st.write(f"**Order #:** {ticket.get('order_no', '') or 'N/A'}")
        st.write(f'**Current Status:** {ticket["internal_status"]}')
        st.write(f'**Attempt #:** {ticket["attempt_no"]}')
        st.write(f'**Recommended Next Action:** {recommended_next_action(int(ticket["attempt_no"] or 1), str(ticket["internal_status"]))}')

    st.session_state.setdefault("quick_log_actions", ["Outbound Call"])

    c1, c2, c3, c4, c5 = st.columns(5)
    if c1.button("Outbound Call"):
        actions = st.session_state.get("quick_log_actions", [])
        if "Outbound Call" not in actions:
            actions = actions + ["Outbound Call"]
        st.session_state["quick_log_actions"] = actions
    if c2.button("Inbound Call"):
        actions = st.session_state.get("quick_log_actions", [])
        if "Inbound Call" not in actions:
            actions = actions + ["Inbound Call"]
        st.session_state["quick_log_actions"] = actions
    if c3.button("Add SMS"):
        actions = st.session_state.get("quick_log_actions", [])
        if "SMS Sent" not in actions:
            actions = actions + ["SMS Sent"]
        st.session_state["quick_log_actions"] = actions
    if c4.button("Add Email"):
        actions = st.session_state.get("quick_log_actions", [])
        if "Email Sent" not in actions:
            actions = actions + ["Email Sent"]
        st.session_state["quick_log_actions"] = actions
    if c5.button("Add Chat"):
        actions = st.session_state.get("quick_log_actions", [])
        if "Internal Chat" not in actions:
            actions = actions + ["Internal Chat"]
        st.session_state["quick_log_actions"] = actions

    d1, d2, d3, d4 = st.columns(4)
    if d1.button("Spoke"):
        st.session_state["quick_log_result_type"] = "Spoke with customer"
    if d2.button("No Answer"):
        st.session_state["quick_log_result_type"] = "No answer"
    if d3.button("Voicemail"):
        st.session_state["quick_log_result_type"] = "Left voicemail"
    if d4.button("Waiting"):
        st.session_state["quick_log_result_type"] = "Waiting on customer"

    if not isinstance(st.session_state.get("quick_log_actions"), list):
        st.session_state["quick_log_actions"] = ["Outbound Call"]
    st.session_state["quick_log_actions"] = [
        a for a in st.session_state.get("quick_log_actions", []) if a in QUICK_LOG_ACTIONS
    ]
    if st.session_state.get("quick_log_result_type") not in QUICK_LOG_RESULTS:
        st.session_state["quick_log_result_type"] = "No answer"

    action_types = st.multiselect(
        "Action Type",
        QUICK_LOG_ACTIONS,
        default=st.session_state.get("quick_log_actions", []),
        key="quick_log_actions",
    )
    result_type = st.selectbox(
        "Result",
        QUICK_LOG_RESULTS,
        index=QUICK_LOG_RESULTS.index(st.session_state["quick_log_result_type"]),
    )
    logging_agent = st.selectbox("Logging Agent", OWNERS, index=OWNERS.index(st.session_state["quick_log_logging_agent"]))
    assigned_owner = st.selectbox("Assigned Owner", OWNERS, index=OWNERS.index(st.session_state["quick_log_assigned_owner"]))

    suggested_note = ""
    action_set = set(action_types)
    if "Outbound Call" in action_set and result_type == "No answer":
        suggested_note = "Attempted outbound call to customer but there was no answer."
    elif "Outbound Call" in action_set and result_type == "Left voicemail":
        suggested_note = "Attempted outbound call to customer but there was no answer. Left voicemail requesting a callback."
    elif "Inbound Call" in action_set:
        suggested_note = "Handled inbound customer call and documented current status."
    elif "SMS Sent" in action_set:
        suggested_note = "Sent SMS update to the customer and documented the current status."
    elif "Email Sent" in action_set:
        suggested_note = "Sent email update to the customer and documented the current status."
    elif "Internal Chat" in action_set:
        suggested_note = "Sent internal chat update and documented the next required step."

    notes = st.text_area("Notes", value=suggested_note, height=120, key="quick_log_notes_area")

    csave, cnew = st.columns(2)
    if csave.button("Save Activity"):
        if not action_types:
            st.warning("Select at least one Action Type.")
        else:
            saved = add_activity(ticket_id, logging_agent, assigned_owner, action_types, result_type, notes)
            st.success("Activity saved.") if saved is not False else st.warning("Duplicate activity ignored.")
            st.rerun()

    if cnew.button("Save + Start New"):
        if not action_types:
            st.warning("Select at least one Action Type.")
        else:
            saved = add_activity(ticket_id, logging_agent, assigned_owner, action_types, result_type, notes)
            if saved is not False:
                st.session_state["quick_log_reset_requested"] = True
            st.success("Activity saved.") if saved is not False else st.warning("Duplicate activity ignored.")
            st.rerun()
    st.subheader("Ticket Timeline")
    timeline_df = get_activities_for_ticket(ticket_id)

    tools1, tools2 = st.columns(2)
    with tools1:
        if st.button("Remove Duplicate Entries", key=f"dedupe_ticket_{ticket_id}"):
            removed = cleanup_duplicate_activities_for_ticket(ticket_id)
            if removed > 0:
                st.success(f"Removed {removed} duplicate entr{'y' if removed == 1 else 'ies'}.")
            else:
                st.info("No duplicates found for this ticket.")
            st.rerun()

    with tools2:
        if not timeline_df.empty:
            delete_options = {
                f"#{int(r['id'])} | {r['activity_date']} | {r['action_type']} | {r['result_type']}": int(r["id"])
                for _, r in timeline_df.iterrows()
            }
            selected_delete = st.selectbox(
                "Delete Activity Entry",
                list(delete_options.keys()),
                key=f"quick_log_delete_entry_{ticket_id}",
            )
            if st.button("Delete Selected Entry", key=f"delete_activity_{ticket_id}"):
                deleted = delete_activity_by_id(delete_options[selected_delete])
                if deleted:
                    st.success("Activity entry deleted.")
                else:
                    st.warning("Could not delete entry.")
                st.rerun()

    st.dataframe(timeline_df, use_container_width=True)


def hubspot_ticket_builder_page_v9():
    st.title("HubSpot Ticket Builder V9")

    apply_builder_pending_autofill()

    st.caption("Paste notes, call transcript, SMS thread, email thread, or internal details.")

    st.text_area(
        "Raw Notes / Transcript / Details",
        key="builder_raw_details",
        height=220,
    )

    c1, c2, c3 = st.columns(3)

    with c1:
        st.text_input("Customer Name", key="builder_customer_name")
        st.text_input("Order #", key="builder_order_no")
        st.text_input("Phone", key="builder_phone")

    with c2:
        st.selectbox("Request Type", REQUEST_TYPES, key="builder_request_type")
        st.selectbox("Issue Type", ISSUE_TYPES, key="builder_issue_type")
        st.selectbox("Ticket Source", TICKET_SOURCES, key="builder_ticket_source")

    with c3:
        st.selectbox("Assigned By", ASSIGNED_BY_OPTIONS, key="builder_assigned_by")
        st.selectbox("Assigned Agent", OWNERS, key="builder_assigned_agent")
        st.selectbox("HubSpot Stage", HUBSPOT_STAGES, key="builder_hubspot_stage")
        st.selectbox("Internal Status", INTERNAL_STATUSES, key="builder_internal_status")
        st.number_input("Attempt #", min_value=1, step=1, key="builder_attempt_no")

    a1, a2, a3, a4 = st.columns(4)

    if a1.button("Auto Detect from Text"):
        pref = extract_prefill(
            st.session_state.get("builder_raw_details", ""),
            default_source=st.session_state.get("builder_ticket_source", "Phone"),
        )
        st.session_state["builder_pending_autofill"] = pref
        st.rerun()

    if a2.button("Detect Multi-Ticket Block"):
        raw = st.session_state.get("builder_raw_details", "")
        blocks = split_possible_multi_ticket_block(raw)
        drafts = []

        for block in blocks:
            pref = extract_prefill(block)
            final_customer = pref.get("customer_name", "")
            final_order = pref.get("order_no", "")
            final_phone = pref.get("phone", "")
            final_issue = pref.get("issue_type", "Other")
            final_request = pref.get("request_type", "Service")

            ticket_name = build_ticket_name(
                final_request, final_issue, final_customer, final_order
            )
            summary = build_hubspot_summary(
                final_customer,
                final_order,
                final_phone,
                final_request,
                final_issue,
                st.session_state.get("builder_assigned_by", ""),
                st.session_state.get("builder_assigned_agent", "Ed Torres"),
                st.session_state.get("builder_hubspot_stage", "New"),
                st.session_state.get("builder_internal_status", "Open"),
                st.session_state.get("builder_attempt_no", 1),
                block,
            )

            drafts.append(
                {
                    "ticket_name": ticket_name,
                    "summary": summary,
                    "raw_block": block,
                    "customer_name": final_customer,
                    "order_no": final_order,
                    "phone": final_phone,
                    "issue_type": final_issue,
                    "request_type": final_request,
                }
            )

        st.session_state["builder_multi_ticket_drafts"] = drafts

    if a3.button("Generate Ticket Draft"):
        do_generate_v9(save_to_tracker=False)

    if a4.button("Generate + Add to Tracker"):
        do_generate_v9(save_to_tracker=True)

    st.divider()

    if st.session_state.get("builder_generated_ticket_name"):
        st.subheader("Generated Outputs")

        st.text_input(
            "HubSpot Ticket Name",
            value=st.session_state.get("builder_generated_ticket_name", ""),
            key="display_ticket_name_v9",
        )

        st.text_area(
            "HubSpot Summary",
            value=st.session_state.get("builder_generated_summary", ""),
            key="display_summary_v9",
            height=220,
        )

        st.text_area(
            "CRM Note",
            value=st.session_state.get("builder_generated_crm_note", ""),
            key="display_crm_note_v9",
            height=180,
        )

        st.text_area(
            "Customer SMS",
            value=st.session_state.get("builder_generated_sms", ""),
            key="display_sms_v9",
            height=140,
        )

        st.text_area(
            "Customer Email",
            value=st.session_state.get("builder_generated_email", ""),
            key="display_email_v9",
            height=240,
        )

    drafts = st.session_state.get("builder_multi_ticket_drafts", [])
    if drafts:
        st.subheader("Detected Multi-Ticket Drafts")
        for i, draft in enumerate(drafts, start=1):
            with st.expander(f"Draft {i}: {draft['ticket_name']}"):
                st.write("**Raw Block**")
                st.code(draft["raw_block"])
                st.write("**Summary**")
                st.text_area(
                    f"Summary {i}",
                    value=draft["summary"],
                    key=f"multi_summary_{i}",
                    height=180,
                )


def service_repair_builder_page():
    st.title("Service / Repair Builder")

    scenario = st.selectbox(
        "Service Scenario",
        [
            "Motor Issue",
            "Fabric Issue",
            "Guidewire Issue",
            "Anchor Issue",
            "Programming / Demonstrate Programming Steps",
            "Charger Reconnection",
            "Remote / Programming Issue",
            "Other",
        ],
    )

    original_order = st.text_input("Original Order #")
    why = st.text_area("Why / Notes")
    photos = st.selectbox("Photos/Videos", ["No", "Yes"])

    if st.button("Generate Service / Repair Template"):
        repair_needed = scenario
        template = (
            f"Original Order#: {original_order}\n"
            f"Repair Needed: Assess the {repair_needed.lower()} for repair/replacement. Fix on the spot if possible or advise on next steps needed.\n"
            f"Why: {why}\n"
            f"Photos/videos: {photos}"
        )
        st.text_area("Generated Template", value=template, height=220)


def scheduling_assistant_page():
    st.title("Scheduling Assistant")

    ctop1, ctop2 = st.columns(2)
    if ctop1.button("Refresh Region Map"):
        load_region_map.clear()
        st.success("Region map refreshed.")
    if ctop2.button("Refresh Scheduling Workbook"):
        load_scheduler_dashboard.clear()
        st.success("Scheduling workbook refreshed.")

    tab1, tab2 = st.tabs(["Scheduling Request Generator", "ZIP -> Installer Priority"])

    with tab1:
        c1, c2 = st.columns(2)
        order_no = c1.text_input("Active Order Number")
        customer = c2.text_input("Customer Name")

        c3, c4 = st.columns(2)
        phone = c3.text_input("Phone")
        installer_region = c4.text_input("Installer/Region", placeholder="SD Region")

        availability = st.text_input("Requested Date & Time / Availability", value="NA/Next Available")
        special_notes = st.text_area("Any special notes")
        flags = st.multiselect("Routing Flags", SCHEDULING_FLAGS)

        if st.button("Generate Scheduling Request"):
            output = (
                "SCHEDULING REQUEST:\n"
                f"🔢 Active Order Number: {order_no or 'N/A'}\n"
                f"👤 Customer Name: {customer or 'N/A'}\n"
                f"📞☎️ Phone: {phone or 'N/A'}\n"
                f"🚚 Installer/Region (if applicable): {installer_region or 'N/A'}\n"
                f"📅 Requested Date & Time or Customer Availability: {availability or 'N/A'}\n"
                f"📝 Any Special Notes: {special_notes or 'N/A'}"
            )
            route, reason = compute_scheduling_route(flags)

            st.text_area("Scheduling Request Output", value=output, height=220)
            st.info(f"Routing: {route} | Reason: {reason}")

            if route == "Moses Email":
                st.text_area(
                    "Email Output for Moses Torres",
                    value=f"Subject: LACB | Scheduling Request | {order_no or 'N/A'}\n\n{output}\n",
                    height=220,
                )
            elif route == "Direct Scheduling":
                st.caption(f"Suggested target date: {add_business_days(date.today(), 14).isoformat()}")

    with tab2:
        zip_code = st.text_input("Enter ZIP code", max_chars=5)
        if zip_code:
            region = lookup_region_by_zip(zip_code)
            if not region:
                st.error("ZIP not found in Region_Map.csv.")
            else:
                st.success(f"Region: {region}")
                installers = get_installer_priority_for_region(region)

                route = "Moses Email" if normalize_region_for_dashboard(region) == "PS" else "Direct Scheduling"
                st.info(f"Default Routing: {route}")
                suggested_target_date = add_business_days(date.today(), 14).isoformat()

                if route == "Direct Scheduling":
                    st.caption(f"Suggested target date: {suggested_target_date}")

                if installers:
                    grouped_installers = group_installers_by_priority(installers)
                    m1, m2, m3 = st.columns(3)
                    m1.metric("P1 Installers", len(grouped_installers["P1"]))
                    m2.metric("P2 Installers", len(grouped_installers["P2"]))
                    m3.metric("P3 Installers", len(grouped_installers["P3"]))

                    for priority in ["P1", "P2", "P3"]:
                        items = grouped_installers[priority]
                        if not items:
                            continue
                        st.markdown(f"**{priority} Priority**")
                        out = pd.DataFrame(
                            [
                                {
                                    "Priority Order": i + 1,
                                    "Installer": item["installer"],
                                }
                                for i, item in enumerate(items)
                            ]
                        )
                        st.dataframe(out, use_container_width=True, hide_index=True)

                    if route == "Direct Scheduling":
                        copy_block = build_scheduling_priority_copy_block(region, installers, suggested_target_date)
                        st.markdown("**Copy Block**")
                        st.code(copy_block, language="text")
                else:
                    st.warning("No installers found for that region in Regional Dashboard.")

def parse_eta_window_days(window_text: str) -> tuple[int, int]:
    txt = clean_text(window_text).lower()

    mixed = re.search(r"(\d+)\s*day[s]?\s*-\s*(\d+)\s*week[s]?", txt)
    if mixed:
        return int(mixed.group(1)), int(mixed.group(2)) * 7

    weeks = re.search(r"(\d+)\s*-\s*(\d+)\s*week[s]?", txt)
    if weeks:
        return int(weeks.group(1)) * 7, int(weeks.group(2)) * 7

    one_week = re.search(r"(\d+)\s*week[s]?", txt)
    if one_week:
        days = int(one_week.group(1)) * 7
        return days, days

    days_rng = re.search(r"(\d+)\s*-\s*(\d+)\s*day[s]?", txt)
    if days_rng:
        return int(days_rng.group(1)), int(days_rng.group(2))

    one_day = re.search(r"(\d+)\s*day[s]?", txt)
    if one_day:
        days = int(one_day.group(1))
        return days, days

    return 0, 0


def eta_checker_page():
    st.title("ETA Checker")
    st.caption("Estimate turnaround ETA date range by product type, vendor, and date sent to vendor.")

    product_options = sorted(list(ETA_REFERENCE.keys()))
    product_type = st.selectbox("Shade/Product Type", product_options, key="eta_product_type")

    vendor_rows = ETA_REFERENCE.get(product_type, [])
    vendor_options = [v["vendor"] for v in vendor_rows]

    vendor = st.selectbox("Vendor", vendor_options, key="eta_vendor")
    sent_date = st.date_input("Date Sent To Vendor", value=date.today(), key="eta_sent_date")

    selected = next((r for r in vendor_rows if r["vendor"] == vendor), None)
    if not selected:
        st.warning("No ETA reference found for selected product/vendor.")
        return

    turnaround_text = selected.get("turnaround", "")
    preschedule_text = selected.get("preschedule", "")

    min_days, max_days = parse_eta_window_days(turnaround_text)
    pre_min_days, pre_max_days = parse_eta_window_days(preschedule_text)

    eta_start = sent_date + timedelta(days=min_days)
    eta_end = sent_date + timedelta(days=max_days)

    st.subheader("ETA Output")
    c1, c2, c3 = st.columns(3)
    c1.metric("Turnaround Window", turnaround_text or "N/A")
    c2.metric("ETA Start", eta_start.strftime("%Y-%m-%d") if min_days > 0 else "N/A")
    c3.metric("ETA End", eta_end.strftime("%Y-%m-%d") if max_days > 0 else "N/A")

    st.write(f"**Product:** {product_type}")
    st.write(f"**Vendor:** {vendor}")
    st.write(f"**Date Sent:** {sent_date.strftime('%Y-%m-%d')}")

    if product_type in {"Roman Shades", "Drapery"}:
        st.warning(
            "Reminder: send an internal message to the Drapery and Roman Shades department "
            "requesting that they contact the customer to provide an updated ETA."
        )

    if pre_min_days > 0 or pre_max_days > 0:
        pre_start = sent_date + timedelta(days=pre_min_days)
        pre_end = sent_date + timedelta(days=pre_max_days)
        st.info(
            f"Preschedule window: {preschedule_text} | Suggested preschedule ETA: "
            f"{pre_start.strftime('%Y-%m-%d')} to {pre_end.strftime('%Y-%m-%d')}"
        )

def get_selected_ticket_context_for_assistant() -> str:
    ticket_id = st.session_state.get("selected_ticket_id")
    ticket = get_ticket_by_id(ticket_id) if ticket_id else None
    if not ticket:
        return ""

    return (
        f"Customer: {ticket.get('customer_name', '')}\n"
        f"Order #: {ticket.get('order_no', '')}\n"
        f"Phone: {ticket.get('phone', '')}\n"
        f"Issue Type: {ticket.get('issue_type', '')}\n"
        f"Request Type: {ticket.get('request_type', '')}\n"
        f"Stage: {ticket.get('hubspot_stage', '')}\n"
        f"Status: {ticket.get('internal_status', '')}\n"
        f"Assigned Agent: {ticket.get('assigned_agent', '')}\n"
        f"Notes: {ticket.get('notes_summary', '')}\n"
    )


def apply_assistant_reset():
    if st.session_state.pop("assistant_reset_requested", False):
        st.session_state["assistant_chat_history"] = []
        st.session_state["assistant_last_output"] = ""
        st.session_state["assistant_last_no_api_package"] = None
        st.session_state["assistant_user_prompt"] = ""


def lacb_assistant_page():
    apply_assistant_reset()

    st.title("LACB Assistant")
    st.caption("Chat-style drafting assistant for HubSpot tickets, CRM notes, SMS, emails, and scheduling requests.")

    assistant_ready, _, assistant_health_message = get_assistant_health_status()
    if assistant_ready:
        st.success(assistant_health_message)
    else:
        st.warning(assistant_health_message)

    with st.expander("Assistant Settings & Diagnostics", expanded=False):
        has_pkg = OpenAI is not None
        has_key = bool(os.getenv("OPENAI_API_KEY", "").strip())
        pkg_label = "Yes" if has_pkg else "No"
        key_label = "Yes" if has_key else "No"
        st.write(f"openai package detected: {pkg_label}")
        st.write(f"API key detected: {key_label}")
        st.write(f"API key preview: {get_masked_api_key_preview()}")
        if st.button("Test OpenAI Connection", key="assistant_test_connection_btn"):
            ok, msg = test_openai_connection()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
    no_api_mode = st.checkbox(
        "Use No-API Mode (local templates/rules)",
        key="assistant_no_api_mode",
        value=True,
    )
    if no_api_mode:
        st.info("No-API Mode is ON. Drafts are generated locally without OpenAI API usage.")

    st.selectbox(
        "Assistant Mode",
        [
            "General",
            "Create HubSpot Ticket",
            "Create CRM Note",
            "Draft SMS",
            "Draft Email",
            "Scheduling Request",
            "Summarize Notes",
        ],
        key="assistant_mode",
    )

    use_context = st.checkbox(
        "Use selected ticket context",
        key="assistant_use_ticket_context",
        value=False,
    )

    ticket_context = ""
    if use_context:
        ticket_context = get_selected_ticket_context_for_assistant()
        st.text_area(
            "Current Ticket Context",
            value=ticket_context,
            height=180,
            key="assistant_ticket_context_preview",
        )

    prompt_help = {
        "General": "Example: Create a HubSpot ticket from this call note.",
        "Create HubSpot Ticket": "Paste notes and ask for a full HubSpot ticket draft.",
        "Create CRM Note": "Paste notes and ask for PROBLEM / RESOLUTION / EXPECTATION.",
        "Draft SMS": "Ask for a short customer update text.",
        "Draft Email": "Ask for a customer or internal email draft.",
        "Scheduling Request": "Ask for the scheduling block and internal note.",
        "Summarize Notes": "Paste a long note block and ask for a concise summary.",
    }


    st.info(prompt_help.get(st.session_state["assistant_mode"], ""))

    st.markdown("### Prompt Templates")
    template_map = get_all_prompt_templates()
    tpl1, tpl2 = st.columns([3, 1])
    selected_template = tpl1.selectbox(
        "Load a saved prompt",
        [""] + list(template_map.keys()),
        key="assistant_template_select",
    )
    if tpl2.button("Load Template") and selected_template:
        st.session_state["assistant_user_prompt"] = template_map[selected_template]

    tname_col, tsave_col, tdel_col = st.columns([3, 1, 1])
    custom_template_name = tname_col.text_input(
        "Template name",
        placeholder="Example: Vendor Follow-Up (Internal)",
        key="assistant_new_template_name",
    )
    if tsave_col.button("Save Current Prompt"):
        ok, msg = save_custom_prompt_template(
            custom_template_name,
            st.session_state.get("assistant_user_prompt", ""),
        )
        if ok:
            st.success(msg)
            st.rerun()
        else:
            st.warning(msg)

    if tdel_col.button("Delete Selected"):
        if selected_template.startswith("Custom: "):
            custom_name = selected_template.replace("Custom: ", "", 1)
            ok, msg = delete_custom_prompt_template(custom_name)
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.warning(msg)
        else:
            st.warning("Select a custom template to delete.")

    st.text_area(
        "What should the assistant do?",
        height=180,
        placeholder="Example: Create a HubSpot ticket for customer Jane Smith, order 59421. Customer states the motor is not working and the shade is stuck open.",
        key="assistant_user_prompt",
    )

    c1, c2 = st.columns(2)
    run_disabled = (not assistant_ready) and (not no_api_mode)
    if c1.button("Run Assistant", disabled=run_disabled):
        user_prompt = clean_text(st.session_state.get("assistant_user_prompt", ""))
        if user_prompt:
            if no_api_mode:
                pkg = generate_no_api_assistant_package(
                    user_prompt=user_prompt,
                    mode=st.session_state.get("assistant_mode", "General"),
                    ticket_context=ticket_context,
                )
                response_text = pkg.get("output_text", "")
                st.session_state["assistant_last_no_api_package"] = pkg
            else:
                response_text = ask_lacb_assistant(
                    user_prompt=user_prompt,
                    ticket_context=ticket_context,
                    chat_history=st.session_state.get("assistant_chat_history", []),
                )
                st.session_state["assistant_last_no_api_package"] = None

            st.session_state["assistant_chat_history"].append(
                {"role": "user", "content": user_prompt}
            )
            st.session_state["assistant_chat_history"].append(
                {"role": "assistant", "content": response_text}
            )
            st.session_state["assistant_last_output"] = response_text
            st.rerun()

    if c2.button("Clear Assistant Chat"):
        st.session_state["assistant_reset_requested"] = True
        st.rerun()

    if st.session_state.get("assistant_last_output"):
        st.subheader("Assistant Output")
        st.text_area(
            "Response",
            value=st.session_state["assistant_last_output"],
            height=320,
            key="assistant_output_display",
        )

        pkg = st.session_state.get("assistant_last_no_api_package")
        if pkg:
            if st.button("Send To HubSpot Builder V9", key="assistant_send_to_builder_btn"):
                ok, msg = send_no_api_package_to_builder(pkg)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.warning(msg)


    history = st.session_state.get("assistant_chat_history", [])
    if history:
        st.subheader("Conversation")
        for i, msg in enumerate(history[-10:], start=1):
            speaker = "You" if msg["role"] == "user" else "Assistant"
            with st.expander(f"{speaker} {i}"):
                st.write(msg["content"])


def history_export_page():
    st.title("History / Export")

    conn = get_conn()
    df_activities = _db_read_sql_query(
        """
        SELECT
            a.id,
            a.ticket_id,
            t.cc_id,
            COALESCE(a.ticket_update_name, t.ticket_title) AS ticket_title,
            COALESCE(a.customer_name, t.customer_name) AS customer_name,
            COALESCE(a.order_no, t.order_no) AS order_no,
            a.io_channel,
            a.phone,
            a.email,
            a.activity_date AS timestamp_pacific,
            a.follow_up_date,
            a.logging_agent,
            a.assigned_owner,
            a.action_type,
            a.result_type,
            a.notes,
            a.created_at
        FROM activities a
        LEFT JOIN tickets t ON t.id = a.ticket_id
        ORDER BY a.id DESC
        """,
        conn,
    )
    conn.close()

    df_tickets = get_all_tickets_df()

    st.subheader("Activity History")
    owner_filter = st.selectbox("Owner Filter", ["All"] + KPI_AGENTS, key="history_owner_filter")
    history_search = st.text_input("History Search", key="history_search")
    date_mode = st.selectbox("Date Filter", ["All Dates", "Single Date", "Date Range"], key="history_date_mode")

    filter_date = None
    start_date = None
    end_date = None
    if date_mode == "Single Date":
        filter_date = st.date_input("Select Date", value=date.today(), key="history_single_date")
    elif date_mode == "Date Range":
        d1, d2 = st.columns(2)
        start_date = d1.date_input("Start Date", value=date.today() - timedelta(days=7), key="history_start_date")
        end_date = d2.date_input("End Date", value=date.today(), key="history_end_date")

    filtered_activities = df_activities.copy()
    if owner_filter != "All" and not filtered_activities.empty:
        filtered_activities = filtered_activities[
            (filtered_activities["logging_agent"] == owner_filter)
            | (filtered_activities["assigned_owner"] == owner_filter)
        ]

    if history_search and not filtered_activities.empty:
        s = history_search.lower()
        mask = (
            filtered_activities["ticket_title"].fillna("").str.lower().str.contains(s)
            | filtered_activities["customer_name"].fillna("").str.lower().str.contains(s)
            | filtered_activities["order_no"].fillna("").str.lower().str.contains(s)
            | filtered_activities["notes"].fillna("").str.lower().str.contains(s)
        )
        filtered_activities = filtered_activities[mask]

    if not filtered_activities.empty and "timestamp_pacific" in filtered_activities.columns:
        activity_dates = filtered_activities["timestamp_pacific"].fillna("").astype(str).str[:10]
        if date_mode == "Single Date" and filter_date is not None:
            filtered_activities = filtered_activities[activity_dates == filter_date.isoformat()]
        elif date_mode == "Date Range" and start_date is not None and end_date is not None:
            filtered_activities = filtered_activities[
                activity_dates.between(start_date.isoformat(), end_date.isoformat())
            ]

    st.dataframe(filtered_activities, use_container_width=True)

    if not filtered_activities.empty:
        edit_options = {
            f"#{int(row['id'])} | {row.get('timestamp_pacific', '') or '-'} | {row.get('customer_name', '') or 'N/A'} | {row.get('result_type', '') or 'N/A'}": int(row["id"])
            for _, row in filtered_activities.iterrows()
        }
        selected_activity_label = st.selectbox(
            "Edit / Delete Activity Entry",
            list(edit_options.keys()),
            key="history_edit_activity_select",
        )
        selected_activity_id = edit_options[selected_activity_label]
        selected_activity_row = filtered_activities[filtered_activities["id"] == selected_activity_id].iloc[0]

        default_channels = [x for x in _split_multi_value(selected_activity_row.get("io_channel", "")) if x in IO_CHANNELS]
        default_actions = [x for x in _split_multi_value(selected_activity_row.get("action_type", "")) if x in IO_TASK_ACTIONS]
        default_outcomes = [x for x in _split_multi_value(selected_activity_row.get("result_type", "")) if x in IO_OUTCOMES]
        default_follow_up_value = clean_text(selected_activity_row.get("follow_up_date", ""))
        default_follow_up = _parse_app_timestamp(default_follow_up_value)
        default_follow_up = default_follow_up.date() if default_follow_up else add_business_days(date.today(), 1)
        if st.session_state.get("history_edit_follow_up_target_id") != selected_activity_id:
            st.session_state["history_edit_follow_up_target_id"] = selected_activity_id
            st.session_state["history_edit_has_follow_up_date"] = bool(default_follow_up_value)

        st.checkbox("Set Follow-Up Date", key="history_edit_has_follow_up_date")

        with st.form("history_edit_activity_form"):
            h1, h2 = st.columns(2)
            with h1:
                edit_channels = st.multiselect("Channel", IO_CHANNELS, default=default_channels)
                edit_actions = st.multiselect("Action Type", IO_TASK_ACTIONS, default=default_actions)
                edit_outcomes = st.multiselect("Outcome", IO_OUTCOMES, default=default_outcomes)
            with h2:
                edit_follow_up_date = None
                if st.session_state.get("history_edit_has_follow_up_date", False):
                    edit_follow_up_date = st.date_input("Follow-Up Date", value=default_follow_up)
                else:
                    st.caption("Follow-Up Date not set for this activity.")
                edit_notes = st.text_area("Notes", value=clean_text(selected_activity_row.get("notes", "")), height=140)

            b1, b2 = st.columns(2)
            save_activity_edit = b1.form_submit_button("Save Activity Changes")
            delete_activity_edit = b2.form_submit_button("Delete Activity Entry")

        if save_activity_edit:
            ok, msg = update_activity_entry_fields(
                activity_id=selected_activity_id,
                io_channels=edit_channels,
                action_types=edit_actions,
                result_types=edit_outcomes,
                follow_up_date=edit_follow_up_date.isoformat() if edit_follow_up_date else "",
                notes=edit_notes,
                ticket_id=_safe_optional_int(selected_activity_row.get("ticket_id", "")),
            )
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.warning(msg)

        if delete_activity_edit:
            deleted = delete_activity_by_id(selected_activity_id)
            if deleted:
                st.success("Activity entry deleted.")
                st.rerun()
            else:
                st.warning("Could not delete activity entry.")

    st.subheader("Ticket History")
    filtered_tickets = df_tickets.copy()
    if owner_filter != "All" and not filtered_tickets.empty:
        filtered_tickets = filtered_tickets[filtered_tickets["assigned_agent"] == owner_filter]

    if history_search and not filtered_tickets.empty:
        s = history_search.lower()
        mask = (
            filtered_tickets["ticket_title"].fillna("").str.lower().str.contains(s)
            | filtered_tickets["customer_name"].fillna("").str.lower().str.contains(s)
            | filtered_tickets["order_no"].fillna("").str.lower().str.contains(s)
            | filtered_tickets["notes_summary"].fillna("").str.lower().str.contains(s)
        )
        filtered_tickets = filtered_tickets[mask]

    if not filtered_tickets.empty and "created_at" in filtered_tickets.columns:
        ticket_dates = filtered_tickets["created_at"].fillna("").astype(str).str[:10]
        if date_mode == "Single Date" and filter_date is not None:
            filtered_tickets = filtered_tickets[ticket_dates == filter_date.isoformat()]
        elif date_mode == "Date Range" and start_date is not None and end_date is not None:
            filtered_tickets = filtered_tickets[
                ticket_dates.between(start_date.isoformat(), end_date.isoformat())
            ]

    st.dataframe(filtered_tickets, use_container_width=True)

    st.subheader("Ticket Timeline Lookup")
    if not df_tickets.empty:
        options = {
            f"{row.get('cc_id', '') or '-'} | {int(row['id'])} | {row.get('ticket_title', '')}": int(row["id"])
            for _, row in df_tickets.iterrows()
        }
        selected = st.selectbox("Select Ticket for Timeline", list(options.keys()), key="history_timeline_select")
        selected_id = options[selected]

        timeline = filtered_activities[filtered_activities["ticket_id"] == selected_id]
        if timeline.empty:
            st.info("No activity history for this ticket yet.")
        else:
            st.dataframe(timeline, use_container_width=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.download_button(
            "Download Activities CSV",
            data=filtered_activities.to_csv(index=False).encode("utf-8"),
            file_name="lacb_activity_history.csv",
            mime="text/csv",
        )
    with c2:
        st.download_button(
            "Download Activities XLSX",
            data=dataframe_to_excel_bytes(filtered_activities, "Activities"),
            file_name="lacb_activity_history.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with c3:
        st.download_button(
            "Download Tickets CSV",
            data=filtered_tickets.to_csv(index=False).encode("utf-8"),
            file_name="lacb_ticket_history.csv",
            mime="text/csv",
        )
    with c4:
        st.download_button(
            "Download Tickets XLSX",
            data=dataframe_to_excel_bytes(filtered_tickets, "Tickets"),
            file_name="lacb_ticket_history.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# =========================================================
# NAVIGATION
# =========================================================

PAGES = {
    "Follow-Up Assistant": follow_up_assistant_page,
    "KPI Dashboard": kpi_dashboard_page,
    "KPI Graph Dashboard": kpi_graph_dashboard_page,
    "Inbound / Outbound Activity Log": inbound_outbound_activity_log_page,
    "Ticket Tracker": ticket_tracker_page,
    "Quick Activity Log": quick_activity_log_page,
    "HubSpot Ticket Builder V9": hubspot_ticket_builder_page_v9,
    "Service / Repair Builder": service_repair_builder_page,
    "Scheduling Assistant": scheduling_assistant_page,
    "ETA Checker": eta_checker_page,
    "LACB Assistant": lacb_assistant_page,
    "History / Export": history_export_page,
}


# =========================================================
# MAIN
# =========================================================

def main():
    init_db()
    init_v9_state()

    st.sidebar.title("LACB Command Center")
    page = st.sidebar.radio("Go to", list(PAGES.keys()), index=list(PAGES.keys()).index(st.session_state["selected_page"]))
    st.session_state["selected_page"] = page

    st.sidebar.markdown("---")
    st.sidebar.write(f"**Today:** {today_str()}")
    st.sidebar.write("**Version:** V9 Starter")

    PAGES[page]()


if __name__ == "__main__":
    main()





























































