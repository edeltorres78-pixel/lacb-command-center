import io
import os
import re
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


# =========================================================
# CONFIG
# =========================================================

APP_TITLE = "LACB Customer Care Command Center V9"
DB_PATH = "lacb_command_center.db"

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
    "Inbound RC",
    "Outbound RC",
    "SMS",
    "RC Chat Scheduling",
    "Email",
    "Service Order Created",
    "Internal RC Chat",
    "Hubspot Ticket Update",
    "Task Review",
]

IO_TASK_ACTIONS = [
    "New Ticket Assigned - Receptionist",
    "New Ticket Assigned - Supervisor",
    "New Ticket Assigned - Sales",
    "New Ticket Assigned - Other",
    "New Ticket Created",
    "Ticket Updated/Notes",
    "Ticket Closed",
    "Ticket Reassigned/ReOpened",
    "Task Updated",
    "Task Marked Complete",
]

IO_OUTCOMES = [
    "Closed/Resolved",
    "Waiting on Customer",
    "Waiting on Vendor",
    "Waiting on DC",
    "Waiting on Internal Dpt",
    "Waiting on Scheduling Team",
    "Scheduled",
    "Follow-up Needed",
    "Escalated",
]
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
    ],
    "Drapery": [
        {"vendor": "Richard Williams", "turnaround": "6-8 WEEKS", "preschedule": "9-10 WEEKS"},
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
    return sqlite3.connect(DB_PATH, check_same_thread=False)


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


def ensure_cc_id_schema_and_backfill(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(tickets)")
    cols = [r[1] for r in cur.fetchall()]
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


def init_db():
    conn = get_conn()
    cur = conn.cursor()
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

    # Backfill/extend activities schema for inbound-outbound activity tracker
    cur.execute("PRAGMA table_info(activities)")
    act_cols = [r[1] for r in cur.fetchall()]
    if "io_channel" not in act_cols:
        cur.execute("ALTER TABLE activities ADD COLUMN io_channel TEXT")
    if "customer_name" not in act_cols:
        cur.execute("ALTER TABLE activities ADD COLUMN customer_name TEXT")
    if "order_no" not in act_cols:
        cur.execute("ALTER TABLE activities ADD COLUMN order_no TEXT")
    if "phone" not in act_cols:
        cur.execute("ALTER TABLE activities ADD COLUMN phone TEXT")
    if "ticket_update_name" not in act_cols:
        cur.execute("ALTER TABLE activities ADD COLUMN ticket_update_name TEXT")
    if "email" not in act_cols:
        cur.execute("ALTER TABLE activities ADD COLUMN email TEXT")

    ensure_cc_id_schema_and_backfill(conn)
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

        # Inbound-outbound tracker form
        "io_form_reset_requested": False,
        "io_date": date.today(),
        "io_channels": ["Inbound RingCX"],
        "io_actions": ["Ticket Updated/Notes"],
        "io_outcome": "Waiting on Customer",
        "io_phone": "",
        "io_name": "",
        "io_order": "",
        "io_agent": "Ed Torres",
        "io_ticket_name": "",
        "io_email": "",
        "io_notes": "",
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# =========================================================
# HELPERS
# =========================================================

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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


def clean_text(value: str) -> str:
    return (value or "").strip()

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
    for canonical, aliases in REGION_ALIASES.items():
        if val in [a.upper() for a in aliases]:
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
        if target == "OC" and (nu == "OC" or "ORANGECOUNTY" in nu):
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


def extract_labeled_value(raw_text: str, labels: list[str]) -> str:
    text = clean_text(raw_text)
    if not text:
        return ""
    for label in labels:
        pattern = rf"(?im)^\s*{re.escape(label)}\s*:\s*(.+)\s*$"
        m = re.search(pattern, text)
        if m:
            return clean_text(m.group(1))
    return ""


def normalize_option(value: str, options: list[str], fallback: str = "") -> str:
    val = clean_text(value)
    if not val:
        return fallback

    for opt in options:
        if val.lower() == opt.lower():
            return opt

    for opt in options:
        if val.lower() in opt.lower() or opt.lower() in val.lower():
            return opt

    return fallback


def extract_pre_sections(raw_text: str) -> tuple[str, str, str]:
    text = clean_text(raw_text)
    if not text:
        return "", "", ""

    problem = ""
    resolution = ""
    expectation = ""

    m_problem = re.search(r"(?is)PROBLEM:\s*(.*?)(?=\n\s*RESOLUTION:|\Z)", text)
    if m_problem:
        problem = clean_text(m_problem.group(1))

    m_resolution = re.search(r"(?is)RESOLUTION:\s*(.*?)(?=\n\s*EXPECTATION:|\Z)", text)
    if m_resolution:
        resolution = clean_text(m_resolution.group(1))

    m_expectation = re.search(r"(?is)EXPECTATION:\s*(.*?)(?=\n\s*[A-Z][A-Z ]{2,}:|\Z)", text)
    if m_expectation:
        expectation = clean_text(m_expectation.group(1))

    return problem, resolution, expectation


def parse_chat_style_ticket(raw_text: str) -> dict:
    text = clean_text(raw_text)
    pref = extract_prefill(text)

    ticket_name = extract_labeled_value(text, ["Ticket Name", "HubSpot Ticket Name", "Ticket/Update name"])
    owner = extract_labeled_value(text, ["Owner", "Assigned Agent"])
    stage = extract_labeled_value(text, ["Stage", "HubSpot Stage"])
    contact = extract_labeled_value(text, ["Contact", "Customer Name"])
    order_no_labeled = extract_labeled_value(text, ["Order #", "Order Number"])
    assigned_by_labeled = extract_labeled_value(text, ["Assigned By"])

    customer_name = pref.get("customer_name", "")
    if contact:
        name_guess = clean_text(re.sub(r"\(.*?\)", "", contact).split("|")[0])
        if name_guess:
            customer_name = name_guess

    order_no = order_no_labeled or pref.get("order_no", "")
    phone = pref.get("phone", "")
    if contact and not phone:
        phone_match = re.search(r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", contact)
        if phone_match:
            phone = clean_text(phone_match.group(1))

    issue_type = normalize_option(pref.get("issue_type", ""), ISSUE_TYPES, "Other")
    request_type = normalize_option(pref.get("request_type", ""), REQUEST_TYPES, "Service")
    assigned_agent = normalize_option(owner, OWNERS, "Ed Torres")
    assigned_by = normalize_option(assigned_by_labeled, ASSIGNED_BY_OPTIONS, "")
    hubspot_stage = normalize_option(stage, HUBSPOT_STAGES, "")

    problem, resolution, expectation = extract_pre_sections(text)
    summary = ""
    crm_note = ""
    if problem and resolution and expectation:
        summary = (
            f"PROBLEM: {problem}\n\n"
            f"RESOLUTION: {resolution}\n\n"
            f"EXPECTATION: {expectation}"
        )
        crm_note = summary

    if not hubspot_stage:
        lower = text.lower()
        if "closed" in lower or "ticket closed" in lower:
            hubspot_stage = "Closed"
        elif "awaiting customer response" in lower or "waiting on customer" in lower:
            hubspot_stage = "Awaiting Customer Response"
        elif "schedule" in lower:
            hubspot_stage = "Scheduling Pending"
        else:
            hubspot_stage = "New"

    internal_status = "Open"
    if hubspot_stage == "Closed":
        internal_status = "Closed"
    elif hubspot_stage == "Awaiting Customer Response":
        internal_status = "Pending Customer"
    elif hubspot_stage == "Scheduling Pending":
        internal_status = "Pending Scheduling"
    elif "vendor" in text.lower():
        internal_status = "Pending Internal"

    return {
        "ticket_name": ticket_name,
        "customer_name": customer_name,
        "order_no": order_no,
        "phone": phone,
        "issue_type": issue_type,
        "request_type": request_type,
        "ticket_source": request_type_to_source(request_type),
        "assigned_by": assigned_by,
        "assigned_agent": assigned_agent,
        "hubspot_stage": hubspot_stage,
        "internal_status": internal_status,
        "summary": summary,
        "crm_note": crm_note,
    }


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

    if pending.get("assigned_by") in ASSIGNED_BY_OPTIONS:
        st.session_state["builder_assigned_by"] = pending["assigned_by"]

    if pending.get("assigned_agent") in OWNERS:
        st.session_state["builder_assigned_agent"] = pending["assigned_agent"]

    if pending.get("hubspot_stage") in HUBSPOT_STAGES:
        st.session_state["builder_hubspot_stage"] = pending["hubspot_stage"]

    if pending.get("internal_status") in INTERNAL_STATUSES:
        st.session_state["builder_internal_status"] = pending["internal_status"]
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
    df = pd.read_sql_query(
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


def save_inbound_outbound_entry(entry: dict):
    raw_channel = entry.get("io_channel")
    if isinstance(raw_channel, list):
        channel_labels = [clean_text(x) for x in raw_channel if clean_text(x)]
        channel = ", ".join(channel_labels)
    else:
        channel = clean_text(raw_channel)
    raw_action = entry.get("action_type")
    if isinstance(raw_action, list):
        action_labels = [clean_text(x) for x in raw_action if clean_text(x)]
        action = ", ".join(action_labels)
    else:
        action = clean_text(raw_action)
    outcome = clean_text(entry.get("result_type"))
    customer_name = clean_text(entry.get("customer_name"))
    order_no = clean_text(entry.get("order_no"))
    phone = clean_text(entry.get("phone"))
    ticket_update_name = clean_text(entry.get("ticket_update_name"))
    email = clean_text(entry.get("email"))
    notes = clean_text(entry.get("notes"))
    logging_agent = clean_text(entry.get("logging_agent")) or "Ed Torres"
    assigned_owner = clean_text(entry.get("assigned_owner")) or logging_agent

    activity_date_raw = clean_text(entry.get("activity_date"))
    if not activity_date_raw:
        activity_date_raw = today_str()

    activity_ts = f"{activity_date_raw} 09:00:00" if len(activity_date_raw) == 10 else activity_date_raw

    conn = get_conn()
    cur = conn.cursor()

    ticket_id = None
    if order_no:
        cur.execute("SELECT id FROM tickets WHERE order_no = ? ORDER BY id DESC LIMIT 1", (order_no,))
        row = cur.fetchone()
        if row:
            ticket_id = int(row[0])

    if ticket_id is None and customer_name:
        cur.execute("SELECT id FROM tickets WHERE customer_name = ? ORDER BY id DESC LIMIT 1", (customer_name,))
        row = cur.fetchone()
        if row:
            ticket_id = int(row[0])

    if ticket_id is None:
        ts = now_ts()
        ticket_title = ticket_update_name or build_ticket_name("Service", "Other", customer_name, order_no)
        cur.execute(
            """
            INSERT INTO tickets (
                ticket_title, customer_name, order_no, phone, issue_type, request_type, ticket_source,
                assigned_by, assigned_agent, hubspot_stage, internal_status, attempt_no,
                last_activity_date, next_followup_date, scheduling_flags, notes_summary, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticket_title, customer_name, order_no, phone,
                "Other", "Service", "Internal", "Other", assigned_owner,
                "New", "Open", 1,
                activity_date_raw[:10] if activity_date_raw else today_str(),
                "", "", notes, ts, ts,
            ),
        )
        ticket_id = int(cur.lastrowid)

    cur.execute(
        """
        INSERT INTO activities (
            ticket_id, activity_date, logging_agent, assigned_owner, action_type, result_type, notes, created_at,
            io_channel, customer_name, order_no, phone, ticket_update_name, email
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticket_id, activity_ts, logging_agent, assigned_owner, action, outcome, notes, now_ts(),
            channel, customer_name, order_no, phone, ticket_update_name, email,
        ),
    )

    cur.execute(
        """
        UPDATE tickets
        SET
            customer_name = COALESCE(NULLIF(?, ''), customer_name),
            order_no = COALESCE(NULLIF(?, ''), order_no),
            phone = COALESCE(NULLIF(?, ''), phone),
            ticket_title = COALESCE(NULLIF(?, ''), ticket_title),
            notes_summary = COALESCE(NULLIF(?, ''), notes_summary),
            assigned_agent = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (customer_name, order_no, phone, ticket_update_name, notes, assigned_owner, now_ts(), ticket_id),
    )

    conn.commit()
    conn.close()


def update_inbound_outbound_entry(entry_id: int, payload: dict) -> bool:
    if not entry_id:
        return False

    raw_channel = payload.get("io_channel")
    if isinstance(raw_channel, list):
        channel = ", ".join([clean_text(x) for x in raw_channel if clean_text(x)])
    else:
        channel = clean_text(raw_channel)

    raw_action = payload.get("action_type")
    if isinstance(raw_action, list):
        action = ", ".join([clean_text(x) for x in raw_action if clean_text(x)])
    else:
        action = clean_text(raw_action)

    activity_date = clean_text(payload.get("activity_date"))
    result_type = clean_text(payload.get("result_type"))
    phone = clean_text(payload.get("phone"))
    customer_name = clean_text(payload.get("customer_name"))
    order_no = clean_text(payload.get("order_no"))
    ticket_update_name = clean_text(payload.get("ticket_update_name"))
    email = clean_text(payload.get("email"))
    notes = clean_text(payload.get("notes"))
    logging_agent = clean_text(payload.get("logging_agent")) or "Ed Torres"
    assigned_owner = clean_text(payload.get("assigned_owner")) or logging_agent

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE activities
        SET
            activity_date = ?,
            io_channel = ?,
            action_type = ?,
            result_type = ?,
            phone = ?,
            customer_name = ?,
            order_no = ?,
            ticket_update_name = ?,
            email = ?,
            notes = ?,
            logging_agent = ?,
            assigned_owner = ?
        WHERE id = ?
        """,
        (
            activity_date,
            channel,
            action,
            result_type,
            phone,
            customer_name,
            order_no,
            ticket_update_name,
            email,
            notes,
            logging_agent,
            assigned_owner,
            int(entry_id),
        ),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed

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
            action_type,
            result_type,
            notes,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticket_id,
            ts,
            logging_agent,
            assigned_owner,
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

def get_activities_for_ticket(ticket_id: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
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
        df = pd.read_sql_query("SELECT * FROM manual_tallies ORDER BY id DESC", conn)
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
        df = pd.read_sql_query(
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



def do_generate_chat_style_v9(save_to_tracker=False):
    raw_details = clean_text(st.session_state.get("builder_raw_details", ""))
    if not raw_details:
        st.warning("Paste your full note block first.")
        return

    parsed = parse_chat_style_ticket(raw_details)

    # Chat-style mode prefers extracted values from the note block.
    final_customer_name = clean_text(parsed.get("customer_name"))
    final_order_no = clean_text(parsed.get("order_no"))
    final_phone = clean_text(parsed.get("phone"))
    final_request_type = normalize_option(parsed.get("request_type", ""), REQUEST_TYPES, st.session_state.get("builder_request_type", "Service"))
    final_issue_type = normalize_option(parsed.get("issue_type", ""), ISSUE_TYPES, st.session_state.get("builder_issue_type", "Other"))
    final_ticket_source = normalize_option(parsed.get("ticket_source", ""), TICKET_SOURCES, st.session_state.get("builder_ticket_source", "Phone"))
    final_assigned_by = normalize_option(parsed.get("assigned_by", ""), ASSIGNED_BY_OPTIONS, st.session_state.get("builder_assigned_by", "Receptionist"))
    final_assigned_agent = normalize_option(parsed.get("assigned_agent", ""), OWNERS, st.session_state.get("builder_assigned_agent", "Ed Torres"))
    final_stage = normalize_option(parsed.get("hubspot_stage", ""), HUBSPOT_STAGES, st.session_state.get("builder_hubspot_stage", "New"))
    final_status = normalize_option(parsed.get("internal_status", ""), INTERNAL_STATUSES, st.session_state.get("builder_internal_status", "Open"))
    final_attempt_no = int(st.session_state.get("builder_attempt_no", 1) or 1)

    ticket_name = clean_text(parsed.get("ticket_name"))
    if not ticket_name:
        ticket_name = build_ticket_name(
            final_request_type,
            final_issue_type,
            final_customer_name,
            final_order_no,
        )

    summary = clean_text(parsed.get("summary"))
    if not summary:
        summary = build_hubspot_summary(
            final_customer_name,
            final_order_no,
            final_phone,
            final_request_type,
            final_issue_type,
            final_assigned_by,
            final_assigned_agent,
            final_stage,
            final_status,
            final_attempt_no,
            raw_details,
        )

    crm_note = clean_text(parsed.get("crm_note"))
    if not crm_note:
        crm_note = build_crm_note(raw_details, final_request_type, final_issue_type)

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
            ticket_source=final_ticket_source,
            assigned_by=final_assigned_by,
            assigned_agent=final_assigned_agent,
            hubspot_stage=final_stage,
            internal_status=final_status,
            attempt_no=final_attempt_no,
            notes_summary=summary,
        )
        if ok:
            st.success(msg)
        else:
            st.warning(msg)
    else:
        st.success("Assistant-style ticket draft generated.")


# =========================================================
# PAGES
# =========================================================

def follow_up_assistant_page():
    st.title("Follow-Up Assistant")

    df = get_all_tickets_df()
    if df.empty:
        st.info("No tickets found.")
        return

    df["attempt_no"] = pd.to_numeric(df["attempt_no"], errors="coerce").fillna(1).astype(int)
    df["recommended_action"] = df.apply(
        lambda row: recommended_next_action(row["attempt_no"], str(row["internal_status"])),
        axis=1,
    )

    st.subheader("Open Follow-Up Queue")
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
            "last_activity_date",
            "recommended_action",
        ]
    ]
    st.dataframe(view_df, use_container_width=True)


def kpi_dashboard_page():
    st.title("KPI Dashboard")

    owner_filter = st.selectbox("Owner View", ["Combined"] + OWNERS, key="kpi_owner_view")
    time_view = st.selectbox("Time View", ["Daily", "Weekly", "Monthly", "Custom Range"], key="kpi_time_view")

    today_dt = date.today()
    if time_view == "Daily":
        selected_day = st.date_input("Date", value=today_dt, key="kpi_day")
        start_date = selected_day
        end_date = selected_day
    elif time_view == "Weekly":
        selected_week_day = st.date_input("Any Date in Week", value=today_dt, key="kpi_week")
        start_date = selected_week_day - timedelta(days=selected_week_day.weekday())
        end_date = start_date + timedelta(days=6)
    elif time_view == "Monthly":
        selected_month_day = st.date_input("Any Date in Month", value=today_dt, key="kpi_month")
        start_date = selected_month_day.replace(day=1)
        if selected_month_day.month == 12:
            next_month = selected_month_day.replace(year=selected_month_day.year + 1, month=1, day=1)
        else:
            next_month = selected_month_day.replace(month=selected_month_day.month + 1, day=1)
        end_date = next_month - timedelta(days=1)
    else:
        c1, c2 = st.columns(2)
        start_date = c1.date_input("Start Date", value=today_dt - timedelta(days=7), key="kpi_custom_start")
        end_date = c2.date_input("End Date", value=today_dt, key="kpi_custom_end")
        if end_date < start_date:
            st.warning("End date cannot be before start date.")
            return

    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()

    conn = get_conn()
    df_tickets = pd.read_sql_query("SELECT * FROM tickets ORDER BY id DESC", conn)
    df_activities = pd.read_sql_query("SELECT * FROM activities ORDER BY id DESC", conn)
    conn.close()

    if not df_tickets.empty:
        df_tickets = df_tickets[df_tickets["created_at"].astype(str).str[:10].between(start_iso, end_iso)]
        if owner_filter != "Combined":
            df_tickets = df_tickets[df_tickets["assigned_agent"] == owner_filter]

    if not df_activities.empty:
        activity_dates = df_activities["activity_date"].astype(str).str[:10]
        df_activities = df_activities[activity_dates.between(start_iso, end_iso)]
        if owner_filter != "Combined":
            df_activities = df_activities[df_activities["assigned_owner"] == owner_filter]

    st.caption(f"Showing KPI from {start_iso} to {end_iso}")

    def _count_action(label: str) -> int:
        if df_activities.empty:
            return 0
        return int(df_activities["action_type"].fillna("").str.contains(re.escape(label), case=False).sum())

    def _count_channel(label: str) -> int:
        if df_activities.empty or "io_channel" not in df_activities.columns:
            return 0
        return int(df_activities["io_channel"].fillna("").str.contains(re.escape(label), case=False, na=False).sum())

    tickets_assigned = (
        _count_action("New Ticket Assigned - Receptionist")
        + _count_action("New Ticket Assigned - Supervisor")
        + _count_action("New Ticket Assigned - Sales")
        + _count_action("New Ticket Assigned - Other")
    )

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Tickets Created", _count_action("New Ticket Created"))
    m2.metric("Tickets Assigned", tickets_assigned)
    m3.metric("Tickets Updated", _count_action("Ticket Updated/Notes"))
    m4.metric("Tickets Closed", _count_action("Ticket Closed"))
    m5.metric("Activity Logs", 0 if df_activities.empty else len(df_activities))

    n1, n2, n3, n4 = st.columns(4)
    n1.metric("Inbound RingCX", _count_channel("Inbound RingCX"))
    n2.metric("Outbound RingCX", _count_channel("Outbound RingCX"))
    n3.metric("Inbound RC", _count_channel("Inbound RC"))
    n4.metric("Outbound RC", _count_channel("Outbound RC"))

    n5, n6, n7 = st.columns(3)
    n5.metric("SMS", _count_channel("SMS"))
    n6.metric("Email", _count_channel("Email"))
    n7.metric("Service Orders", _count_channel("Service Order Created"))

    st.subheader("Channel Breakdown")
    channel_rows = []
    for ch in IO_CHANNELS:
        channel_rows.append({"Channel": ch, "Count": _count_channel(ch)})
    st.dataframe(pd.DataFrame(channel_rows), use_container_width=True, hide_index=True)

    st.subheader("Action Breakdown")
    action_rows = []
    for act in IO_TASK_ACTIONS:
        action_rows.append({"Action": act, "Count": _count_action(act)})
    st.dataframe(pd.DataFrame(action_rows), use_container_width=True, hide_index=True)

    st.subheader("Outcome Breakdown")
    outcome_rows = []
    if df_activities.empty:
        outcome_df = pd.DataFrame(columns=["Outcome", "Count"])
    else:
        base = df_activities["result_type"].fillna("").value_counts().reset_index()
        base.columns = ["Outcome", "Count"]
        for out in IO_OUTCOMES:
            count = int(base.loc[base["Outcome"] == out, "Count"].sum())
            outcome_rows.append({"Outcome": out, "Count": count})
        outcome_df = pd.DataFrame(outcome_rows)
    st.dataframe(outcome_df, use_container_width=True, hide_index=True)

    st.subheader("Filtered Activity Log")
    activity_cols = [
        c for c in [
            "activity_date", "io_channel", "action_type", "customer_name", "order_no", "phone", "ticket_update_name",
            "email", "result_type", "logging_agent", "assigned_owner", "notes"
        ] if c in df_activities.columns
    ]
    st.dataframe(df_activities[activity_cols] if not df_activities.empty else df_activities, use_container_width=True)

    st.subheader("Filtered Tickets Created")
    ticket_cols = [
        c for c in [
            "cc_id", "ticket_title", "customer_name", "order_no", "assigned_agent", "internal_status", "created_at"
        ] if c in df_tickets.columns
    ]
    st.dataframe(df_tickets[ticket_cols] if not df_tickets.empty else df_tickets, use_container_width=True)


def ticket_tracker_page():
    st.title("Ticket Tracker")

    if st.session_state.pop("io_form_reset_requested", False):
        st.session_state["io_date"] = date.today()
        st.session_state["io_channels"] = ["Inbound RingCX"]
        st.session_state["io_actions"] = ["Ticket Updated/Notes"]
        st.session_state["io_outcome"] = "Waiting on Customer"
        st.session_state["io_phone"] = ""
        st.session_state["io_name"] = ""
        st.session_state["io_order"] = ""
        st.session_state["io_agent"] = "Ed Torres"
        st.session_state["io_ticket_name"] = ""
        st.session_state["io_email"] = ""
        st.session_state["io_notes"] = ""

    st.subheader("Inbound-Outbound Activity Entry")
    st.caption("Excel-style entry form aligned to Inbound-outbound-Activities.")

    with st.form("io_activity_form"):
        a1, a2, a3, a4 = st.columns(4)
        io_date = a1.date_input("Date", value=date.today(), key="io_date")
        io_channels = a2.multiselect("Inbound/Outbound", IO_CHANNELS, default=["Inbound RingCX"], key="io_channels")
        io_actions = a3.multiselect("Task or action done", IO_TASK_ACTIONS, default=["Ticket Updated/Notes"], key="io_actions")
        io_outcome = a4.selectbox("Outcome", IO_OUTCOMES, key="io_outcome")

        b1, b2, b3, b4 = st.columns(4)
        io_phone = b1.text_input("Phone Number", key="io_phone")
        io_name = b2.text_input("Name", key="io_name")
        io_order = b3.text_input("Order #", key="io_order")
        io_agent = b4.selectbox("Agent", OWNERS, key="io_agent")

        c1, c2 = st.columns(2)
        io_title = c1.text_input("Ticket Name", key="io_ticket_name")
        io_email = c2.text_input("Email", key="io_email")
        io_notes = st.text_area("Notes", key="io_notes", height=140)

        io_save = st.form_submit_button("Save Activity Entry")

    if io_save:
        save_inbound_outbound_entry(
            {
                "activity_date": io_date.isoformat(),
                "io_channel": io_channels,
                "action_type": io_actions,
                "result_type": io_outcome,
                "phone": io_phone,
                "customer_name": io_name,
                "order_no": io_order,
                "ticket_update_name": io_title,
                "email": io_email,
                "notes": io_notes,
                "logging_agent": io_agent,
                "assigned_owner": io_agent,
            }
        )
        st.session_state["io_form_reset_requested"] = True
        st.success("Activity entry saved.")
        st.rerun()

    conn = get_conn()
    io_df = pd.read_sql_query("SELECT * FROM activities ORDER BY id DESC", conn)
    conn.close()

    st.markdown("### Inbound-Outbound Activity Log")
    io_cols = [c for c in ["id", "activity_date", "io_channel", "action_type", "phone", "customer_name", "order_no", "ticket_update_name", "email", "notes", "result_type", "logging_agent"] if c in io_df.columns]
    st.dataframe(io_df[io_cols] if not io_df.empty else io_df, use_container_width=True)

    if not io_df.empty:
        st.markdown("### Edit Existing Inbound-Outbound Entry")

        edit_options = {
            f"#{int(r['id'])} | {r.get('activity_date', '')} | {r.get('customer_name', '') or 'N/A'} | {r.get('order_no', '') or 'N/A'}": int(r["id"])
            for _, r in io_df.iterrows()
        }
        selected_edit_label = st.selectbox("Select Entry to Edit", list(edit_options.keys()), key="io_edit_select_entry")
        selected_edit_id = edit_options[selected_edit_label]

        st.info(f"Currently editing entry #{selected_edit_id}: {selected_edit_label}")

        st.session_state.setdefault("io_edit_loaded_id", None)

        def _split_multi(val):
            return [x.strip() for x in str(val or "").split(",") if x.strip()]

        if st.session_state.get("io_edit_loaded_id") != selected_edit_id:
            row = io_df[io_df["id"] == selected_edit_id].iloc[0]

            edit_date_default = pd.to_datetime(row.get("activity_date"), errors="coerce")
            if pd.isna(edit_date_default):
                edit_date_default = pd.to_datetime(row.get("created_at"), errors="coerce")
            if pd.isna(edit_date_default):
                edit_date_default = pd.Timestamp(date.today())

            edit_channels_default = [x for x in _split_multi(row.get("io_channel")) if x in IO_CHANNELS]
            if not edit_channels_default:
                edit_channels_default = ["Inbound RingCX"]

            edit_actions_default = [x for x in _split_multi(row.get("action_type")) if x in IO_TASK_ACTIONS]
            if not edit_actions_default:
                edit_actions_default = ["Ticket Updated/Notes"]

            outcome_default = row.get("result_type") if row.get("result_type") in IO_OUTCOMES else IO_OUTCOMES[0]
            agent_default = row.get("logging_agent") if row.get("logging_agent") in OWNERS else OWNERS[0]

            st.session_state["io_edit_date"] = edit_date_default.date()
            st.session_state["io_edit_channels"] = edit_channels_default
            st.session_state["io_edit_actions"] = edit_actions_default
            st.session_state["io_edit_outcome"] = outcome_default
            st.session_state["io_edit_phone"] = str(row.get("phone") or "")
            st.session_state["io_edit_name"] = str(row.get("customer_name") or "")
            st.session_state["io_edit_order"] = str(row.get("order_no") or "")
            st.session_state["io_edit_agent"] = agent_default
            st.session_state["io_edit_ticket_name"] = str(row.get("ticket_update_name") or "")
            st.session_state["io_edit_email"] = str(row.get("email") or "")
            st.session_state["io_edit_notes"] = str(row.get("notes") or "")
            st.session_state["io_edit_confirm_delete"] = False
            st.session_state["io_edit_loaded_id"] = selected_edit_id
            st.rerun()

        with st.form("io_edit_form"):
            e1, e2, e3, e4 = st.columns(4)
            e_date = e1.date_input("Date", key="io_edit_date")
            e_channels = e2.multiselect("Inbound/Outbound", IO_CHANNELS, key="io_edit_channels")
            e_actions = e3.multiselect("Task or action done", IO_TASK_ACTIONS, key="io_edit_actions")
            e_outcome = e4.selectbox("Outcome", IO_OUTCOMES, key="io_edit_outcome")

            f1, f2, f3, f4 = st.columns(4)
            e_phone = f1.text_input("Phone Number", key="io_edit_phone")
            e_name = f2.text_input("Name", key="io_edit_name")
            e_order = f3.text_input("Order #", key="io_edit_order")
            e_agent = f4.selectbox("Agent", OWNERS, key="io_edit_agent")

            g1, g2 = st.columns(2)
            e_title = g1.text_input("Ticket Name", key="io_edit_ticket_name")
            e_email = g2.text_input("Email", key="io_edit_email")
            e_notes = st.text_area("Notes", height=140, key="io_edit_notes")

            confirm_delete = st.checkbox("Confirm delete this entry", key="io_edit_confirm_delete")
            h1, h2 = st.columns(2)
            edit_save = h1.form_submit_button("Save Entry Changes")
            edit_delete = h2.form_submit_button("Delete Entry")

        if edit_save:
            changed = update_inbound_outbound_entry(
                selected_edit_id,
                {
                    "activity_date": e_date.isoformat(),
                    "io_channel": e_channels,
                    "action_type": e_actions,
                    "result_type": e_outcome,
                    "phone": e_phone,
                    "customer_name": e_name,
                    "order_no": e_order,
                    "ticket_update_name": e_title,
                    "email": e_email,
                    "notes": e_notes,
                    "logging_agent": e_agent,
                    "assigned_owner": e_agent,
                },
            )
            if changed:
                st.success("Inbound-Outbound entry updated.")
            else:
                st.warning("No changes were saved.")
            st.rerun()

        if edit_delete:
            if not confirm_delete:
                st.warning("Please check confirm delete before deleting.")
            else:
                deleted = delete_activity_by_id(selected_edit_id)
                if deleted:
                    st.success("Inbound-Outbound entry deleted.")
                else:
                    st.warning("Could not delete entry.")
                st.rerun()

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

    st.caption("Assistant-style: paste your full ChatGPT/LACB note block and use Assistant-Style Generate for one-click draft creation.")
    q1, q2, q3 = st.columns(3)
    if q1.button("Assistant-Style Auto Fill Form", key="builder_chat_autofill"):
        parsed = parse_chat_style_ticket(st.session_state.get("builder_raw_details", ""))
        pref = {
            "customer_name": parsed.get("customer_name", ""),
            "order_no": parsed.get("order_no", ""),
            "phone": parsed.get("phone", ""),
            "issue_type": parsed.get("issue_type", ""),
            "request_type": parsed.get("request_type", ""),
            "ticket_source": parsed.get("ticket_source", ""),
            "assigned_by": parsed.get("assigned_by", ""),
            "assigned_agent": parsed.get("assigned_agent", ""),
            "hubspot_stage": parsed.get("hubspot_stage", ""),
            "internal_status": parsed.get("internal_status", ""),
        }
        st.session_state["builder_pending_autofill"] = pref
        st.rerun()

    if q2.button("Assistant-Style Generate", key="builder_chat_generate"):
        do_generate_chat_style_v9(save_to_tracker=False)

    if q3.button("Assistant-Style Generate + Add to Tracker", key="builder_chat_generate_save"):
        do_generate_chat_style_v9(save_to_tracker=True)

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

                if route == "Direct Scheduling":
                    st.caption(f"Suggested target date: {add_business_days(date.today(), 14).isoformat()}")

                if installers:
                    out = pd.DataFrame(
                        [
                            {
                                "Priority Order": i + 1,
                                "Installer": item["installer"],
                                "Tier": item["priority"],
                            }
                            for i, item in enumerate(installers)
                        ]
                    )
                    st.dataframe(out, use_container_width=True, hide_index=True)
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
    df_activities = pd.read_sql_query(
        """
        SELECT
            a.id,
            a.ticket_id,
            t.cc_id,
            COALESCE(a.ticket_update_name, t.ticket_title) AS ticket_title,
            COALESCE(NULLIF(a.customer_name, ''), t.customer_name) AS customer_name,
            COALESCE(NULLIF(a.order_no, ''), t.order_no) AS order_no,
            a.activity_date,
            a.logging_agent,
            a.assigned_owner,
            a.io_channel,
            a.action_type,
            a.result_type,
            a.phone,
            a.email,
            a.notes,
            a.created_at,
            a.ticket_update_name
        FROM activities a
        LEFT JOIN tickets t ON t.id = a.ticket_id
        ORDER BY a.id DESC
        """,
        conn,
    )
    conn.close()

    df_tickets = get_all_tickets_df()

    tab_io, tab_activity, tab_tickets = st.tabs([
        "Inbound-Outbound Export",
        "Activity History",
        "Ticket History",
    ])

    with tab_io:
        st.subheader("Inbound-Outbound Activity Log")

        io_df = df_activities.copy()

        for col in [
            "io_channel", "action_type", "result_type", "phone", "customer_name", "order_no",
            "ticket_update_name", "email", "notes", "logging_agent", "activity_date", "created_at"
        ]:
            if col not in io_df.columns:
                io_df[col] = ""

        io_df["activity_dt"] = pd.to_datetime(io_df["activity_date"], errors="coerce")
        missing_mask = io_df["activity_dt"].isna()
        if missing_mask.any():
            io_df.loc[missing_mask, "activity_dt"] = pd.to_datetime(io_df.loc[missing_mask, "created_at"], errors="coerce")
        io_df["activity_day"] = io_df["activity_dt"].dt.date

        c1, c2, c3 = st.columns(3)
        date_mode = c1.selectbox(
            "Date Filter",
            ["All Dates", "Single Date", "Date Range"],
            key="history_io_date_mode",
        )
        owner_filter = c2.selectbox("Owner Filter", ["All"] + OWNERS, key="history_io_owner_filter")
        io_search = c3.text_input("Search", key="history_io_search", placeholder="name, order, ticket, notes, phone")

        if date_mode == "Single Date":
            target_date = st.date_input("Activity Date", value=date.today(), key="history_io_single_date")
            io_df = io_df[io_df["activity_day"] == target_date]
        elif date_mode == "Date Range":
            r1, r2 = st.columns(2)
            start_date = r1.date_input("Start Date", value=date.today() - timedelta(days=7), key="history_io_start_date")
            end_date = r2.date_input("End Date", value=date.today(), key="history_io_end_date")
            if end_date < start_date:
                st.warning("End date cannot be before start date.")
            else:
                io_df = io_df[
                    (io_df["activity_day"] >= start_date)
                    & (io_df["activity_day"] <= end_date)
                ]

        if owner_filter != "All" and not io_df.empty:
            io_df = io_df[
                (io_df["logging_agent"] == owner_filter)
                | (io_df["assigned_owner"] == owner_filter)
            ]

        if io_search and not io_df.empty:
            s = io_search.lower()
            io_df = io_df[
                io_df["customer_name"].fillna("").astype(str).str.lower().str.contains(s)
                | io_df["order_no"].fillna("").astype(str).str.lower().str.contains(s)
                | io_df["ticket_title"].fillna("").astype(str).str.lower().str.contains(s)
                | io_df["ticket_update_name"].fillna("").astype(str).str.lower().str.contains(s)
                | io_df["notes"].fillna("").astype(str).str.lower().str.contains(s)
                | io_df["phone"].fillna("").astype(str).str.lower().str.contains(s)
                | io_df["email"].fillna("").astype(str).str.lower().str.contains(s)
            ]

        io_export = pd.DataFrame({
            "Date": io_df["activity_day"].astype(str).replace("NaT", ""),
            "Inbound/Outbound": io_df["io_channel"].fillna(""),
            "Task or action done": io_df["action_type"].fillna(""),
            "Phone Number": io_df["phone"].fillna(""),
            "Name": io_df["customer_name"].fillna(""),
            "Order #": io_df["order_no"].fillna(""),
            "Ticket/Update name": io_df["ticket_update_name"].fillna(io_df["ticket_title"]).fillna(""),
            "Email": io_df["email"].fillna(""),
            "Notes (Optional)": io_df["notes"].fillna(""),
            "Outcome": io_df["result_type"].fillna(""),
            "Agent": io_df["logging_agent"].fillna(""),
        })

        st.dataframe(io_export, use_container_width=True, hide_index=True)

        b1, b2 = st.columns(2)
        with b1:
            st.download_button(
                "Download Inbound-Outbound CSV",
                data=io_export.to_csv(index=False).encode("utf-8"),
                file_name="LACB_Inbound_Outbound_Activities_Export.csv",
                mime="text/csv",
                key="download_io_csv",
            )
        with b2:
            xbuf = io.BytesIO()
            with pd.ExcelWriter(xbuf, engine="openpyxl") as writer:
                io_export.to_excel(writer, index=False, sheet_name="Inbound-outbound-Activities")
            st.download_button(
                "Download Inbound-Outbound Excel (Team Template Name)",
                data=xbuf.getvalue(),
                file_name="LACB_Inbound_Outbound_Activities_Team_Clean_v13_Scheduling_Typo_Fixed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_io_xlsx",
            )

    with tab_activity:
        st.subheader("Activity History")
        owner_filter = st.selectbox("Owner Filter", ["All"] + OWNERS, key="history_owner_filter")
        history_search = st.text_input("History Search", key="history_search")

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
                | filtered_activities["io_channel"].fillna("").str.lower().str.contains(s)
                | filtered_activities["action_type"].fillna("").str.lower().str.contains(s)
            )
            filtered_activities = filtered_activities[mask]

        st.dataframe(filtered_activities, use_container_width=True)

        st.download_button(
            "Download Activities CSV",
            data=filtered_activities.to_csv(index=False).encode("utf-8"),
            file_name="lacb_activity_history.csv",
            mime="text/csv",
            key="download_activities_csv",
        )

    with tab_tickets:
        st.subheader("Ticket History")

        owner_filter_t = st.selectbox("Owner Filter", ["All"] + OWNERS, key="history_owner_filter_tickets")
        history_search_t = st.text_input("Ticket Search", key="history_search_tickets")

        filtered_tickets = df_tickets.copy()
        if owner_filter_t != "All" and not filtered_tickets.empty:
            filtered_tickets = filtered_tickets[filtered_tickets["assigned_agent"] == owner_filter_t]

        if history_search_t and not filtered_tickets.empty:
            s = history_search_t.lower()
            mask = (
                filtered_tickets["ticket_title"].fillna("").str.lower().str.contains(s)
                | filtered_tickets["customer_name"].fillna("").str.lower().str.contains(s)
                | filtered_tickets["order_no"].fillna("").str.lower().str.contains(s)
                | filtered_tickets["notes_summary"].fillna("").str.lower().str.contains(s)
            )
            filtered_tickets = filtered_tickets[mask]

        st.dataframe(filtered_tickets, use_container_width=True)

        st.download_button(
            "Download Tickets CSV",
            data=filtered_tickets.to_csv(index=False).encode("utf-8"),
            file_name="lacb_ticket_history.csv",
            mime="text/csv",
            key="download_tickets_csv",
        )

# =========================================================
# NAVIGATION
# =========================================================

PAGES = {
    "Follow-Up Assistant": follow_up_assistant_page,
    "KPI Dashboard": kpi_dashboard_page,
    "Ticket Tracker": ticket_tracker_page,
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




























































