import io
import os
import re
import sqlite3
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

DB_PATH = "lacc_command_center.db"
REGION_MAP_CANDIDATES = ["Region_Map.csv", "region_map.csv"]
SCHEDULER_CANDIDATES = ["Scheduling Tool_2_20.xlsx", "Scheduling Tool.xlsx"]

AGENTS = ["Ed Torres", "Erika Sagasta"]
TICKET_SOURCES = [
    "Receptionist", "Supervisor", "Sales Team", "Self-Created",
    "Direct SMS", "Direct Call", "Email", "Internal Chat", "DC Request"
]
ASSIGNED_BY = ["Reception Desk", "Melisa", "Salma", "Ervin", "Self", "Other"]
HUBSPOT_STAGES = ["New", "Waiting on customer", "Waiting on vendor", "Waiting on us", "Closed"]
INTERNAL_STATUSES = [
    "Waiting on Customer", "Waiting on Vendor", "Waiting on DC",
    "Waiting on Internal Dept", "Scheduled", "Ready to Close", "Closed"
]
ISSUE_TYPES = [
    "Service",
    "Order Status/ETA Update",
    "PC Intro",
    "Repair Assessment",
    "Motor/Remote Troubleshooting",
    "Shipping",
    "Vendor",
    "Install",
    "DC Request",
    "Payment",
    "Warranty",
    "Other",
]
REQUEST_TYPES = [
    "Inbound Call",
    "Outbound Call",
    "SMS Sent",
    "Email Sent",
    "Service Order Created",
    "Order Status Update",
    "Vendor Follow-Up",
    "Scheduling Update",
    "Payment Follow-Up",
    "Repair Assessment",
    "PC Intro",
    "DC Request",
    "Internal Department Follow-Up",
    "Other",
]
ACTIONS = [
    "Outbound Call", "Inbound Call", "SMS Sent", "Email Sent", "Internal Chat",
    "HubSpot Note Added", "QuoteRite Note Added", "Service Order Created",
    "Appointment Scheduled", "Scheduling Request Sent", "Vendor Follow-Up",
    "DC Follow-Up", "Internal Dept Follow-Up", "Ticket Closed"
]
RESULTS = [
    "Spoke with customer", "No answer", "Left voicemail", "Waiting on customer",
    "Waiting on vendor", "Waiting on DC", "Waiting on internal department", "Resolved"
]
SCHEDULING_FLAGS = [
    "Palm Springs", "Shutters", "Arizona", "2-Man Job",
    "Ladder >10 ft", "Ladder >20 ft", "Scaffolding"
]

REPAIR_PRESETS = {
    "Motor Issue": {
        "repair_needed": "Assess the motor for repair/replacement. Assess and try to fix on the spot. If not, determine the next steps needed.",
        "why": "Customer reported the motor is not working properly and the shade requires troubleshooting to determine whether programming, charging, repair, or replacement is needed."
    },
    "Fabric Issue": {
        "repair_needed": "Assess the fabric for repair/replacement. Installer must determine the nature of the issue before approval of a warranty replacement.",
        "why": "Customer reported fabric-related damage such as fraying, holes, or wear and the shade must be assessed to determine cause and next steps."
    },
    "Guidewire Issue": {
        "repair_needed": "Assess the guidewires and related hardware for repair/replacement. Fix on the spot if possible or advise the next steps needed.",
        "why": "Customer reported an issue with the guidewires affecting operation, alignment, or tension of the shade system."
    },
    "Anchor Issue": {
        "repair_needed": "Assess the anchors and mounting hardware for repair/replacement. Fix on the spot if possible or advise the next steps needed.",
        "why": "Customer reported an issue with the anchors or mounting points for the shade system and an assessment is needed to determine the proper correction."
    },
    "Programming / Demonstrate Programming Steps": {
        "repair_needed": "Program the shade and demonstrate the programming steps to the customer on site. Confirm proper operation of the motor, remote, and any accessories. If programming is not successful, assess and advise the next steps needed.",
        "why": "Customer requires assistance with programming and needs a demonstration of the correct programming steps to ensure proper operation after service is completed."
    },
    "Charger Reconnection": {
        "repair_needed": "Reconnect the charger and assess the motor for proper charging and response. Attempt to fix on the spot and advise the next steps if programming or replacement is needed.",
        "why": "Customer reported the charger became disconnected or the shade is not responding and the motor must be assessed after reconnecting and charging."
    },
    "Remote / Programming Issue": {
        "repair_needed": "Assess the remote and programming for repair/troubleshooting. Attempt to reprogram on site and verify motor response. If unsuccessful, advise the next steps needed.",
        "why": "Customer reported the shade is not responding to the remote and troubleshooting is needed to determine whether the issue is programming related or requires replacement parts."
    },
    "Other": {"repair_needed": "", "why": ""},
}

REGION_ALIASES = {
    "LA": ["LA", "LOS ANGELES"],
    "OC": ["OC", "ORANGE COUNTY"],
    "IE": ["IE", "RIVERSIDE", "IE/RIVERSIDE"],
    "SD": ["SD", "SAN DIEGO"],
    "PS": ["PS", "PALM SPRINGS"],
    "ARIZONA": ["AZ", "ARIZONA"],
    "LAS VEGAS": ["LAS VEGAS", "VEGAS", "LV"],
    "VALLEY": ["VALLEY"],
    "VENTURA": ["VENTURA", "VENTURA CO"],
}

# =========================
# GENERATOR HELPERS
# =========================

def generate_service_repair_template(original_order="", repair_needed="", why="", photos_videos="No"):
    original_order = (original_order or "N/A").strip()
    repair_needed = (repair_needed or "N/A").strip()
    why = (why or "N/A").strip()
    photos_videos = (photos_videos or "No").strip()

    return f"""Service/Repair order template:

Original Order#: {original_order}

Repair Needed:
{repair_needed}

Why:
{why}

Photos/videos: {photos_videos}"""


def generate_scheduling_request(order_no="", customer="", phone="", installer_region="", availability="", special_notes=""):
    order_no = (order_no or "N/A").strip()
    customer = (customer or "N/A").strip()
    phone = (phone or "N/A").strip()
    installer_region = (installer_region or "N/A").strip()
    availability = (availability or "Next Available").strip()
    special_notes = (special_notes or "N/A").strip()

    return f"""SCHEDULING REQUEST:
🔢 Active Order Number: {order_no}
👤 Customer Name: {customer}
📞☎️ Phone: {phone}
🚚 Installer/Region: {installer_region}
📅 Requested Date & Time / Availability: {availability}
📝 Any special notes: {special_notes}"""

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_iso():
    return date.today().isoformat()


def add_business_days(start_dt, business_days):
    current = start_dt
    added = 0
    while added < business_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def next_customer_followup(attempt_no):
    return add_business_days(date.today(), 2 if attempt_no >= 4 else 1).isoformat()


def recommended_next_action(status, attempt_no):
    if status == "Waiting on Customer":
        if attempt_no <= 2:
            return "Call + SMS"
        if attempt_no == 3:
            return "Call + SMS + Email"
        return "Call + SMS + Final Email / Review closure"
    if status == "Waiting on Vendor":
        return "Vendor follow-up"
    if status == "Waiting on DC":
        return "DC follow-up"
    if status == "Waiting on Internal Dept":
        return "Internal dept follow-up"
    if status == "Ready to Close":
        return "Review and close"
    if status == "Scheduled":
        return "Monitor appointment"
    return "Review ticket"


def compute_scheduling_route(flags):
    flags = set(flags or [])
    if "Palm Springs" in flags:
        return "Moses Email", "Palm Springs"
    if flags:
        return "Chat Copy/Paste", ", ".join(sorted(flags))
    return "Direct Scheduling", "Standard Service"


def normalize_text(val):
    return re.sub(r"\s+", " ", str(val or "")).strip()


def extract_order_no(text):
    m = re.search(r"\b[OQ]-\d+\b", text or "", flags=re.I)
    return m.group(0).upper() if m else ""


def extract_phone(text):
    m = re.search(r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", text or "")
    return m.group(1) if m else ""


def extract_customer_name(text):
    text = text or ""
    m = re.search(r"CUSTOMER\s*:?\s*([A-Z][A-Za-z\-\.' ]+)", text, flags=re.I)
    return normalize_text(m.group(1)) if m else ""


def detect_issue_type(text):
    t = (text or "").lower()
    if any(x in t for x in ["eta", "order status", "status update", "in production", "ready date", "lead time"]):
        return "Order Status/ETA Update"
    if any(x in t for x in ["pc intro", "project coordinator", "i'll be overseeing your order", "i am the project coordinator"]):
        return "PC Intro"
    if any(x in t for x in ["remote", "motor", "charger", "program", "not responding"]):
        return "Motor/Remote Troubleshooting"
    if any(x in t for x in ["assess", "assessment", "service call", "repair needed"]):
        return "Repair Assessment"
    if any(x in t for x in ["payment", "quote", "invoice"]):
        return "Payment"
    if any(x in t for x in ["install", "installer", "appointment", "schedule"]):
        return "Install"
    if any(x in t for x in ["ship", "tracking", "vendor", "warehouse"]):
        return "Shipping"
    if "warranty" in t:
        return "Warranty"
    return "Service"


def detect_request_type(text):
    t = (text or "").lower()
    if "service order" in t:
        return "Service Order Created"
    if any(x in t for x in ["eta", "order status", "status update"]):
        return "Order Status Update"
    if any(x in t for x in ["vendor", "warehouse", "tracking"]):
        return "Vendor Follow-Up"
    if any(x in t for x in ["appointment", "schedule", "installer"]):
        return "Scheduling Update"
    if any(x in t for x in ["payment", "quote", "invoice"]):
        return "Payment Follow-Up"
    if any(x in t for x in ["call transcript", "inbound call", "called in"]):
        return "Inbound Call"
    if any(x in t for x in ["sms", "texted"]):
        return "SMS Sent"
    if "email" in t:
        return "Email Sent"
    return "Other"


def request_type_to_source(request_type):
    mapping = {
        "Inbound Call": "Direct Call",
        "Outbound Call": "Direct Call",
        "SMS Sent": "Direct SMS",
        "Email Sent": "Email",
        "Service Order Created": "Self-Created",
        "Order Status Update": "Self-Created",
        "Vendor Follow-Up": "Internal Chat",
        "Scheduling Update": "Internal Chat",
        "Payment Follow-Up": "Email",
        "Repair Assessment": "Self-Created",
        "PC Intro": "Self-Created",
        "DC Request": "Internal Chat",
        "Internal Department Follow-Up": "Internal Chat",
        "Other": "Self-Created",
    }
    return mapping.get(request_type, "Self-Created")


def default_internal_status(request_type):
    mapping = {
        "Inbound Call": "Waiting on Customer",
        "Outbound Call": "Waiting on Customer",
        "SMS Sent": "Waiting on Customer",
        "Email Sent": "Waiting on Customer",
        "Service Order Created": "Waiting on Customer",
        "Order Status Update": "Waiting on Vendor",
        "Vendor Follow-Up": "Waiting on Vendor",
        "Scheduling Update": "Scheduled",
        "Payment Follow-Up": "Waiting on Customer",
        "Repair Assessment": "Waiting on Customer",
        "PC Intro": "Waiting on Customer",
        "DC Request": "Waiting on DC",
        "Internal Department Follow-Up": "Waiting on Internal Dept",
        "Other": "Waiting on Customer",
    }
    return mapping.get(request_type, "Waiting on Customer")


def generate_ticket_title(source_label, issue_type, order_no, customer_name):
    source_map = {
        "Direct Call": "Inbound Call",
        "Receptionist": "Inbound Call",
        "Email": "Email Sent",
        "Direct SMS": "SMS Sent",
        "Internal Chat": "Internal Chat",
        "Sales Team": "Inbound Call",
        "Supervisor": "Inbound Call",
        "Self-Created": "Inbound Call",
    }
    prefix = source_map.get(source_label, "Inbound Call")
    issue = issue_type or "Service Request"
    order_part = order_no or "Order TBD"
    name_part = customer_name or "Customer"
    return f"{prefix} – {issue} – {order_part} – {name_part}"


def extract_prefill(raw_text, default_source="Direct Call"):
    order_no = extract_order_no(raw_text)
    phone = extract_phone(raw_text)
    customer = extract_customer_name(raw_text)
    issue_type = detect_issue_type(raw_text)
    title = generate_ticket_title(default_source, issue_type, order_no, customer)
    return {
        "order_no": order_no,
        "phone": phone,
        "customer_name": customer,
        "issue_type": issue_type,
        "hubspot_title": title,
        "request_type": detect_request_type(raw_text),
    }


def first_sentence(text):
    txt = normalize_text(text)
    if not txt:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", txt)
    return parts[0]


def clean_internal_text(text):
    txt = str(text or "")
    txt = re.sub(r"(?i)crm note\s*/\s*ticket update\s*[–-].*", "", txt)
    txt = re.sub(r"(?i)hubspot ticket\s*:.*", "", txt)
    txt = txt.replace("PROBLEM:", "").replace("RESOLUTION:", "").replace("EXPECTATION:", "")
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt


def build_internal_outputs(request_type, customer_name, order_no, raw_details, issue_type, existing_notes=""):
    details = clean_internal_text(raw_details)
    summary_source = clean_internal_text(existing_notes) if existing_notes else details

    problem = f"{customer_name or 'Customer'} contacted me regarding order {order_no or 'N/A'} for {issue_type or 'service'}."
    resolution = summary_source or "I documented the latest activity and provided the appropriate update."
    expectation = f"I will continue monitoring and follow up on the next required step for order {order_no or 'N/A'}."

    if request_type == "Service Order Created":
        problem = f"Service order was created for {customer_name or 'customer'} under order {order_no or 'N/A'}."
        expectation = "I will contact the customer with the next available appointment and continue monitoring until service is completed."
    elif request_type == "Order Status Update":
        problem = f"{customer_name or 'Customer'} requested an order status / ETA update for order {order_no or 'N/A'}."
        expectation = "I will continue monitoring vendor / production updates and provide the customer with the next available status update."
    elif request_type == "Payment Follow-Up":
        problem = f"{customer_name or 'Customer'} needed guidance regarding payment / quote steps for order {order_no or 'N/A'}."
        expectation = "Awaiting customer payment confirmation. Once payment is received, I will proceed with the next scheduling step."
    elif request_type == "Scheduling Update":
        problem = f"Scheduling update was needed for {customer_name or 'customer'} under order {order_no or 'N/A'}."
        expectation = "I will continue monitoring the appointment and provide any additional update needed."
    elif request_type == "Vendor Follow-Up":
        problem = f"Vendor follow-up was needed for order {order_no or 'N/A'} for {customer_name or 'customer'}."
        expectation = "Awaiting vendor response. I will provide the next update once received."
    elif request_type == "PC Intro":
        problem = f"Project Coordinator introduction was needed for {customer_name or 'customer'} under order {order_no or 'N/A'}."
        expectation = "I will continue overseeing the order and provide updates as needed."

    hubspot_summary = f"""PROBLEM:
{problem}

RESOLUTION:
{resolution}

EXPECTATION:
{expectation}"""

    crm_note = f"""PROBLEM: {problem}

RESOLUTION: {resolution}

EXPECTATION: {expectation}"""
    return hubspot_summary, crm_note, problem, resolution, expectation


def make_customer_sms(customer_name, order_no, request_type, expectation, resolution):
    short_res = first_sentence(clean_internal_text(resolution))
    short_exp = first_sentence(clean_internal_text(expectation))
    prefix = f"Hi {customer_name or 'there'}, this is Ed from LA Custom Blinds."
    if request_type == "Payment Follow-Up":
        body = f"I wanted to provide you with an update regarding order {order_no or 'N/A'}. {short_res} {short_exp}"
    elif request_type == "Scheduling Update":
        body = f"I wanted to provide you with a scheduling update regarding order {order_no or 'N/A'}. {short_res} {short_exp}"
    else:
        body = f"I wanted to provide you with an update regarding order {order_no or 'N/A'}. {short_res} {short_exp}"
    return f"{prefix} {body} You can text me here or call 1-800-533-7957 ext. 1111."


def make_customer_email(customer_name, order_no, resolution, expectation):
    res = clean_internal_text(resolution)
    exp = clean_internal_text(expectation)
    return f"""Subject: LACB | Update | {order_no or 'Order'} | {customer_name or 'Customer'}

Hi {customer_name or 'there'},

I wanted to provide you with an update regarding your order ({order_no or 'N/A'}).

{res}

{exp}

If you have any questions, you can reply to this email or call me directly at 1-800-533-7957 ext. 1111.

Thank you,
Ed Torres
Project Coordinator
LA Custom Blinds"""


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cc_id TEXT UNIQUE,
            hubspot_title TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            order_no TEXT,
            phone TEXT,
            ticket_source TEXT,
            assigned_by TEXT,
            assigned_agent TEXT,
            issue_type TEXT,
            hubspot_stage TEXT,
            internal_status TEXT,
            attempt_no INTEGER DEFAULT 0,
            last_activity_date TEXT,
            next_followup_date TEXT,
            special_scheduling_required INTEGER DEFAULT 0,
            scheduling_route TEXT,
            scheduling_reason TEXT,
            notes_summary TEXT,
            raw_conversation TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS activities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_cc_id TEXT NOT NULL,
            hubspot_title TEXT,
            customer_name TEXT,
            order_no TEXT,
            agent TEXT,
            actions TEXT,
            result TEXT,
            notes_summary TEXT,
            activity_ts TEXT,
            activity_date TEXT,
            attempt_no INTEGER,
            next_followup_date TEXT
        )
        """
    )
    conn.commit()
    cur.execute("PRAGMA table_info(tickets)")
    cols = [r["name"] for r in cur.fetchall()]
    if "raw_conversation" not in cols:
        cur.execute("ALTER TABLE tickets ADD COLUMN raw_conversation TEXT")
        conn.commit()
    conn.close()


def next_cc_id(conn):
    cur = conn.cursor()
    cur.execute("SELECT cc_id FROM tickets ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    if not row or not row["cc_id"]:
        return "CC-0001"
    return f"CC-{int(row['cc_id'].split('-')[1]) + 1:04d}"


def fetch_tickets(where="", params=()):
    conn = get_conn()
    q = "SELECT * FROM tickets"
    if where:
        q += " " + where
    q += " ORDER BY updated_at DESC"
    df = pd.read_sql_query(q, conn, params=params)
    conn.close()
    return df


def fetch_activities(where="", params=()):
    conn = get_conn()
    q = "SELECT * FROM activities"
    if where:
        q += " " + where
    q += " ORDER BY activity_ts DESC"
    df = pd.read_sql_query(q, conn, params=params)
    conn.close()
    return df


def ticket_exists(order_no, hubspot_title, exclude_cc_id=None):
    conn = get_conn()
    cur = conn.cursor()
    if exclude_cc_id:
        cur.execute("SELECT cc_id FROM tickets WHERE order_no = ? AND hubspot_title = ? AND cc_id <> ? LIMIT 1", (order_no, hubspot_title, exclude_cc_id))
    else:
        cur.execute("SELECT cc_id FROM tickets WHERE order_no = ? AND hubspot_title = ? LIMIT 1", (order_no, hubspot_title))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def save_ticket(data):
    conn = get_conn()
    cur = conn.cursor()
    if data.get("cc_id"):
        cur.execute(
            """
            UPDATE tickets SET
                hubspot_title=?, customer_name=?, order_no=?, phone=?, ticket_source=?, assigned_by=?, assigned_agent=?,
                issue_type=?, hubspot_stage=?, internal_status=?, attempt_no=?, last_activity_date=?, next_followup_date=?,
                special_scheduling_required=?, scheduling_route=?, scheduling_reason=?, notes_summary=?, raw_conversation=?, updated_at=?
            WHERE cc_id=?
            """,
            (
                data["hubspot_title"], data["customer_name"], data["order_no"], data["phone"], data["ticket_source"],
                data["assigned_by"], data["assigned_agent"], data["issue_type"], data["hubspot_stage"], data["internal_status"],
                int(data["attempt_no"]), data["last_activity_date"], data["next_followup_date"], int(bool(data["special_scheduling_required"])),
                data["scheduling_route"], data["scheduling_reason"], data["notes_summary"], data.get("raw_conversation", ""), now_iso(), data["cc_id"]
            ),
        )
        cc_id = data["cc_id"]
    else:
        cc_id = next_cc_id(conn)
        cur.execute(
            """
            INSERT INTO tickets (
                cc_id, hubspot_title, customer_name, order_no, phone, ticket_source, assigned_by, assigned_agent,
                issue_type, hubspot_stage, internal_status, attempt_no, last_activity_date, next_followup_date,
                special_scheduling_required, scheduling_route, scheduling_reason, notes_summary, raw_conversation, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cc_id, data["hubspot_title"], data["customer_name"], data["order_no"], data["phone"], data["ticket_source"],
                data["assigned_by"], data["assigned_agent"], data["issue_type"], data["hubspot_stage"], data["internal_status"],
                int(data["attempt_no"]), data["last_activity_date"], data["next_followup_date"], int(bool(data["special_scheduling_required"])),
                data["scheduling_route"], data["scheduling_reason"], data["notes_summary"], data.get("raw_conversation", ""), now_iso(), now_iso()
            ),
        )
    conn.commit()
    conn.close()
    return cc_id


def delete_ticket(cc_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM activities WHERE ticket_cc_id = ?", (cc_id,))
    cur.execute("DELETE FROM tickets WHERE cc_id = ?", (cc_id,))
    conn.commit()
    conn.close()


def save_activity(ticket, logging_agent, actions, result, notes_summary, assigned_owner=None):
    actions = actions or []
    today = today_iso()
    attempt_no = int(ticket["attempt_no"] or 0)
    current_status = ticket["internal_status"]
    hubspot_stage = ticket["hubspot_stage"]
    new_status = current_status
    next_followup_date = ticket["next_followup_date"] or add_business_days(date.today(), 1).isoformat()

    if current_status == "Waiting on Customer" and "Outbound Call" in actions:
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
    if result in result_map:
        new_status = result_map[result]
    if "Appointment Scheduled" in actions:
        new_status = "Scheduled"
    if "Ticket Closed" in actions or result == "Resolved":
        new_status = "Closed"
        hubspot_stage = "Closed"
        next_followup_date = ""
    if new_status in {"Waiting on Vendor", "Waiting on DC", "Waiting on Internal Dept"}:
        next_followup_date = add_business_days(date.today(), 1).isoformat()

    owner_to_save = assigned_owner or ticket.get("assigned_agent") or logging_agent

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO activities (
            ticket_cc_id, hubspot_title, customer_name, order_no, agent, actions, result,
            notes_summary, activity_ts, activity_date, attempt_no, next_followup_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticket["cc_id"], ticket["hubspot_title"], ticket["customer_name"], ticket["order_no"], logging_agent,
            ", ".join(actions), result, notes_summary, now_iso(), today, attempt_no, next_followup_date
        ),
    )
    cur.execute(
        """
        UPDATE tickets SET
            assigned_agent=?, attempt_no=?, last_activity_date=?, next_followup_date=?, internal_status=?,
            hubspot_stage=?, notes_summary=?, updated_at=?
        WHERE cc_id=?
        """,
        (owner_to_save, attempt_no, today, next_followup_date, new_status, hubspot_stage, notes_summary, now_iso(), ticket["cc_id"]),
    )
    conn.commit()
    conn.close()


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
                    df["ZIP_CODE"] = df["ZIP_CODE"].astype(str).str.extract(r"(\d{5})", expand=False).fillna(df["ZIP_CODE"].astype(str))
                if "REGION" in df.columns:
                    df["REGION"] = df["REGION"].astype(str).str.strip()
                return df
            except Exception:
                return pd.DataFrame()
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_scheduler_dashboard():
    for name in SCHEDULER_CANDIDATES:
        if os.path.exists(name):
            try:
                df = pd.read_excel(name, sheet_name="Regional Dashboard", header=1, dtype=str)
                df.columns = [str(c).strip() for c in df.columns]
                return df
            except Exception:
                return pd.DataFrame()
    return pd.DataFrame()


def normalize_region_for_dashboard(region_value):
    val = str(region_value or "").strip().upper()
    for canonical, aliases in REGION_ALIASES.items():
        if val in [a.upper() for a in aliases]:
            return canonical
    return val


def find_dashboard_region_column(df, region_value):
    target = normalize_region_for_dashboard(region_value)
    for col in df.columns:
        u = str(col).upper()
        if target == "LA" and "LA(" in u:
            return col
        if target == "OC" and "OC" in u:
            return col
        if target == "IE" and "IE/RIVERSIDE" in u:
            return col
        if target == "SD" and "SAN DIEGO" in u:
            return col
        if target == "PS" and "PALM SPRINGS" in u:
            return col
        if target == "ARIZONA" and "ARIZONA" in u:
            return col
        if target == "LAS VEGAS" and "LAS VEGAS" in u:
            return col
        if target == "VALLEY" and "VALLEY" in u:
            return col
        if target == "VENTURA" and "VENTURA" in u:
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
        if str(row.get("ROLE", "")).strip().upper() != "INSTALLER":
            continue
        marker = str(row.get(col, "")).strip().upper()
        if marker in {"P1", "P2", "P3"}:
            installers.append({
                "installer": str(row.get("NAME", "")).strip(),
                "priority": marker,
                "row_order": row_order,
            })
    priority_rank = {"P1": 1, "P2": 2, "P3": 3}
    return sorted(installers, key=lambda x: (priority_rank.get(x["priority"], 99), x["row_order"]))


def lookup_region_by_zip(zip_code):
    df = load_region_map()
    if df.empty or "ZIP_CODE" not in df.columns or "REGION" not in df.columns:
        return ""
    zip_code = re.sub(r"\D", "", str(zip_code).strip())[:5]
    match = df[df["ZIP_CODE"].astype(str).str.strip() == zip_code]
    if match.empty:
        return ""
    return str(match.iloc[0]["REGION"]).strip()


def df_download_button(df, file_name, label):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Export", index=False)
    st.download_button(label=label, data=buf.getvalue(), file_name=file_name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def apply_ticket_filters(df, agent_filter, status_filter, search):
    out = df.copy()
    if agent_filter != "All":
        out = out[out["assigned_agent"] == agent_filter]
    if status_filter != "All":
        out = out[out["internal_status"] == status_filter]
    if search:
        s = search.lower()
        mask = (
            out["cc_id"].fillna("").str.lower().str.contains(s) |
            out["hubspot_title"].fillna("").str.lower().str.contains(s) |
            out["customer_name"].fillna("").str.lower().str.contains(s) |
            out["order_no"].fillna("").str.lower().str.contains(s)
        )
        out = out[mask]
    return out


def clear_prefill_state():
    for key in [
        "prefill_hubspot_title", "prefill_customer_name", "prefill_order_no",
        "prefill_phone", "prefill_issue_type", "prefill_ticket_source"
    ]:
        st.session_state.pop(key, None)


def request_ticket_form_reset():
    st.session_state["ticket_form_reset_requested"] = True


def apply_ticket_form_reset():
    defaults = {
        "ticket_edit_select": "Create New",
        "ticket_loaded_ccid": "__new__",
        "ticket_hubspot_title": "",
        "ticket_customer_name": "",
        "ticket_order_no": "",
        "ticket_phone": "",
        "ticket_issue_type": ISSUE_TYPES[0],
        "ticket_ticket_source": TICKET_SOURCES[0],
        "ticket_assigned_by": ASSIGNED_BY[0],
        "ticket_assigned_agent": AGENTS[0],
        "ticket_hubspot_stage": HUBSPOT_STAGES[0],
        "ticket_internal_status": INTERNAL_STATUSES[0],
        "ticket_attempt_no": 0,
        "ticket_last_activity_date": date.today(),
        "ticket_next_followup_date": date.today(),
        "ticket_scheduling_flags": [],
        "ticket_notes_summary": "",
        "ticket_form_reset_requested": False,
    }
    for key, value in defaults.items():
        st.session_state[key] = value


def ensure_ticket_form_state(tickets):
    st.session_state.setdefault("ticket_form_reset_requested", False)
    st.session_state.setdefault("ticket_loaded_ccid", "__new__")
    st.session_state.setdefault("ticket_edit_select", "Create New")

    if st.session_state.get("ticket_form_reset_requested", False):
        apply_ticket_form_reset()
        return

    selected = st.session_state.get("ticket_edit_select", "Create New")
    if selected == "Create New":
        if st.session_state.get("ticket_loaded_ccid") != "__new__":
            apply_ticket_form_reset()
            return
        if "ticket_hubspot_title" not in st.session_state:
            apply_ticket_form_reset()
            return
        if "prefill_hubspot_title" in st.session_state and st.session_state.get("ticket_hubspot_title", "") == "":
            st.session_state["ticket_hubspot_title"] = st.session_state.get("prefill_hubspot_title", "")
            st.session_state["ticket_customer_name"] = st.session_state.get("prefill_customer_name", "")
            st.session_state["ticket_order_no"] = st.session_state.get("prefill_order_no", "")
            st.session_state["ticket_phone"] = st.session_state.get("prefill_phone", "")
            st.session_state["ticket_issue_type"] = st.session_state.get("prefill_issue_type", ISSUE_TYPES[0])
            st.session_state["ticket_ticket_source"] = st.session_state.get("prefill_ticket_source", TICKET_SOURCES[0])
    else:
        if st.session_state.get("ticket_loaded_ccid") != selected:
            row_df = tickets[tickets["cc_id"] == selected]
            if not row_df.empty:
                row = row_df.iloc[0]
                st.session_state["ticket_hubspot_title"] = row["hubspot_title"]
                st.session_state["ticket_customer_name"] = row["customer_name"]
                st.session_state["ticket_order_no"] = row["order_no"] or ""
                st.session_state["ticket_phone"] = row["phone"] or ""
                st.session_state["ticket_issue_type"] = row["issue_type"] if row["issue_type"] in ISSUE_TYPES else ISSUE_TYPES[0]
                st.session_state["ticket_ticket_source"] = row["ticket_source"] if row["ticket_source"] in TICKET_SOURCES else TICKET_SOURCES[0]
                st.session_state["ticket_assigned_by"] = row["assigned_by"] if row["assigned_by"] in ASSIGNED_BY else ASSIGNED_BY[0]
                st.session_state["ticket_assigned_agent"] = row["assigned_agent"] if row["assigned_agent"] in AGENTS else AGENTS[0]
                st.session_state["ticket_hubspot_stage"] = row["hubspot_stage"] if row["hubspot_stage"] in HUBSPOT_STAGES else HUBSPOT_STAGES[0]
                st.session_state["ticket_internal_status"] = row["internal_status"] if row["internal_status"] in INTERNAL_STATUSES else INTERNAL_STATUSES[0]
                st.session_state["ticket_attempt_no"] = int(row["attempt_no"]) if pd.notna(row["attempt_no"]) else 0
                st.session_state["ticket_last_activity_date"] = date.today() if not row["last_activity_date"] else datetime.strptime(row["last_activity_date"], "%Y-%m-%d").date()
                st.session_state["ticket_next_followup_date"] = date.today() if not row["next_followup_date"] else datetime.strptime(row["next_followup_date"], "%Y-%m-%d").date()
                st.session_state["ticket_scheduling_flags"] = [] if not row["scheduling_reason"] else [x.strip() for x in str(row["scheduling_reason"]).split(",") if x.strip() in SCHEDULING_FLAGS]
                st.session_state["ticket_notes_summary"] = row["notes_summary"] or ""
                st.session_state["ticket_loaded_ccid"] = selected


def followup_page(agent_filter):
    st.subheader("Follow-Up Assistant")
    tickets = fetch_tickets("WHERE internal_status != 'Closed'")
    tickets = apply_ticket_filters(tickets, agent_filter, "All", "")
    if tickets.empty:
        st.info("No open tickets found.")
        return
    today = today_iso()
    due = tickets[tickets["next_followup_date"] == today].copy()
    overdue = tickets[(tickets["next_followup_date"].fillna("") < today) & (tickets["next_followup_date"].fillna("") != "")].copy()
    ready = tickets[tickets["internal_status"] == "Ready to Close"].copy()

    def prep(df):
        if df.empty:
            return df
        out = df[["cc_id", "hubspot_title", "customer_name", "order_no", "assigned_agent", "internal_status", "attempt_no", "last_activity_date", "next_followup_date"]].copy()
        out["recommended_next_action"] = out.apply(lambda r: recommended_next_action(r["internal_status"], int(r["attempt_no"] or 0)), axis=1)
        return out

    st.markdown("### Call First / Due Today")
    out = prep(due)
    if not out.empty:
        st.dataframe(out, use_container_width=True, hide_index=True)
    else:
        st.caption("No follow-ups due today.")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Overdue")
        out = prep(overdue)
        if not out.empty:
            st.dataframe(out, use_container_width=True, hide_index=True)
        else:
            st.caption("No overdue follow-ups.")
    with c2:
        st.markdown("### Ready to Close")
        out = prep(ready)
        if not out.empty:
            st.dataframe(out, use_container_width=True, hide_index=True)
        else:
            st.caption("No tickets ready to close.")


def dashboard_page(agent_filter):
    st.subheader("KPI Dashboard")

    st.markdown("### KPI Filters")
    f1, f2, f3, f4 = st.columns([1.2, 1.2, 1.2, 1.2])
    owner_view = f1.selectbox("Owner View", ["Combined", "Ed Torres", "Erika Sagasta"])
    time_view = f2.selectbox("Time View", ["Daily", "Weekly", "Monthly", "Custom Range"])

    today_dt = date.today()
    if time_view == "Daily":
        selected_day = f3.date_input("Date", value=today_dt, key="kpi_daily_date")
        start_date = selected_day
        end_date = selected_day
        f4.caption(f"Showing: {start_date.isoformat()}")
    elif time_view == "Weekly":
        selected_week_day = f3.date_input("Any Date in Week", value=today_dt, key="kpi_week_date")
        start_date = selected_week_day - timedelta(days=selected_week_day.weekday())
        end_date = start_date + timedelta(days=6)
        f4.caption(f"Week: {start_date.isoformat()} to {end_date.isoformat()}")
    elif time_view == "Monthly":
        selected_month_day = f3.date_input("Any Date in Month", value=today_dt, key="kpi_month_date")
        start_date = selected_month_day.replace(day=1)
        if selected_month_day.month == 12:
            next_month = selected_month_day.replace(year=selected_month_day.year + 1, month=1, day=1)
        else:
            next_month = selected_month_day.replace(month=selected_month_day.month + 1, day=1)
        end_date = next_month - timedelta(days=1)
        f4.caption(f"Month: {start_date.strftime('%B %Y')}")
    else:
        start_date = f3.date_input("Start Date", value=today_dt - timedelta(days=7), key="kpi_custom_start")
        end_date = f4.date_input("End Date", value=today_dt, key="kpi_custom_end")
        if end_date < start_date:
            st.warning("End date cannot be before start date.")
            return

    tickets = fetch_tickets()
    acts = fetch_activities()

    if owner_view != "Combined":
        tickets = tickets[tickets["assigned_agent"] == owner_view]
        acts = acts[acts["agent"] == owner_view]

    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()

    tickets_created = tickets[tickets["created_at"].fillna("").str[:10].between(start_iso, end_iso)] if not tickets.empty else tickets
    tickets_updated = tickets[tickets["updated_at"].fillna("").str[:10].between(start_iso, end_iso)] if not tickets.empty else tickets
    acts_period = acts[acts["activity_date"].fillna("").between(start_iso, end_iso)] if not acts.empty else acts

    def count_action(df, label):
        if df.empty:
            return 0
        return int(df["actions"].fillna("").str.contains(label, regex=False).sum())

    tickets_created_count = len(tickets_created)
    quick_logs_count = len(acts_period)
    outbound_calls = count_action(acts_period, "Outbound Call")
    inbound_calls = count_action(acts_period, "Inbound Call")
    sms_sent = count_action(acts_period, "SMS Sent")
    emails_sent = count_action(acts_period, "Email Sent")
    internal_chats = count_action(acts_period, "Internal Chat")
    tickets_closed = count_action(acts_period, "Ticket Closed")
    tickets_scheduled = count_action(acts_period, "Appointment Scheduled")
    ready_to_close = len(tickets_updated[tickets_updated["internal_status"] == "Ready to Close"]) if not tickets_updated.empty else 0

    st.markdown("### KPI Summary")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Tickets Created", tickets_created_count)
    m2.metric("Quick Logs Entered", quick_logs_count)
    m3.metric("Outbound Calls", outbound_calls)
    m4.metric("Inbound Calls", inbound_calls)
    m5.metric("SMS Sent", sms_sent)

    n1, n2, n3, n4, n5 = st.columns(5)
    n1.metric("Emails Sent", emails_sent)
    n2.metric("Internal Chats", internal_chats)
    n3.metric("Tickets Closed", tickets_closed)
    n4.metric("Scheduled", tickets_scheduled)
    n5.metric("Ready to Close", ready_to_close)

    st.markdown("### Filtered Activity Log")
    if acts_period.empty:
        st.caption("No activity logs for the selected filter.")
    else:
        st.dataframe(
            acts_period[["ticket_cc_id", "customer_name", "order_no", "agent", "actions", "result", "activity_date", "attempt_no"]],
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("### Filtered Tickets Created")
    if tickets_created.empty:
        st.caption("No tickets created for the selected filter.")
    else:
        st.dataframe(
            tickets_created[["cc_id", "hubspot_title", "customer_name", "order_no", "assigned_agent", "issue_type", "internal_status", "created_at"]],
            use_container_width=True,
            hide_index=True,
        )


def ticket_tracker_page(agent_filter):
    st.subheader("Ticket Tracker")
    tickets = fetch_tickets()
    ensure_ticket_form_state(tickets)
    tickets = fetch_tickets()

    with st.expander("Create / Update Ticket", expanded=False):
        ticket_options = ["Create New"] + (tickets["cc_id"].tolist() if not tickets.empty else [])
        current_edit = st.session_state.get("ticket_edit_select", "Create New")
        if current_edit not in ticket_options:
            current_edit = "Create New"

        st.selectbox("Edit existing ticket", ticket_options, index=ticket_options.index(current_edit), key="ticket_edit_select")

        editing_ccid = None if st.session_state["ticket_edit_select"] == "Create New" else st.session_state["ticket_edit_select"]
        editing_row = None
        if editing_ccid:
            row_df = tickets[tickets["cc_id"] == editing_ccid]
            if not row_df.empty:
                editing_row = row_df.iloc[0]

        with st.form("ticket_form_v8"):
            a1, a2 = st.columns(2)
            hubspot_title = a1.text_input("HubSpot Ticket Title", key="ticket_hubspot_title")
            customer_name = a2.text_input("Customer Name", key="ticket_customer_name")

            b1, b2, b3 = st.columns(3)
            order_no = b1.text_input("Order #", key="ticket_order_no")
            phone = b2.text_input("Phone", key="ticket_phone")
            issue_type = b3.selectbox("Issue Type", ISSUE_TYPES, key="ticket_issue_type")

            c1, c2, c3 = st.columns(3)
            ticket_source = c1.selectbox("Ticket Source", TICKET_SOURCES, key="ticket_ticket_source")
            assigned_by = c2.selectbox("Assigned By", ASSIGNED_BY, key="ticket_assigned_by")
            assigned_agent = c3.selectbox("Assigned Agent", AGENTS, key="ticket_assigned_agent")

            d1, d2, d3 = st.columns(3)
            hubspot_stage = d1.selectbox("HubSpot Stage", HUBSPOT_STAGES, key="ticket_hubspot_stage")
            internal_status = d2.selectbox("Internal Status", INTERNAL_STATUSES, key="ticket_internal_status")
            attempt_no = d3.number_input("Attempt #", min_value=0, max_value=4, key="ticket_attempt_no")

            e1, e2 = st.columns(2)
            last_activity_date = e1.date_input("Last Activity Date", key="ticket_last_activity_date")
            next_followup_date = e2.date_input("Next Follow-Up Date", key="ticket_next_followup_date")

            scheduling_flags = st.multiselect("Scheduling Flags", SCHEDULING_FLAGS, key="ticket_scheduling_flags")
            scheduling_route, scheduling_reason = compute_scheduling_route(scheduling_flags)
            notes_summary = st.text_area("Notes Summary", key="ticket_notes_summary")

            f1, f2, f3 = st.columns(3)
            submitted = f1.form_submit_button("Save Ticket")
            save_start_new = f2.form_submit_button("Save + Start New")
            clear_form = f3.form_submit_button("Clear Form")

        if clear_form:
            clear_prefill_state()
            request_ticket_form_reset()
            st.rerun()

        if submitted or save_start_new:
            duplicate_cc = ticket_exists(order_no, hubspot_title, exclude_cc_id=editing_ccid)
            if duplicate_cc:
                st.warning(f"Possible duplicate ticket detected: {duplicate_cc}. Save was blocked.")
            else:
                cc_id = save_ticket({
                    "cc_id": editing_ccid,
                    "hubspot_title": hubspot_title,
                    "customer_name": customer_name,
                    "order_no": order_no,
                    "phone": phone,
                    "ticket_source": ticket_source,
                    "assigned_by": assigned_by,
                    "assigned_agent": assigned_agent,
                    "issue_type": issue_type,
                    "hubspot_stage": hubspot_stage,
                    "internal_status": internal_status,
                    "attempt_no": int(attempt_no),
                    "last_activity_date": last_activity_date.isoformat(),
                    "next_followup_date": next_followup_date.isoformat(),
                    "special_scheduling_required": 1 if scheduling_flags else 0,
                    "scheduling_route": scheduling_route,
                    "scheduling_reason": scheduling_reason,
                    "notes_summary": notes_summary,
                    "raw_conversation": "" if editing_row is None else editing_row.get("raw_conversation", ""),
                })
                clear_prefill_state()
                if save_start_new:
                    request_ticket_form_reset()
                st.success(f"Ticket saved: {cc_id}")
                st.rerun()

    st.markdown("### Ticket List")
    c1, c2 = st.columns(2)
    status_filter = c1.selectbox("Filter by Status", ["All"] + INTERNAL_STATUSES)
    search = c2.text_input("Search by CC ID, customer, order, or title")
    filtered = apply_ticket_filters(tickets, agent_filter, status_filter, search)
    if filtered.empty:
        st.info("No matching tickets.")
    else:
        st.dataframe(filtered[["cc_id", "hubspot_title", "customer_name", "order_no", "assigned_agent", "issue_type", "internal_status", "attempt_no", "next_followup_date", "scheduling_route", "updated_at"]], use_container_width=True, hide_index=True)

    st.markdown("### Ticket Cleanup")
    dup_cc = st.selectbox("Select ticket to delete", [""] + (tickets["cc_id"].tolist() if not tickets.empty else []), key="delete_ticket_select")
    if st.button("Delete Selected Ticket"):
        if dup_cc:
            delete_ticket(dup_cc)
            st.success(f"Deleted {dup_cc}")
            st.rerun()
        else:
            st.warning("Select a ticket first.")


def quick_log_page(agent_filter):
    st.subheader("Quick Activity Log")
    tickets = fetch_tickets()
    tickets = tickets[tickets["internal_status"] != "Closed"]
    if tickets.empty:
        st.warning("Create a ticket first.")
        return

    st.session_state.setdefault("ql_actions", [])
    st.session_state.setdefault("ql_result", RESULTS[0])
    st.session_state.setdefault("ql_notes", "")

    search = st.text_input("Search ticket")
    tickets = apply_ticket_filters(tickets, "All", "All", search)
    if tickets.empty:
        st.info("No matching open tickets.")
        return

    options = {f'{r["cc_id"]} | {r["customer_name"]} | {r["order_no"] or ""} | {r["hubspot_title"]}': r["cc_id"] for _, r in tickets.iterrows()}
    selected_label = st.selectbox("Select Ticket", list(options.keys()))
    ticket = tickets[tickets["cc_id"] == options[selected_label]].iloc[0]
    st.caption(f'Current Status: {ticket["internal_status"]} | Attempt #: {int(ticket["attempt_no"] or 0)} | Recommended Next Action: {recommended_next_action(ticket["internal_status"], int(ticket["attempt_no"] or 0))}')

    st.markdown("### Ticket Timeline")
    timeline_df = fetch_activities("WHERE ticket_cc_id = ?", (ticket["cc_id"],))
    if timeline_df.empty:
        st.caption("No activity history yet for this ticket.")
    else:
        st.dataframe(
            timeline_df[["activity_date", "agent", "actions", "result", "notes_summary"]],
            use_container_width=True,
            hide_index=True,
        )

    with st.form("quick_log_form_v8"):
        c1, c2, c3, c4 = st.columns(4)
        customer_name = c1.text_input("Customer Name", value=ticket["customer_name"])
        order_no = c2.text_input("Order #", value=ticket["order_no"] or "")
        default_logging_idx = AGENTS.index(agent_filter) if agent_filter in AGENTS else 0
        logging_agent = c3.selectbox("Logging Agent", AGENTS, index=default_logging_idx)
        assigned_owner = c4.selectbox("Assigned Owner", AGENTS, index=AGENTS.index(ticket["assigned_agent"]) if ticket["assigned_agent"] in AGENTS else 0)

        st.markdown("### Quick Action Buttons")
        qa1, qa2, qa3, qa4 = st.columns(4)
        quick_call = qa1.form_submit_button("📞 Add Call")
        quick_sms = qa2.form_submit_button("📩 Add SMS")
        quick_email = qa3.form_submit_button("📧 Add Email")
        quick_chat = qa4.form_submit_button("💬 Add Chat")

        if quick_call and "Outbound Call" not in st.session_state["ql_actions"]:
            st.session_state["ql_actions"] = st.session_state["ql_actions"] + ["Outbound Call"]
        if quick_sms and "SMS Sent" not in st.session_state["ql_actions"]:
            st.session_state["ql_actions"] = st.session_state["ql_actions"] + ["SMS Sent"]
        if quick_email and "Email Sent" not in st.session_state["ql_actions"]:
            st.session_state["ql_actions"] = st.session_state["ql_actions"] + ["Email Sent"]
        if quick_chat and "Internal Chat" not in st.session_state["ql_actions"]:
            st.session_state["ql_actions"] = st.session_state["ql_actions"] + ["Internal Chat"]

        actions = st.multiselect("Actions Performed", ACTIONS, key="ql_actions")

        st.markdown("### Quick Result Buttons")
        qr1, qr2, qr3, qr4 = st.columns(4)
        if qr1.form_submit_button("✔ Spoke"):
            st.session_state["ql_result"] = "Spoke with customer"
        if qr2.form_submit_button("📭 No Answer"):
            st.session_state["ql_result"] = "No answer"
        if qr3.form_submit_button("📞 Voicemail"):
            st.session_state["ql_result"] = "Left voicemail"
        if qr4.form_submit_button("⏳ Waiting"):
            st.session_state["ql_result"] = "Waiting on customer"

        result = st.selectbox("Result", RESULTS, index=RESULTS.index(st.session_state.get("ql_result", RESULTS[0])), key="ql_result")

        auto_note = ""
        if actions:
            if "Outbound Call" in actions and result == "No answer":
                auto_note = "Attempted outbound call to customer but there was no answer."
            elif "Outbound Call" in actions and result == "Left voicemail":
                auto_note = "Attempted outbound call to customer but there was no answer. Left voicemail requesting a callback."
            elif "SMS Sent" in actions:
                auto_note = "Sent SMS update to the customer and documented the current status."
            elif "Email Sent" in actions:
                auto_note = "Sent email update to the customer and documented the current status."
            elif "Internal Chat" in actions:
                auto_note = "Sent internal chat update and documented the next required step."

        notes_default = st.session_state.get("ql_notes", "") or auto_note
        notes_summary = st.text_area("Notes Summary", value=notes_default, key="ql_notes")

        b1, b2 = st.columns(2)
        save_activity_btn = b1.form_submit_button("Save Activity")
        save_start_new_btn = b2.form_submit_button("Save + Start New")

        if save_activity_btn or save_start_new_btn:
            if not actions:
                st.error("Select at least one action.")
            else:
                ticket_dict = ticket.to_dict()
                ticket_dict["customer_name"] = customer_name
                ticket_dict["order_no"] = order_no
                save_activity(ticket_dict, logging_agent, actions, result, notes_summary, assigned_owner=assigned_owner)
                if save_start_new_btn:
                    st.session_state["ql_actions"] = []
                    st.session_state["ql_result"] = RESULTS[0]
                    st.session_state["ql_notes"] = ""
                    st.success("Activity saved. Ready for next log.")
                else:
                    st.success("Activity saved.")
                st.rerun()

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

    if pending.get("issue_type") in ISSUE_TYPES:
        st.session_state["builder_issue_type"] = pending["issue_type"]

    if pending.get("request_type") in REQUEST_TYPES:
        st.session_state["builder_request_type"] = pending["request_type"]
        st.session_state["builder_internal_status"] = default_internal_status(
            pending["request_type"]
        )
        st.session_state["builder_ticket_source"] = request_type_to_source(
            pending["request_type"]
        )

def hubspot_ticket_builder_page():
    st.subheader("HubSpot Ticket Builder")
    apply_builder_pending_autofill()
    st.caption("Paste your service details, call transcript, SMS interaction, email thread, or internal notes. Generate editable HubSpot / CRM output and optionally auto-fill Ticket Tracker.")

    st.session_state.setdefault("builder_generated", False)
    st.session_state.setdefault("builder_title", "")
    st.session_state.setdefault("builder_hubspot_summary", "")
    st.session_state.setdefault("builder_crm_note", "")
    st.session_state.setdefault("builder_sms", "")
    st.session_state.setdefault("builder_email", "")
    st.session_state.setdefault("builder_request_type", REQUEST_TYPES[0])
    st.session_state.setdefault("builder_customer_name", "")
    st.session_state.setdefault("builder_order_no", "")
    st.session_state.setdefault("builder_phone", "")
    st.session_state.setdefault("builder_ticket_source", request_type_to_source(REQUEST_TYPES[0]))
    st.session_state.setdefault("builder_assigned_by", ASSIGNED_BY[0])
    st.session_state.setdefault("builder_assigned_agent", AGENTS[0])
    st.session_state.setdefault("builder_issue_type", ISSUE_TYPES[0])
    st.session_state.setdefault("builder_hubspot_stage", HUBSPOT_STAGES[0])
    st.session_state.setdefault("builder_internal_status", default_internal_status(REQUEST_TYPES[0]))
    st.session_state.setdefault("builder_attempt_no", 0)
    st.session_state.setdefault("builder_raw_details", "")

    c1, c2, c3, c4 = st.columns(4)
    request_type = c1.selectbox("Request Type", REQUEST_TYPES, key="builder_request_type")
    customer_name = c2.text_input("Customer Name", key="builder_customer_name")
    order_no = c3.text_input("Order #", key="builder_order_no")
    phone = c4.text_input("Phone", key="builder_phone")

    d1, d2, d3, d4 = st.columns(4)
    source_default = request_type_to_source(request_type)
    if st.session_state.get("builder_ticket_source") not in TICKET_SOURCES:
        st.session_state["builder_ticket_source"] = source_default
    ticket_source = d1.selectbox("Ticket Source", TICKET_SOURCES, key="builder_ticket_source")
    assigned_by = d2.selectbox("Assigned By", ASSIGNED_BY, key="builder_assigned_by")
    assigned_agent = d3.selectbox("Assigned Agent", AGENTS, key="builder_assigned_agent")
    issue_type = d4.selectbox("Issue Type", ISSUE_TYPES, key="builder_issue_type")

    e1, e2, e3 = st.columns(3)
    hubspot_stage = e1.selectbox("HubSpot Stage", HUBSPOT_STAGES, key="builder_hubspot_stage")
    internal_default = default_internal_status(request_type)
    if st.session_state.get("builder_internal_status") not in INTERNAL_STATUSES:
        st.session_state["builder_internal_status"] = internal_default
    internal_status = e2.selectbox("Internal Status", INTERNAL_STATUSES, key="builder_internal_status")
    attempt_no = e3.number_input("Attempt #", min_value=0, max_value=4, key="builder_attempt_no")

    raw_details = st.text_area("Service details / call transcript / SMS interaction / email thread / internal notes", height=260, key="builder_raw_details")

    a1, a2, a3, a4 = st.columns(4)
    if a1.button("Auto Detect from Text"):
        pref = extract_prefill(raw_details, default_source=ticket_source)
        st.session_state["builder_pending_autofill"] = pref
        st.rerun()

    def do_generate():
        detected_issue = detect_issue_type(st.session_state.get("builder_raw_details", ""))
        req_type = st.session_state.get("builder_request_type", REQUEST_TYPES[0])
        source = st.session_state.get("builder_ticket_source", request_type_to_source(req_type))
        cust = st.session_state.get("builder_customer_name", "")
        ord_no = st.session_state.get("builder_order_no", "")
        current_issue = st.session_state.get("builder_issue_type", ISSUE_TYPES[0])
        if current_issue == "Service" and detected_issue != "Service":
            st.session_state["builder_issue_type"] = detected_issue
            current_issue = detected_issue
        ticket_title = generate_ticket_title(source, current_issue, ord_no, cust)
        hubspot_summary, crm_note, problem, resolution, expectation = build_internal_outputs(
            req_type,
            cust,
            ord_no,
            st.session_state.get("builder_raw_details", ""),
            current_issue,
        )
        customer_sms = make_customer_sms(cust, ord_no, req_type, expectation, resolution)
        customer_email = make_customer_email(cust, ord_no, resolution, expectation)
        st.session_state["builder_title"] = ticket_title
        st.session_state["builder_hubspot_summary"] = hubspot_summary
        st.session_state["builder_crm_note"] = crm_note
        st.session_state["builder_sms"] = customer_sms
        st.session_state["builder_email"] = customer_email
        st.session_state["builder_generated"] = True

    if a2.button("Generate Ticket"):
        do_generate()
        st.rerun()

    if a3.button("Generate + Add to Tracker"):
        do_generate()
        st.session_state["prefill_hubspot_title"] = st.session_state["builder_title"]
        st.session_state["prefill_customer_name"] = st.session_state.get("builder_customer_name", "")
        st.session_state["prefill_order_no"] = st.session_state.get("builder_order_no", "")
        st.session_state["prefill_phone"] = st.session_state.get("builder_phone", "")
        st.session_state["prefill_issue_type"] = st.session_state.get("builder_issue_type", ISSUE_TYPES[0])
        st.session_state["prefill_ticket_source"] = st.session_state.get("builder_ticket_source", TICKET_SOURCES[0])
        st.session_state["ticket_hubspot_title"] = st.session_state["builder_title"]
        st.session_state["ticket_customer_name"] = st.session_state.get("builder_customer_name", "")
        st.session_state["ticket_order_no"] = st.session_state.get("builder_order_no", "")
        st.session_state["ticket_phone"] = st.session_state.get("builder_phone", "")
        st.session_state["ticket_issue_type"] = st.session_state.get("builder_issue_type", ISSUE_TYPES[0])
        st.session_state["ticket_ticket_source"] = st.session_state.get("builder_ticket_source", TICKET_SOURCES[0])
        st.session_state["ticket_assigned_by"] = st.session_state.get("builder_assigned_by", ASSIGNED_BY[0])
        st.session_state["ticket_assigned_agent"] = st.session_state.get("builder_assigned_agent", AGENTS[0])
        st.session_state["ticket_hubspot_stage"] = st.session_state.get("builder_hubspot_stage", HUBSPOT_STAGES[0])
        st.session_state["ticket_internal_status"] = st.session_state.get("builder_internal_status", INTERNAL_STATUSES[0])
        st.session_state["ticket_attempt_no"] = int(st.session_state.get("builder_attempt_no", 0))
        st.session_state["ticket_last_activity_date"] = date.today()
        st.session_state["ticket_next_followup_date"] = date.today() if st.session_state.get("builder_internal_status") != "Waiting on Customer" else add_business_days(date.today(), 1)
        st.session_state["ticket_notes_summary"] = st.session_state["builder_hubspot_summary"]
        st.session_state["ticket_edit_select"] = "Create New"
        st.session_state["ticket_loaded_ccid"] = "__new__"
        st.session_state["builder_generated"] = True
        st.success("Tracker fields prefilled. Go to Ticket Tracker and click Save Ticket.")

    if a4.button("Clear Builder"):
        for key, default in {
            "builder_generated": False,
            "builder_title": "",
            "builder_hubspot_summary": "",
            "builder_crm_note": "",
            "builder_sms": "",
            "builder_email": "",
            "builder_request_type": REQUEST_TYPES[0],
            "builder_customer_name": "",
            "builder_order_no": "",
            "builder_phone": "",
            "builder_ticket_source": request_type_to_source(REQUEST_TYPES[0]),
            "builder_assigned_by": ASSIGNED_BY[0],
            "builder_assigned_agent": AGENTS[0],
            "builder_issue_type": ISSUE_TYPES[0],
            "builder_hubspot_stage": HUBSPOT_STAGES[0],
            "builder_internal_status": default_internal_status(REQUEST_TYPES[0]),
            "builder_attempt_no": 0,
            "builder_raw_details": "",
        }.items():
            st.session_state[key] = default
        st.rerun()

    if st.session_state.get("builder_generated"):
        st.markdown("### Editable Outputs")
        st.text_input("HubSpot Ticket Name", key="builder_title")
        st.text_area("HubSpot Summary", height=260, key="builder_hubspot_summary")
        st.text_area("CRM Note", height=240, key="builder_crm_note")
        st.text_area("Customer SMS", height=150, key="builder_sms")
        st.text_area("Customer Email", height=260, key="builder_email")


def generate_service_repair_template(original_order="", repair_needed="", why="", photos_videos="No"):
    original_order = original_order or "N/A"
    repair_needed = repair_needed or "N/A"
    why = why or "N/A"
    photos_videos = photos_videos or "No"

    return f"""Original Order#: {original_order}

Repair Needed: {repair_needed}

Why: {why}

Photos/videos: {photos_videos}"""


def service_repair_builder_page():
    st.subheader("Service / Repair Builder")
    preset = st.selectbox("Template", list(REPAIR_PRESETS.keys()))
    original_order = st.text_input("Original Order #")
    photos_videos = st.selectbox("Photos / Videos", ["Yes", "No"])
    repair_needed = st.text_area("Repair Needed", value=REPAIR_PRESETS[preset]["repair_needed"], height=120)
    why = st.text_area("Why", value=REPAIR_PRESETS[preset]["why"], height=120)
    if st.button("Generate Service / Repair Template"):
        st.text_area("Template Output", value=generate_service_repair_template(original_order, repair_needed, why, photos_videos), height=220)


def generate_scheduling_request(order_no="", customer="", phone="", installer_region="", availability="", special_notes=""):
    order_no = order_no or "N/A"
    customer = customer or "N/A"
    phone = phone or "N/A"
    installer_region = installer_region or "N/A"
    availability = availability or "Next Available"
    special_notes = special_notes or "N/A"

    return f"""SCHEDULING REQUEST:
🔢 Active Order Number: {order_no}
👤 Customer Name: {customer}
📞☎️ Phone: {phone}
🚚 Installer/Region: {installer_region}
📅 Requested Date & Time / Availability: {availability}
📝 Any special notes: {special_notes}"""


def scheduling_page():
    st.subheader("Scheduling Assistant")
    ctop1, ctop2 = st.columns(2)
    if ctop1.button("Refresh Region Map"):
        load_region_map.clear()
        st.success("Region map refreshed.")
    if ctop2.button("Refresh Scheduling Workbook"):
        load_scheduler_dashboard.clear()
        st.success("Scheduling workbook refreshed.")

    tab1, tab2 = st.tabs(["Scheduling Request Generator", "ZIP → Installer Priority"])
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
            template = generate_scheduling_request(order_no, customer, phone, installer_region, availability, special_notes)
            route, reason = compute_scheduling_route(flags)
            st.text_area("Scheduling Request Output", value=template, height=200)
            st.info(f"Routing: {route} | Reason: {reason}")
            if route == "Moses Email":
                st.text_area("Email Output for Moses Torres", value=f"Subject: LACB | Scheduling Request | {order_no}\n\n{template}\n", height=220)
            elif route == "Direct Scheduling":
                st.success(f"Suggested direct scheduling target date: {add_business_days(date.today(), 14).isoformat()}")

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
                    out = pd.DataFrame([{"Priority Order": i + 1, "Installer": item["installer"], "Tier": item["priority"]} for i, item in enumerate(installers)])
                    st.dataframe(out, use_container_width=True, hide_index=True)
                else:
                    st.warning("No installers found for that region in Regional Dashboard.")


def history_page(agent_filter):
    st.subheader("History / Export")
    acts = fetch_activities()
    tickets = fetch_tickets()
    if agent_filter != "All":
        acts = acts[acts["agent"] == agent_filter]
        tickets = tickets[tickets["assigned_agent"] == agent_filter]
    search = st.text_input("History search")
    if search:
        s = search.lower()
        acts = acts[
            acts["ticket_cc_id"].fillna("").str.lower().str.contains(s) |
            acts["hubspot_title"].fillna("").str.lower().str.contains(s) |
            acts["customer_name"].fillna("").str.lower().str.contains(s) |
            acts["order_no"].fillna("").str.lower().str.contains(s)
        ]
    st.dataframe(acts, use_container_width=True, hide_index=True)
    c1, c2 = st.columns(2)
    with c1:
        df_download_button(acts, "LACB_Activity_History_V8.xlsx", "Download Activity History")
    with c2:
        df_download_button(tickets, "LACB_Ticket_Tracker_V8.xlsx", "Download Ticket Tracker")


def main():
    st.set_page_config(page_title="LACB Customer Care Command Center V8", layout="wide")
    init_db()
    st.title("LACB Customer Care Command Center V8")
    st.caption("Follow-ups, KPI tracking, ticket control, HubSpot ticket building, scheduling support, duplicate protection, service / repair building, and editable outputs for Ed Torres and Erika Sagasta.")

    with st.sidebar:
        agent_filter = st.selectbox("Logged in as", ["All"] + AGENTS, index=1)
        page = st.radio("Navigation", [
            "Follow-Up Assistant",
            "KPI Dashboard",
            "Ticket Tracker",
            "Quick Log",
            "HubSpot Ticket Builder",
            "Service / Repair Builder",
            "Scheduling Assistant",
            "History / Export",
        ])

    if page == "Follow-Up Assistant":
        followup_page(agent_filter)
    elif page == "KPI Dashboard":
        dashboard_page(agent_filter)
    elif page == "Ticket Tracker":
        ticket_tracker_page(agent_filter)
    elif page == "Quick Log":
        quick_log_page(agent_filter)
    elif page == "HubSpot Ticket Builder":
        hubspot_ticket_builder_page()
    elif page == "Service / Repair Builder":
        service_repair_builder_page()
    elif page == "Scheduling Assistant":
        scheduling_page()
    elif page == "History / Export":
        history_page(agent_filter)


if __name__ == "__main__":
    main()
