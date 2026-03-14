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
    "Direct SMS", "Direct Call", "Email", "Internal Chat"
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


# -----------------------------
# Core helpers
# -----------------------------
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


# -----------------------------
# Parsing / autofill helpers
# -----------------------------
def extract_order_no(text):
    m = re.search(r"\bO-\d+\b", text or "", flags=re.I)
    return m.group(0).upper() if m else ""


def extract_phone(text):
    m = re.search(r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", text or "")
    return m.group(1) if m else ""


def detect_issue_type(text):
    t = (text or "").lower()
    if any(x in t for x in ["eta", "order status", "status update", "in production", "ready date"]):
        return "Order Status/ETA Update"
    if any(x in t for x in ["pc intro", "project coordinator", "i'll be overseeing your order", "i am the project coordinator"]):
        return "PC Intro"
    if any(x in t for x in ["remote", "motor", "charger", "program", "not responding"]):
        return "Motor/Remote Troubleshooting"
    if any(x in t for x in ["assess", "assessment", "service call", "repair needed"]):
        return "Repair Assessment"
    if any(x in t for x in ["install", "installer"]):
        return "Install"
    if any(x in t for x in ["ship", "tracking", "vendor", "warehouse"]):
        return "Shipping"
    if any(x in t for x in ["warranty"]):
        return "Warranty"
    return "Service"


def extract_customer_name(text):
    text = text or ""
    m = re.search(r"CUSTOMER\s*:\s*([A-Z][A-Za-z\-\' ]+)", text, flags=re.I)
    if m:
        return normalize_text(m.group(1))
    # common transcript pattern: "Order #: ..." then a name nearby won't be reliable
    return ""


def generate_ticket_title(source_label, issue_type, order_no, customer_name):
    source_map = {
        "Direct Call": "Inbound Call",
        "Receptionist": "Inbound Call",
        "Email": "Email",
        "Direct SMS": "SMS",
        "Internal Chat": "Internal Chat",
        "Sales Team": "Inbound Call",
        "Supervisor": "Inbound Call",
        "Self-Created": "Inbound Call",
    }
    prefix = source_map.get(source_label, "Inbound Call")
    issue = issue_type if issue_type else "Service Request"
    order_part = order_no if order_no else "Order TBD"
    name_part = customer_name if customer_name else "Customer"
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
    }


# -----------------------------
# DB init
# -----------------------------
def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
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
    """)
    cur.execute("""
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
    """)
    conn.commit()

    cur.execute("PRAGMA table_info(tickets)")
    cols = [r["name"] for r in cur.fetchall()]
    if "raw_conversation" not in cols:
        cur.execute("ALTER TABLE tickets ADD COLUMN raw_conversation TEXT")
        conn.commit()

    cur.execute("SELECT COUNT(*) AS c FROM tickets")
    if cur.fetchone()["c"] == 0:
        seed_demo(conn)
    conn.close()


def seed_demo(conn):
    t = date.today()
    demo = [
        (
            "CC-0001", "Barry REQ Service Call - Denise Schmidt", "Denise Schmidt", "O-36379", "(714) 876-5969",
            "Receptionist", "Reception Desk", "Ed Torres", "Service", "Waiting on customer", "Waiting on Customer",
            2, t.isoformat(), add_business_days(t, 1).isoformat(), 0, "Direct Scheduling", "Standard Service",
            "Customer needs service follow-up.", "", now_iso(), now_iso()
        ),
        (
            "CC-0002", "Email Sent to Vendor - ETA Request", "Amber Margolis", "O-66211", "",
            "Supervisor", "Melisa", "Erika Sagasta", "Vendor", "Waiting on vendor", "Waiting on Vendor",
            0, t.isoformat(), add_business_days(t, 1).isoformat(), 0, "", "",
            "Waiting on vendor ETA.", "", now_iso(), now_iso()
        ),
    ]
    conn.executemany("""
        INSERT INTO tickets (
            cc_id, hubspot_title, customer_name, order_no, phone, ticket_source, assigned_by, assigned_agent,
            issue_type, hubspot_stage, internal_status, attempt_no, last_activity_date, next_followup_date,
            special_scheduling_required, scheduling_route, scheduling_reason, notes_summary, raw_conversation, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, demo)
    conn.commit()


def next_cc_id(conn):
    cur = conn.cursor()
    cur.execute("SELECT cc_id FROM tickets ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    if not row or not row["cc_id"]:
        return "CC-0001"
    num = int(row["cc_id"].split("-")[1]) + 1
    return f"CC-{num:04d}"


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


def save_ticket(data):
    conn = get_conn()
    cur = conn.cursor()
    if data.get("cc_id"):
        cur.execute("""
            UPDATE tickets SET
                hubspot_title=?, customer_name=?, order_no=?, phone=?, ticket_source=?, assigned_by=?, assigned_agent=?,
                issue_type=?, hubspot_stage=?, internal_status=?, attempt_no=?, last_activity_date=?, next_followup_date=?,
                special_scheduling_required=?, scheduling_route=?, scheduling_reason=?, notes_summary=?, raw_conversation=?, updated_at=?
            WHERE cc_id=?
        """, (
            data["hubspot_title"], data["customer_name"], data["order_no"], data["phone"], data["ticket_source"],
            data["assigned_by"], data["assigned_agent"], data["issue_type"], data["hubspot_stage"], data["internal_status"],
            int(data["attempt_no"]), data["last_activity_date"], data["next_followup_date"], int(bool(data["special_scheduling_required"])),
            data["scheduling_route"], data["scheduling_reason"], data["notes_summary"], data.get("raw_conversation", ""), now_iso(), data["cc_id"]
        ))
    else:
        cc_id = next_cc_id(conn)
        cur.execute("""
            INSERT INTO tickets (
                cc_id, hubspot_title, customer_name, order_no, phone, ticket_source, assigned_by, assigned_agent,
                issue_type, hubspot_stage, internal_status, attempt_no, last_activity_date, next_followup_date,
                special_scheduling_required, scheduling_route, scheduling_reason, notes_summary, raw_conversation, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cc_id, data["hubspot_title"], data["customer_name"], data["order_no"], data["phone"], data["ticket_source"],
            data["assigned_by"], data["assigned_agent"], data["issue_type"], data["hubspot_stage"], data["internal_status"],
            int(data["attempt_no"]), data["last_activity_date"], data["next_followup_date"], int(bool(data["special_scheduling_required"])),
            data["scheduling_route"], data["scheduling_reason"], data["notes_summary"], data.get("raw_conversation", ""), now_iso(), now_iso()
        ))
        data["cc_id"] = cc_id
    conn.commit()
    conn.close()
    return data["cc_id"]


def save_activity(ticket, agent, actions, result, notes_summary):
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

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO activities (
            ticket_cc_id, hubspot_title, customer_name, order_no, agent, actions, result,
            notes_summary, activity_ts, activity_date, attempt_no, next_followup_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ticket["cc_id"], ticket["hubspot_title"], ticket["customer_name"], ticket["order_no"], agent,
        ", ".join(actions), result, notes_summary, now_iso(), today, attempt_no, next_followup_date
    ))
    cur.execute("""
        UPDATE tickets SET
            assigned_agent=?, attempt_no=?, last_activity_date=?, next_followup_date=?, internal_status=?,
            hubspot_stage=?, notes_summary=?, updated_at=?
        WHERE cc_id=?
    """, (
        agent, attempt_no, today, next_followup_date, new_status, hubspot_stage, notes_summary, now_iso(), ticket["cc_id"]
    ))
    conn.commit()
    conn.close()


# -----------------------------
# File loaders
# -----------------------------
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
    for _, row in df.iterrows():
        role = str(row.get("ROLE", "")).strip().upper()
        if role != "INSTALLER":
            continue
        marker = str(row.get(col, "")).strip().upper()
        if marker in {"P1", "P2", "P3"}:
            installers.append({
                "installer": str(row.get("NAME", "")).strip(),
                "priority": marker,
            })
    return installers


def lookup_region_by_zip(zip_code):
    df = load_region_map()
    if df.empty or "ZIP_CODE" not in df.columns or "REGION" not in df.columns:
        return ""
    zip_code = re.sub(r"\D", "", str(zip_code).strip())[:5]
    match = df[df["ZIP_CODE"].astype(str).str.strip() == zip_code]
    if match.empty:
        return ""
    return str(match.iloc[0]["REGION"]).strip()


# -----------------------------
# Generators
# -----------------------------
def generate_crm_note(customer, order_no, action, issue, summary):
    return f"""CRM NOTE / Ticket Update – {action} – {issue}

PROBLEM:
{customer} regarding order {order_no or "N/A"}: {issue}.

RESOLUTION:
I {summary}.

EXPECTATION:
I will continue monitoring and follow up on the next required step until resolved."""


def generate_ticket_summary(problem, resolution, expectation):
    return f"""PROBLEM:
{problem}

RESOLUTION:
{resolution}

EXPECTATION:
{expectation}"""


def generate_scheduling_request(order_no, customer, phone, installer_region, availability, special_notes):
    return f"""SCHEDULING REQUEST:
🔢 Active Order Number: {order_no}
👤 Customer Name: {customer}
📞☎️ Phone: {phone}
🚚 Installer/Region: {installer_region}
📅 Requested Date & Time / Availability: {availability}
📝 Any special notes: {special_notes}"""


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


# -----------------------------
# Pages
# -----------------------------
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
        out = df[[
            "cc_id", "hubspot_title", "customer_name", "order_no", "assigned_agent",
            "internal_status", "attempt_no", "last_activity_date", "next_followup_date"
        ]].copy()
        out["recommended_next_action"] = df.apply(
            lambda r: recommended_next_action(r["internal_status"], int(r["attempt_no"] or 0)), axis=1
        )
        return out

    st.markdown("### Call First / Due Today")
    out = prep(due)
    if out.empty:
        st.caption("No follow-ups due today.")
    else:
        st.dataframe(out, use_container_width=True, hide_index=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Overdue")
        out = prep(overdue)
        if out.empty:
            st.caption("No overdue follow-ups.")
        else:
            st.dataframe(out, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("### Ready to Close")
        out = prep(ready)
        if out.empty:
            st.caption("No tickets ready to close.")
        else:
            st.dataframe(out, use_container_width=True, hide_index=True)


def dashboard_page(agent_filter):
    st.subheader("KPI Dashboard")
    tickets = fetch_tickets()
    acts = fetch_activities()
    if agent_filter != "All":
        tickets = tickets[tickets["assigned_agent"] == agent_filter]
        acts = acts[acts["agent"] == agent_filter]
    today = today_iso()
    acts_today = acts[acts["activity_date"] == today] if not acts.empty else acts

    def count_action(label):
        if acts_today.empty:
            return 0
        return acts_today["actions"].fillna("").str.contains(label, regex=False).sum()

    st.markdown("### Today's Activity")
    a1, a2, a3, a4, a5 = st.columns(5)
    a1.metric("Outbound Calls", count_action("Outbound Call"))
    a2.metric("Inbound Calls", count_action("Inbound Call"))
    a3.metric("SMS Sent", count_action("SMS Sent"))
    a4.metric("Emails Sent", count_action("Email Sent"))
    a5.metric("Internal Chats", count_action("Internal Chat"))

    st.markdown("### Ticket Workload")
    t1, t2, t3, t4, t5 = st.columns(5)
    t1.metric("Tickets Touched Today", len(acts_today))
    t2.metric("New Tickets", len(tickets[tickets["created_at"].fillna("").str.startswith(today)]) if not tickets.empty else 0)
    t3.metric("Closed", len(tickets[tickets["internal_status"] == "Closed"]))
    t4.metric("Ready to Close", len(tickets[tickets["internal_status"] == "Ready to Close"]))
    t5.metric("Scheduled", len(tickets[tickets["internal_status"] == "Scheduled"]))

    if not tickets.empty and "issue_type" in tickets.columns:
        st.markdown("### Issue Types")
        issue_counts = tickets["issue_type"].fillna("Unspecified").value_counts().reset_index()
        issue_counts.columns = ["Issue Type", "Count"]
        st.dataframe(issue_counts, use_container_width=True, hide_index=True)


def ticket_tracker_page(agent_filter):
    st.subheader("Ticket Tracker")
    tickets = fetch_tickets()

    with st.expander("Create / Update Ticket", expanded=False):
        editable = st.selectbox("Edit existing ticket", ["Create New"] + (tickets["cc_id"].tolist() if not tickets.empty else []))
        row = None
        if editable != "Create New":
            row = tickets[tickets["cc_id"] == editable].iloc[0]

        with st.form("ticket_form_v5"):
            c1, c2 = st.columns(2)
            hubspot_title = c1.text_input("HubSpot Ticket Title", value="" if row is None else row["hubspot_title"])
            customer_name = c2.text_input("Customer Name", value="" if row is None else row["customer_name"])

            c3, c4, c5 = st.columns(3)
            order_no = c3.text_input("Order #", value="" if row is None else (row["order_no"] or ""))
            phone = c4.text_input("Phone", value="" if row is None else (row["phone"] or ""))
            issue_type = c5.selectbox(
                "Issue Type",
                ISSUE_TYPES,
                index=ISSUE_TYPES.index(row["issue_type"]) if row is not None and row["issue_type"] in ISSUE_TYPES else 0,
            )

            c6, c7, c8 = st.columns(3)
            ticket_source = c6.selectbox(
                "Ticket Source",
                TICKET_SOURCES,
                index=TICKET_SOURCES.index(row["ticket_source"]) if row is not None and row["ticket_source"] in TICKET_SOURCES else 0,
            )
            assigned_by = c7.selectbox(
                "Assigned By",
                ASSIGNED_BY,
                index=ASSIGNED_BY.index(row["assigned_by"]) if row is not None and row["assigned_by"] in ASSIGNED_BY else 0,
            )
            assigned_agent = c8.selectbox(
                "Assigned Agent",
                AGENTS,
                index=AGENTS.index(row["assigned_agent"]) if row is not None and row["assigned_agent"] in AGENTS else 0,
            )

            c9, c10, c11 = st.columns(3)
            hubspot_stage = c9.selectbox(
                "HubSpot Stage",
                HUBSPOT_STAGES,
                index=HUBSPOT_STAGES.index(row["hubspot_stage"]) if row is not None and row["hubspot_stage"] in HUBSPOT_STAGES else 0,
            )
            internal_status = c10.selectbox(
                "Internal Status",
                INTERNAL_STATUSES,
                index=INTERNAL_STATUSES.index(row["internal_status"]) if row is not None and row["internal_status"] in INTERNAL_STATUSES else 0,
            )
            attempt_no = c11.number_input(
                "Attempt #",
                min_value=0,
                max_value=4,
                value=int(row["attempt_no"]) if row is not None and pd.notna(row["attempt_no"]) else 0,
            )

            c12, c13 = st.columns(2)
            last_activity_date = c12.date_input(
                "Last Activity Date",
                value=date.today() if row is None or not row["last_activity_date"] else datetime.strptime(row["last_activity_date"], "%Y-%m-%d").date(),
            )
            next_followup_date = c13.date_input(
                "Next Follow-Up Date",
                value=date.today() if row is None or not row["next_followup_date"] else datetime.strptime(row["next_followup_date"], "%Y-%m-%d").date(),
            )

            scheduling_flags = st.multiselect(
                "Scheduling Flags",
                SCHEDULING_FLAGS,
                default=[] if row is None or not row["scheduling_reason"] else [
                    x.strip() for x in str(row["scheduling_reason"]).split(",") if x.strip() in SCHEDULING_FLAGS
                ],
            )
            scheduling_route, scheduling_reason = compute_scheduling_route(scheduling_flags)
            notes_summary = st.text_area("Notes Summary", value="" if row is None else (row["notes_summary"] or ""))

            col_save, col_save_new, col_clear = st.columns(3)
            submitted = col_save.form_submit_button("Save Ticket")
            save_start_new = col_save_new.form_submit_button("Save + Start New")
            clear_form = col_clear.form_submit_button("Clear Form")
            if clear_form:
                st.session_state.clear()
                st.rerun()

            if submitted or save_start_new:
                cc_id = save_ticket({
                    "cc_id": None if row is None else row["cc_id"],
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
                    "raw_conversation": "" if row is None else (row["raw_conversation"] if "raw_conversation" in row.index else ""),
                })
                st.success(f"Ticket saved: {cc_id}")
                if save_start_new:
                    st.session_state.clear()
                st.rerun()

    st.markdown("### Ticket List")
    c1, c2 = st.columns(2)
    status_filter = c1.selectbox("Filter by Status", ["All"] + INTERNAL_STATUSES)
    search = c2.text_input("Search by CC ID, customer, order, or title")
    filtered = apply_ticket_filters(tickets, agent_filter, status_filter, search)
    if filtered.empty:
        st.info("No matching tickets.")
    else:
        st.dataframe(
            filtered[[
                "cc_id", "hubspot_title", "customer_name", "order_no", "assigned_agent",
                "issue_type", "internal_status", "attempt_no", "next_followup_date", "scheduling_route", "updated_at"
            ]],
            use_container_width=True,
            hide_index=True,
        )


def quick_log_page(agent_filter):
    st.subheader("Quick Activity Log")
    tickets = fetch_tickets()
    tickets = tickets[tickets["internal_status"] != "Closed"]
    if tickets.empty:
        st.warning("Create a ticket first.")
        return
    search = st.text_input("Search ticket")
    tickets = apply_ticket_filters(tickets, "All", "All", search)
    if tickets.empty:
        st.info("No matching open tickets.")
        return
    options = {f'{r["cc_id"]} | {r["customer_name"]} | {r["order_no"] or ""} | {r["hubspot_title"]}': r["cc_id"] for _, r in tickets.iterrows()}
    selected_label = st.selectbox("Select Ticket", list(options.keys()))
    ticket = tickets[tickets["cc_id"] == options[selected_label]].iloc[0]
    st.caption(
        f'Current Status: {ticket["internal_status"]} | Attempt #: {int(ticket["attempt_no"] or 0)} | '
        f'Recommended Next Action: {recommended_next_action(ticket["internal_status"], int(ticket["attempt_no"] or 0))}'
    )
    with st.form("quick_log_form_v5"):
        c1, c2, c3 = st.columns(3)
        customer_name = c1.text_input("Customer Name", value=ticket["customer_name"])
        order_no = c2.text_input("Order #", value=ticket["order_no"] or "")
        idx = AGENTS.index(agent_filter) if agent_filter in AGENTS else 0
        agent = c3.selectbox("Agent", AGENTS, index=idx)
        actions = st.multiselect("Actions Performed", ACTIONS)
        result = st.selectbox("Result", RESULTS)
        notes_summary = st.text_area("Notes Summary")
        if st.form_submit_button("Save Activity"):
            if not actions:
                st.error("Select at least one action.")
            else:
                ticket_dict = ticket.to_dict()
                ticket_dict["customer_name"] = customer_name
                ticket_dict["order_no"] = order_no
                save_activity(ticket_dict, agent, actions, result, notes_summary)
                st.success("Activity saved.")
                st.rerun()


def conversation_page():
    st.subheader("Conversation + CRM Builder")
    st.caption("Paste transcripts, SMS, emails, or notes. Extract ticket info and generate CRM summaries from the same screen.")

    if "conv_text" not in st.session_state:
        st.session_state.conv_text = ""

    source_default = st.selectbox("Conversation Source", ["Direct Call", "Email", "Direct SMS", "Internal Chat", "Receptionist"])

    st.session_state.conv_text = st.text_area(
        "Paste call transcript, SMS thread, email thread, or notes",
        value=st.session_state.conv_text,
        height=260
    )

    c1, c2 = st.columns(2)

    with c1:
        if st.button("Extract / Prefill Ticket Info"):
            prefill = extract_prefill(st.session_state.conv_text, default_source=source_default)
            st.session_state["prefill_hubspot_title"] = prefill["hubspot_title"]
            st.session_state["prefill_customer_name"] = prefill["customer_name"]
            st.session_state["prefill_order_no"] = prefill["order_no"]
            st.session_state["prefill_phone"] = prefill["phone"]
            st.session_state["prefill_issue_type"] = prefill["issue_type"]
            st.success("Ticket fields detected.")
            st.json(prefill)

    with c2:
        if st.button("Clear Conversation Text"):
            st.session_state.conv_text = ""
            st.rerun()

    st.markdown("---")
    st.markdown("### CRM / HubSpot Summary Generator")

    problem = st.text_area("PROBLEM")
    resolution = st.text_area("RESOLUTION")
    expectation = st.text_area("EXPECTATION")

    if st.button("Generate Summary"):
        st.text_area(
            "Summary Output",
            value=generate_ticket_summary(problem, resolution, expectation),
            height=260
        )


def crm_note_page():
    st.info("This page has been merged into 'Conversation Intake'. Use the Conversation + CRM Builder instead.")
    st.subheader("CRM Note Generator")
    mode = st.radio("Generator Mode", ["CRM Note", "HubSpot Summary"], horizontal=True)

    if mode == "CRM Note":
        c1, c2 = st.columns(2)
        customer = c1.text_input("Customer Name")
        order_no = c2.text_input("Order #")
        c3, c4 = st.columns(2)
        action = c3.text_input("Action", placeholder="SMS Sent / Outbound Call / Vendor Follow-Up")
        issue = c4.text_input("Issue / Topic", placeholder="Shipping request / service follow-up / payment request")
        summary = st.text_area("Summary points", placeholder="sent SMS update confirming request submitted to warehouse")
        if st.button("Generate CRM Note"):
            st.text_area("Output", value=generate_crm_note(customer, order_no, action, issue, summary), height=240)
    else:
        problem = st.text_area("PROBLEM")
        resolution = st.text_area("RESOLUTION")
        expectation = st.text_area("EXPECTATION")
        if st.button("Generate HubSpot Summary"):
            st.text_area("Summary Output", value=generate_ticket_summary(problem, resolution, expectation), height=260)


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
        st.caption("Uses Region_Map.csv for ZIP → Region and Scheduling Tool_2_20.xlsx / Regional Dashboard for installer priority.")
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
                    out = pd.DataFrame([
                        {"Priority Order": i + 1, "Installer": item["installer"], "Tier": item["priority"]}
                        for i, item in enumerate(installers)
                    ])
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
        df_download_button(acts, "LACB_Activity_History_V5.xlsx", "Download Activity History")
    with c2:
        df_download_button(tickets, "LACB_Ticket_Tracker_V5.xlsx", "Download Ticket Tracker")


# -----------------------------
# App
# -----------------------------
def main():
    st.set_page_config(page_title="LACB Customer Care Command Center V5", layout="wide")
    init_db()
    st.title("LACB Customer Care Command Center V5")
    st.caption("Follow-ups, KPI tracking, ticket control, CRM notes, transcript intake, scheduling support, and smarter ticket creation for Ed Torres and Erika Sagasta.")

    with st.sidebar:
        agent_filter = st.selectbox("Logged in as", ["All"] + AGENTS, index=1)
        page = st.radio(
            "Navigation",
            [
                "Follow-Up Assistant",
                "KPI Dashboard",
                "Ticket Tracker",
                "Quick Log",
                "Conversation Intake",  # Conversation + CRM Builder
                "CRM Note Generator",
                "Scheduling Assistant",
                "History / Export",
            ],
        )

    if page == "Follow-Up Assistant":
        followup_page(agent_filter)
    elif page == "KPI Dashboard":
        dashboard_page(agent_filter)
    elif page == "Ticket Tracker":
        ticket_tracker_page(agent_filter)
    elif page == "Quick Log":
        quick_log_page(agent_filter)
    elif page == "Conversation Intake":
        conversation_page()
    elif page == "CRM Note Generator":
        crm_note_page()
    elif page == "Scheduling Assistant":
        scheduling_page()
    elif page == "History / Export":
        history_page(agent_filter)


if __name__ == "__main__":
    main()
