
import streamlit as st

st.set_page_config(page_title="LACB Customer Care Command Center", layout="wide")

st.title("LACB Customer Care Command Center")

st.sidebar.title("Navigation")

page = st.sidebar.radio(
    "Go to",
    [
        "Follow-Up Assistant",
        "Dashboard",
        "Ticket Tracker",
        "Quick Log",
        "CRM Note Generator",
        "Scheduling Request",
        "History / Export"
    ]
)

if page == "Follow-Up Assistant":
    st.header("Follow-Up Assistant")
    st.write("Tickets that need action today.")

if page == "Dashboard":
    st.header("KPI Dashboard")

if page == "Ticket Tracker":
    st.header("Ticket Tracker")

if page == "Quick Log":
    st.header("Quick Activity Log")

if page == "CRM Note Generator":
    st.header("CRM Note Generator")

if page == "Scheduling Request":
    st.header("Scheduling Request Generator")

if page == "History / Export":
    st.header("History and Export")