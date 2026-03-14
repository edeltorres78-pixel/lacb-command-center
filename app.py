import streamlit as st
import pandas as pd
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# ---------------- CONFIG ----------------
EXCEL_FILE = "Scheduling Tool_2_20.xlsx"

st.set_page_config(page_title="LACB Scheduling Assistant", layout="wide")

st.title("LACB Scheduling Assistant (MVP)")
st.caption("Paste an address → detect ZIP → match region → show installers by P1 then P2.")

# Reload button
if st.button("🔄 Reload Excel Data"):
    st.cache_data.clear()
    st.rerun()

# ---------------- GEOCODER ----------------
@st.cache_data(show_spinner=False)
def geocode_address(address: str) -> dict:

    address = address.strip()

    # ZIP-only input
    if re.fullmatch(r"\d{5}", address):
        return {
            "found": True,
            "zip": address,
            "city": "",
            "county": "",
            "state": "California"
        }

    geolocator = Nominatim(user_agent="lacb-scheduling-assistant")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    location = geocode(
        address,
        addressdetails=True,
        country_codes="us"
    )

    if not location:
        return {"found": False}

    addr = location.raw.get("address", {})
    postcode = addr.get("postcode", "")
    zip5 = re.findall(r"\d{5}", str(postcode))
    zip5 = zip5[0] if zip5 else ""

    return {
        "found": True,
        "zip": zip5,
        "city": addr.get("city") or addr.get("town") or "",
        "county": addr.get("county", ""),
        "state": addr.get("state", ""),
    }


# ---------------- LOAD DASHBOARD ----------------
@st.cache_data
def load_regional_dashboard(excel_path: str):

    df = pd.read_excel(
        excel_path,
        sheet_name="Regional Dashboard",
        header=1  # headers are on row 2
    )

    installers = df[df["ROLE"].str.upper() == "INSTALLER"].copy()

    region_cols = [
        c for c in installers.columns
        if c not in ["NAME", "ROLE"]
        and not str(c).lower().startswith("unnamed")
    ]

    # Clean region headers
    clean_map = {}
    for c in region_cols:
        col = str(c)
        col = col.replace("\xa0", " ")
        col = re.sub(r"\(.*?\)", "", col)
        col = re.sub(r"\s+", " ", col).strip().upper()
        clean_map[c] = col

    installers = installers.rename(columns=clean_map)
    region_cols_clean = list(clean_map.values())

    long_df = installers.melt(
        id_vars=["NAME"],
        value_vars=region_cols_clean,
        var_name="REGION",
        value_name="PRIORITY"
    )

    long_df = long_df.dropna(subset=["PRIORITY"])
    long_df["INSTALLER"] = long_df["NAME"]
    long_df["PRIORITY"] = long_df["PRIORITY"].str.upper().str.strip()

    long_df["PRIORITY_RANK"] = long_df["PRIORITY"].map({"P1": 1, "P2": 2})

    all_regions = sorted(long_df["REGION"].unique())

    return long_df, all_regions


# ---------------- REGION MAP ----------------
@st.cache_data(show_spinner=False)
def load_region_map(excel_path: str) -> dict:
    try:
        rm = pd.read_excel(excel_path, sheet_name="Region_Map")
    except Exception as e:
        st.error(f"Could not load Region_Map sheet: {e}")
        return {}

    # Normalize column names
    rm.columns = [str(c).strip().lower() for c in rm.columns]

    if "zip" not in rm.columns or "region" not in rm.columns:
        st.error("Region_Map must contain columns named 'zip' and 'region'")
        return {}

    # CLEAN ZIP VALUES (THIS IS THE FIX)
    rm["zip"] = (
        rm["zip"]
        .astype(str)
        .str.replace(".0", "", regex=False)  # remove excel decimals
        .str.extract(r"(\d{5})")[0]          # keep only 5 digits
    )

    # CLEAN REGION VALUES
    rm["region"] = (
        rm["region"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    rm = rm.dropna(subset=["zip", "region"])
    rm = rm.drop_duplicates(subset=["zip"], keep="last")

    return dict(zip(rm["zip"], rm["region"]))


# ---------------- INSTALLER FILTER ----------------
def get_installers_for_region(long_df, region):

    if not region:
        return pd.DataFrame()

    region = str(region).upper().strip()

    out = long_df[long_df["REGION"] == region].copy()
    out = out.sort_values(["PRIORITY_RANK", "INSTALLER"])

    return out[["INSTALLER", "PRIORITY"]]


# ---------------- LOAD DATA ----------------
try:
    long_df, all_regions = load_regional_dashboard(EXCEL_FILE)
    region_map = load_region_map(EXCEL_FILE)
except Exception as e:
    st.error(f"Could not read Excel. Make sure '{EXCEL_FILE}' is in this folder.\nDetails: {e}")
    st.stop()


# ---------------- UI ----------------
address = st.text_input("Customer address", placeholder="Example: 92618 or full address")

st.divider()

geo = geocode_address(address) if address.strip() else {"found": False}

detected_region = None

if geo.get("found"):
    st.write(
        f"**ZIP:** {geo['zip']} | "
        f"**City:** {geo['city']} | "
        f"**County:** {geo['county']} | "
        f"**State:** {geo['state']}"
    )

    detected_region = region_map.get(geo["zip"])

# Region selector
if detected_region:
    st.success(f"Detected region: {detected_region}")
    region = detected_region
else:
    region = st.selectbox("Select region", options=all_regions)

installers = get_installers_for_region(long_df, region)

# Recommended installer
best_installer = installers[installers["PRIORITY"] == "P1"].head(1)

if not best_installer.empty:
    st.success(f"✅ Recommended Installer: {best_installer.iloc[0]['INSTALLER']} (P1)")

if installers.empty:
    st.warning("No installers found for that region.")
    st.stop()

# ---------------- DISPLAY ----------------
p1 = installers[installers["PRIORITY"] == "P1"]
p2 = installers[installers["PRIORITY"] == "P2"]

c1, c2 = st.columns(2)

with c1:
    st.subheader("Priority 1 (P1)")
    st.dataframe(p1[["INSTALLER"]], use_container_width=True, hide_index=True)

with c2:
    st.subheader("Priority 2 (P2)")
    st.dataframe(p2[["INSTALLER"]], use_container_width=True, hide_index=True)

st.divider()

copy_text = (
    "P1:\n" + "\n".join(p1["INSTALLER"].tolist()) +
    "\n\nP2:\n" + "\n".join(p2["INSTALLER"].tolist())
)

st.subheader("Quick Copy")
st.code(copy_text)