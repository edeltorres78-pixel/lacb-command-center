import pandas as pd

# ---------- URLs ----------
URLS = {
    "VENTURA CO": "https://www.ciclt.net/sn/clt/capitolimpact/gw_ziplist.aspx?ClientCode=capitolimpact&FIPS=06111&State=ca",
    "LA": "https://www.ciclt.net/sn/clt/capitolimpact/gw_ziplist.aspx?ClientCode=capitolimpact&FIPS=06037&State=ca",
    "OC": "https://www.ciclt.net/sn/clt/capitolimpact/gw_ziplist.aspx?ClientCode=capitolimpact&FIPS=06059&State=ca",
    "RIVERSIDE": "https://www.ciclt.net/sn/clt/capitolimpact/gw_ziplist.aspx?ClientCode=capitolimpact&FIPS=06065&State=ca",
    "SAN BERN": "https://www.ciclt.net/sn/clt/capitolimpact/gw_ziplist.aspx?ClientCode=capitolimpact&FIPS=06071&State=ca",
    "SAN DIEGO": "https://www.ciclt.net/sn/clt/capitolimpact/gw_ziplist.aspx?ClientCode=capitolimpact&FIPS=06073&State=ca",
    "LAS VEGAS": "https://www.ciclt.net/sn/clt/capitolimpact/gw_ziplist.aspx?ClientCode=capitolimpact&FIPS=32003&State=nv",
}

# ---------- Helper ----------
def fetch_zip_table(url):
    tables = pd.read_html(url)

    best_df = None
    best_count = 0

    for t in tables:
        df = t.copy()
        df.columns = [str(c).upper() for c in df.columns]

        zip_col = next((c for c in df.columns if "ZIP" in c), None)
        city_col = next((c for c in df.columns if "CITY" in c), None)

        if not zip_col or not city_col:
            continue

        zips = df[zip_col].astype(str).str.extract(r"(\d{5})")[0]
        count = zips.notna().sum()

        if count > best_count:
            best_count = count
            best_df = df[[zip_col, city_col]].copy()
            best_df.columns = ["ZIP_CODE", "CITY"]

    if best_df is None:
        raise ValueError(f"No ZIP table found at {url}")

    best_df["ZIP_CODE"] = (
        best_df["ZIP_CODE"]
        .astype(str)
        .str.extract(r"(\d{5})")[0]
        .dropna()
    )

    best_df["CITY"] = best_df["CITY"].astype(str).str.upper().str.strip()

    best_df = best_df.dropna()
    best_df["ZIP_CODE"] = best_df["ZIP_CODE"].str.zfill(5)

    return best_df


# ---------- MAIN ----------
def main():
    frames = []

    # Ventura
    df = fetch_zip_table(URLS["VENTURA CO"])
    df["REGION"] = "VENTURA CO"
    frames.append(df)

    # LA
    df = fetch_zip_table(URLS["LA"])
    df["REGION"] = "LA"
    frames.append(df)

    # OC
    df = fetch_zip_table(URLS["OC"])
    df["REGION"] = "OC"
    frames.append(df)

    # IE/Riverside (Riverside + San Bernardino)
    df1 = fetch_zip_table(URLS["RIVERSIDE"])
    df2 = fetch_zip_table(URLS["SAN BERN"])
    df_ie = pd.concat([df1, df2], ignore_index=True)
    df_ie["REGION"] = "IE/RIVERSIDE"
    frames.append(df_ie)

    # San Diego
    df = fetch_zip_table(URLS["SAN DIEGO"])
    df["REGION"] = "SAN DIEGO"
    frames.append(df)

    # Las Vegas
    df = fetch_zip_table(URLS["LAS VEGAS"])
    df["REGION"] = "LAS VEGAS"
    frames.append(df)

    # Combine all
    out = pd.concat(frames, ignore_index=True)

    out = out[["ZIP_CODE", "REGION"]].drop_duplicates(subset=["ZIP_CODE"])
    out = out.sort_values(["REGION", "ZIP_CODE"]).reset_index(drop=True)

    print("TOTAL ROWS:", len(out))
    print(out.head())

    out.to_csv("Region_Map.csv", index=False)
    print("✅ Region_Map.csv created successfully")


if __name__ == "__main__":
    main()