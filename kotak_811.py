import os
import argparse
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import pygsheets
import json

# ======================================================
# CONFIG
# ======================================================

BASE_URL = "https://hq1.appsflyer.com/api/raw-data/export/app/com.kotak811mobilebankingapp.instantsavingsupiscanandpayrecharge"

SPREADSHEET_ID = "1ABrtV_PrFGn7Bue0wzK62LlBpN05dlsuzngGqszNNpo"
SHEET_NAME = "Automate"

# ======================================================
# AUTH
# ======================================================

# 🔐 AppsFlyer Token from GitHub Secret
AFTOKEN = os.getenv("AFTOKEN")

if not AFTOKEN:
    raise Exception("❌ Missing TOKEN environment variable in GitHub Secrets!")

HEADERS = {
    "accept": "text/csv",
    "authorization": f"Bearer {AFTOKEN}"
}

# 🔐 Google Service Account JSON from GitHub Secret
service_json = "/tmp/service_account.json"
with open(service_json, "w") as f:
    f.write(os.getenv("GOOGLE_SERVICE_JSON"))

gc = pygsheets.authorize(service_file=service_json)
sh = gc.open_by_key(SPREADSHEET_ID)
wks = sh.worksheet_by_title(SHEET_NAME)

# ======================================================
# FETCH FUNCTION
# ======================================================

def fetch_data(endpoint: str, start: str, end: str):
    url = f"{BASE_URL}/{endpoint}/v5?from={start}&to={end}&timezone=Asia%2FKolkata"
    print(f"📡 Fetching: {url}")

    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        df = pd.read_csv(StringIO(response.text))
        print(f"✅ Received {len(df)} rows for {endpoint}")
        return df
    else:
        print(f"❌ Error {response.status_code}: {response.text}")
        return pd.DataFrame()

# ======================================================
# PROCESS DATA
# ======================================================

def process_installs(df):
    if df.empty:
        print("⚠️ Installs empty!")
        return pd.DataFrame()

    df["Event Time"] = pd.to_datetime(df["Event Time"])
    df["Event Date"] = df["Event Time"].dt.date

    pivot = df.pivot_table(
        index=["Event Date", "Campaign", "State", "Adset"],
        columns="Event Name",
        aggfunc="size",
        fill_value=0
    )

    return pivot.reset_index()


def process_inapp(df):
    if df.empty:
        print("⚠️ In-App events empty!")
        return pd.DataFrame()

    df["Event Time"] = pd.to_datetime(df["Event Time"])
    df["Event Date"] = df["Event Time"].dt.date

    pivot = df.pivot_table(
        index=["Event Date", "Campaign", "State", "Adset"],
        columns="Event Name",
        aggfunc="size",
        fill_value=0
    )

    return pivot.reset_index()


# ======================================================
# MAIN
# ======================================================

def main(start_date, end_date):

    installs = fetch_data("installs_report", start_date, end_date)
    inapp = fetch_data("in-app-events-postbacks", start_date, end_date)

    pivot_install = process_installs(installs)
    pivot_inapp = process_inapp(inapp)

    merged = pd.merge(
        pivot_install,
        pivot_inapp,
        on=["Event Date", "Campaign", "State", "Adset"],
        how="outer"
    ).fillna(0)

    print("🧹 Clearing sheet...")
    wks.clear(start="A1")

    print("📤 Writing pivot_install to A3")
    wks.set_dataframe(pivot_install, start=(3, 1))

    print("📤 Writing pivot_inapp to G3")
    wks.set_dataframe(pivot_inapp, start=(3, 7))

    print("📤 Writing merged pivot to R3")
    wks.set_dataframe(merged, start=(3, 18))

    print("🎉 ALL DONE!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    main(args.start, args.end)

