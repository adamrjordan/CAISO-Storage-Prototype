import json
import os
import sys
import base64
import time
import argparse
import gspread
import pandas as pd
from datetime import datetime, timedelta, date
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- ARGUMENT PARSING ---
parser = argparse.ArgumentParser()
parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD")
parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD")
args = parser.parse_args()

START_DATE = datetime.strptime(args.start, "%Y-%m-%d").date()
END_DATE = datetime.strptime(args.end, "%Y-%m-%d").date()

# --- GOOGLE SHEETS AUTH ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = base64.b64decode(os.environ["GOOGLE_SHEETS_KEY_BASE64"]).decode("utf-8")
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
spreadsheet = client.open("CAISO Storage Chart Data")

# --- CHROME SETUP (ONCE) ---
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver_dir = ChromeDriverManager().install()
driver_path = os.path.join(os.path.dirname(driver_dir), "chromedriver")
if not os.path.isfile(driver_path):
    raise FileNotFoundError(f"Expected chromedriver binary not found at: {driver_path}")
os.chmod(driver_path, 0o755)

# --- LOOP THROUGH EACH DATE ---
for TARGET_DATE in [START_DATE + timedelta(days=n) for n in range((END_DATE - START_DATE).days + 1)]:
    print(f"\n📅 Processing: {TARGET_DATE}")
    WEB_URL = f"https://www.caiso.com/documents/daily-energy-storage-report-{TARGET_DATE.strftime('%b-%d-%Y').lower()}.html"

    driver = None
    try:
        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(WEB_URL)

        if "404" in driver.title.lower() or "page not found" in driver.page_source.lower():
            print(f"❌ Report not found for {TARGET_DATE} — 404 page.")
            continue

        for _ in range(10):
            try:
                is_ready = driver.execute_script("return typeof Highcharts !== 'undefined' && Highcharts.charts[0] !== undefined;")
                if is_ready:
                    break
            except:
                pass
            time.sleep(1)
        else:
            print("❌ Highcharts not found. Skipping.")
            continue

        chart_data = driver.execute_script("""
            return Highcharts.charts.map(function(chart) {
                return {
                    title: chart.title ? chart.title.textStr : null,
                    series: chart.series.map(function(s) {
                        return {
                            name: s.name,
                            data: s.data.map(function(d) {
                                return { x: d.x, y: d.y };
                            })
                        };
                    })
                };
            });
        """)

        if not chart_data:
            print("❌ No chart data found.")
            continue

        for chart_index, chart in enumerate(chart_data):
            series_list = chart["series"]
            sheet_title = f"Chart_{chart_index + 1}"
            if not series_list or not series_list[0]["data"]:
                print(f"⚠️ Chart {sheet_title} had no data. Skipping.")
                continue

            datetimes = pd.date_range(start=f"{TARGET_DATE} 00:00", freq="5min", periods=len(series_list[0]["data"]))
            df = pd.DataFrame({"Timestamp": datetimes})
            for s in series_list:
                df[s["name"]] = [point["y"] for point in s["data"]]
            df["Timestamp"] = df["Timestamp"].astype(str)

            try:
                sheet = spreadsheet.worksheet(sheet_title)
            except gspread.exceptions.WorksheetNotFound:
                sheet = spreadsheet.add_worksheet(title=sheet_title, rows="300", cols="10")

            existing = sheet.get_all_values()
            if not existing:
                sheet.append_rows([df.columns.tolist()] + df.values.tolist())
                print(f"✅ Created new sheet: {sheet_title}")
            else:
                existing_timestamps = {row[0] for row in existing[1:]}
                new_rows = [row for row in df.values.tolist() if row[0] not in existing_timestamps]
                if new_rows:
                    sheet.append_rows(new_rows)
                    print(f"✅ Appended {len(new_rows)} new rows to {sheet_title}.")
                else:
                    print(f"⏭️ No new data to append to {sheet_title} — already exists.")

        time.sleep(20)

    except Exception as e:
        print(f"❌ Failed to process {TARGET_DATE}: {e}")
        time.sleep(20)
    finally:
        if driver:
            driver.quit()
