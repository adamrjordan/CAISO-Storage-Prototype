import json
import os
import sys
import base64
import time
import gspread
import pandas as pd
from datetime import datetime, timedelta, date
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# --- GOOGLE SHEETS AUTH ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = base64.b64decode(os.environ["GOOGLE_SHEETS_KEY_BASE64"]).decode("utf-8")
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
spreadsheet = client.open("CAISO Storage Chart Data")

# --- BACKFILL CONFIG ---
START_DATE = date(2023, 1, 1)
END_DATE = date(2025, 4, 26)

# --- CHROME OPTIONS ---
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# --- LOOP THROUGH EACH DATE ---
for TARGET_DATE in [START_DATE + timedelta(days=n) for n in range((END_DATE - START_DATE).days + 1)]:
    print(f"\n📅 Processing: {TARGET_DATE}")
    WEB_URL = f"https://www.caiso.com/documents/daily-energy-storage-report-{TARGET_DATE.strftime('%b-%d-%Y').lower()}.html"

    try:
        driver_dir = ChromeDriverManager().install()
        driver_path = os.path.join(os.path.dirname(driver_dir), "chromedriver")
        if not os.path.isfile(driver_path):
            raise FileNotFoundError(f"Expected chromedriver binary not found at: {driver_path}")
        os.chmod(driver_path, 0o755)
        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(WEB_URL)

        # Wait for Highcharts to load
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
            driver.quit()
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
        driver.quit()

        if not chart_data:
            print("❌ No chart data found.")
            continue

        for chart_index, chart in enumerate(chart_data):
            series_list = chart["series"]
            sheet_title = f"Chart_{chart_index + 1}"
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
                    
        time.sleep(20)  # Pause 20 seconds after successfully processing one day
        
    except Exception as e:
        print(f"❌ Failed to process {TARGET_DATE}: {e}")
        time.sleep(20)  # Also sleep after failure, just to be gentle on API
