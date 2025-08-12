import json
import os
import base64
import gspread
import pandas as pd
import math
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import sys
from selenium.webdriver.support.ui import WebDriverWait

# --- GOOGLE SHEETS AUTH ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = base64.b64decode(os.environ["GOOGLE_SHEETS_KEY_BASE64"]).decode("utf-8")
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
spreadsheet = client.open("CAISO Storage Chart Data")

# --- SELENIUM SETUP ---
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver_dir = ChromeDriverManager().install()
driver_path = os.path.join(os.path.dirname(driver_dir), "chromedriver")
if not os.path.isfile(driver_path):
    print(f"❌ Expected chromedriver binary not found at: {driver_path}", file=sys.stderr)
    sys.exit(1)
os.chmod(driver_path, 0o755)
service = Service(executable_path=driver_path)

# --- HELPER TO SANITIZE NON-JSON COMPLIANT FLOATS ---
def sanitize_row(row):
    return [
        "" if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v
        for v in row
    ]

# --- LOOP OVER MULTIPLE DATES ---
for offset in [2, 3, 4, 5]:
    TARGET_DATE = (datetime.utcnow() - timedelta(days=offset)).date()
    WEB_URL = f"https://www.caiso.com/documents/daily-energy-storage-report-{TARGET_DATE.strftime('%b-%d-%Y').lower()}.html"

    print(f"🔍 Attempting to scrape report for {TARGET_DATE}...")

    driver = webdriver.Chrome(service=service, options=options)
    driver.get(WEB_URL)

    if "404" in driver.title.lower() or "page not found" in driver.page_source.lower():
        print(f"❌ No report found for {TARGET_DATE}. Skipping.")
        driver.quit()
        continue

    try:
        WebDriverWait(driver, 30).until(
            lambda d: d.execute_script("return typeof Highcharts !== 'undefined' && Highcharts.charts.length > 0")
        )
        # Pull raw xData/yData for all points (not just visible) for each series
        chart_data = driver.execute_script("""
          if (Highcharts && Highcharts.charts[0]) {
            return Highcharts.charts.map(function(chart) {
              return {
                title: chart.title ? chart.title.textStr : null,
                series: chart.series.map(function(s) {
                  return {
                    name: s.name,
                    x: (s.xData || []).slice(),   // ms since epoch
                    y: (s.yData || []).slice()
                  };
                })
              };
            });
          } else {
            return null;
          }
        """)
    finally:
        driver.quit()

    if not chart_data:
        print(f"⚠️ No Highcharts data found for {TARGET_DATE}.")
        continue

    # --- WRITE EACH CHART TO ITS OWN TAB ---
    for chart_index, chart in enumerate(chart_data):
        series_list = chart["series"]
        sheet_title = f"Chart_{chart_index + 1}"

        # Use the first series' x-values as the master timeline
        xs = series_list[0]["x"] if series_list and "x" in series_list[0] else []
        if not xs:
            print(f"⚠️ No data points in {sheet_title} for {TARGET_DATE}.")
            continue

        # Convert ms epoch -> US/Pacific timestamps (strings for Sheets)
        ts = pd.to_datetime(xs, unit="ms", utc=True).tz_convert("US/Pacific").astype(str)
        df = pd.DataFrame({"Timestamp": ts})

        # Attach each series' y-values (aligned by Highcharts)
        for s in series_list:
            df[s["name"]] = s["y"]

        # Optional: quick cadence info for logs
        deltas = pd.to_datetime(xs, unit="ms").to_series().diff().dt.total_seconds().div(60).dropna()
        if not deltas.empty:
            print(f"ℹ️ {sheet_title} {TARGET_DATE}: points={len(xs)}, median Δ={int(deltas.median())} min")

        try:
            sheet = spreadsheet.worksheet(sheet_title)
        except gspread.exceptions.WorksheetNotFound:
            # Pre-size with enough rows/cols to fit the first write
            rows_needed = max(300, len(df) + 1)
            cols_needed = max(10, len(df.columns))
            sheet = spreadsheet.add_worksheet(title=sheet_title, rows=str(rows_needed), cols=str(cols_needed))
            sheet = spreadsheet.worksheet(sheet_title)

        existing = sheet.get_all_values()

        if not existing:
            sanitized = [sanitize_row(row) for row in df.values.tolist()]
            all_rows = [df.columns.tolist()] + sanitized
            sheet.update("A1", all_rows, value_input_option="USER_ENTERED")
            print(f"✅ Sheet {sheet_title} was empty. Wrote full data for {TARGET_DATE}.")
        else:
            existing_timestamps = {row[0] for row in existing[1:]}
            new_rows = [row for row in df.values.tolist() if row[0] not in existing_timestamps]
            if new_rows:
                sanitized_new = [sanitize_row(row) for row in new_rows]
                sheet.append_rows(sanitized_new, value_input_option="USER_ENTERED")
                print(f"✅ Appended {len(sanitized_new)} new rows to {sheet_title} for {TARGET_DATE}.")
            else:
                print(f"⏭️ No new data to append to {sheet_title} for {TARGET_DATE}.")

print("\n✅ All eligible reports processed and data updated.")

