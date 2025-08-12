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

# --- HELPERS ---
def sanitize_row(row):
    return [
        "" if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v
        for v in row
    ]

def parse_sheet_timestamps_to_epoch_ms(existing_rows):
    """
    Build a set of epoch-ms keys from the sheet's first column (Timestamp),
    tolerating mixed display formats like '8/7/2025 9:05:00' and '2025-08-07 09:05:00'.
    """
    if not existing_rows:
        return set()
    ts_strings = [r[0] for r in existing_rows if len(r) > 0 and r[0]]
    if not ts_strings:
        return set()
    s = pd.to_datetime(pd.Series(ts_strings), errors="coerce", infer_datetime_format=True)
    s = s.dropna()
    # Treat timestamps as US/Pacific local, then convert to UTC to match Highcharts epochs
    try:
        s = s.dt.tz_localize("US/Pacific", ambiguous="infer", nonexistent="NaT")
    except TypeError:
        # Older pandas without 'nonexistent' kw
        s = s.dt.tz_localize("US/Pacific", ambiguous="infer")
    s = s.dropna()
    s = s.dt.tz_convert("UTC")
    epoch_ms = (s.view("int64") // 1_000_000).astype(str)
    return set(epoch_ms.tolist())

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
                    x: (s.xData || []).slice(),   // ms since epoch (UTC)
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

        # Build Timestamp (Pacific, remove tz so Sheets parses uniformly)
        ts = (
            pd.to_datetime(xs, unit="ms", utc=True)
              .tz_convert("US/Pacific")
              .tz_localize(None)
        )
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")   # consistent parse in Sheets

        # DataFrame (keep EpochMs INTERNAL ONLY for de-dupe)
        df_full = pd.DataFrame({"Timestamp": ts_str})
        df_full.insert(1, "EpochMs", xs)  # internal key
        for s in series_list:
            df_full[s["name"]] = s["y"]

        # What we actually write (no EpochMs column)
        write_df = df_full.drop(columns=["EpochMs"])

        # Optional: log cadence
        deltas = pd.to_datetime(xs, unit="ms").to_series().diff().dt.total_seconds().div(60).dropna()
        if not deltas.empty:
            print(f"ℹ️ {sheet_title} {TARGET_DATE}: points={len(xs)}, median Δ={int(deltas.median())} min")

        try:
            sheet = spreadsheet.worksheet(sheet_title)
        except gspread.exceptions.WorksheetNotFound:
            rows_needed = max(300, len(write_df) + 1)
            cols_needed = max(10, len(write_df.columns))
            sheet = spreadsheet.add_worksheet(title=sheet_title, rows=str(rows_needed), cols=str(cols_needed))
            sheet = spreadsheet.worksheet(sheet_title)

        existing = sheet.get_all_values()

        if not existing:
            sanitized = [sanitize_row(row) for row in write_df.values.tolist()]
            all_rows = [write_df.columns.tolist()] + sanitized
            sheet.update("A1", all_rows, value_input_option="USER_ENTERED")
            print(f"✅ Sheet {sheet_title} was empty. Wrote full data for {TARGET_DATE}.")
        else:
            # Robust de-dupe: parse existing Timestamps -> epoch ms
            existing_epoch_keys = parse_sheet_timestamps_to_epoch_ms(existing[1:])
            # Keep only rows whose EpochMs aren't already present
            new_full_rows = [row for row in df_full.values.tolist() if str(row[1]) not in existing_epoch_keys]

            if new_full_rows:
                # Drop EpochMs before writing
                to_append = pd.DataFrame(new_full_rows, columns=df_full.columns).drop(columns=["EpochMs"])
                sanitized_new = [sanitize_row(r) for r in to_append.values.tolist()]
                sheet.append_rows(sanitized_new, value_input_option="USER_ENTERED")
                print(f"✅ Appended {len(sanitized_new)} new rows to {sheet_title} for {TARGET_DATE}.")
            else:
                print(f"⏭️ No new data to append to {sheet_title} for {TARGET_DATE}.")

print("\n✅ All eligible reports processed and data updated.")


