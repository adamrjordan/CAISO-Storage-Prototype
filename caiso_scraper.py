import json
import os
import base64
import gspread
import pandas as pd
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# --- GOOGLE SHEETS AUTH ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = base64.b64decode(os.environ["GOOGLE_SHEETS_KEY_BASE64"]).decode("utf-8")
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
spreadsheet = client.open("CAISO Storage Chart Data")

# --- CONFIGURATION ---
TARGET_DATE = "2025-03-31"
WEB_URL = "https://www.caiso.com/documents/daily-energy-storage-report-" + datetime.strptime(TARGET_DATE, "%Y-%m-%d").strftime("%b-%d-%Y").lower() + ".html"

# --- SELENIUM SETUP ---
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# Install ChromeDriver and fix broken zip path issue
driver_dir = ChromeDriverManager().install()

# Walk the directory to find the real binary
for root, dirs, files in os.walk(driver_dir):
    for f in files:
        if f == "chromedriver":
            driver_path = os.path.join(root, f)
            break
    else:
        continue
    break

service = Service(executable_path=driver_path)
driver = webdriver.Chrome(service=service, options=options)


driver.get(WEB_URL)

chart_data = driver.execute_script("""
    if (Highcharts && Highcharts.charts[0]) {
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
    } else {
        return null;
    }
""")
driver.quit()

if not chart_data:
    raise RuntimeError("No Highcharts data found on the page.")

# --- WRITE EACH CHART TO ITS OWN TAB ---
for chart_index, chart in enumerate(chart_data):
    series_list = chart["series"]
    sheet_title = f"Chart_{chart_index + 1}"
    datetimes = pd.date_range(start=f"{TARGET_DATE} 00:00", freq="5min", periods=len(series_list[0]["data"]))
    df = pd.DataFrame({ "Timestamp": datetimes })

    for s in series_list:
        df[s["name"]] = [point["y"] for point in s["data"]]

    df["Timestamp"] = df["Timestamp"].astype(str)

    try:
        sheet = spreadsheet.worksheet(sheet_title)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_title, rows="300", cols="10")

    sheet.clear()
    sheet.append_rows([df.columns.tolist()] + df.values.tolist())

print("✅ Scraper completed and data written to Google Sheets.")

print("✅ All charts written to their respective Google Sheet tabs.")

