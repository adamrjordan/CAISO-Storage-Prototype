
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURATION ---
TARGET_DATE = "2025-03-31"
WEB_URL = "https://www.caiso.com/documents/daily-energy-storage-report-" + datetime.strptime(TARGET_DATE, "%Y-%m-%d").strftime("%b-%d-%Y").lower() + ".html"
CHROMEDRIVER_PATH = "C:\\chromedriver-win64\\chromedriver.exe"
GOOGLE_CREDS_FILE = "google_creds.json"
GOOGLE_SHEET_NAME = "CAISO Storage Chart Data"

# --- SELENIUM SETUP ---
service = Service(CHROMEDRIVER_PATH)
options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(service=service, options=options)

# --- LOAD PAGE & EXTRACT CHART DATA ---
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

# --- GOOGLE SHEETS AUTH ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
client = gspread.authorize(creds)
spreadsheet = client.open(GOOGLE_SHEET_NAME)

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

print("✅ All charts written to their respective Google Sheet tabs.")

