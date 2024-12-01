import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Function to scrape weather data
def scrape_weather_data():
    url = "https://metkhi.pmd.gov.pk/Max-Temp.php"
    
    # Mimic a browser with headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://metkhi.pmd.gov.pk/"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an HTTPError for bad responses
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

    # Parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table")
    if not table:
        print("Table not found on the webpage.")
        return None

    # Extract table rows
    rows = table.find_all("tr")[1:]  # Skip the header row
    data = []
    for row in rows:
        cols = row.find_all("td")

        # Ensure there are at least 3 columns (Station Code, Name, and Max Temp)
        if len(cols) >= 4:
            station_code = cols[1].get_text(strip=True)
            station_name = cols[2].get_text(strip=True)
            max_temp = cols[3].get_text(strip=True)

            # If max_temp is missing or invalid, set it to "(-)"
            if not max_temp or max_temp == "-":
                max_temp = "(-)"

            # Append the row data
            data.append([station_code, station_name, max_temp])
        else:
            print(f"Skipping malformed row: {row}")

    # Create a DataFrame with proper columns
    df = pd.DataFrame(data, columns=["Station Code", "Station Name", "Max Temperature"])
    df["Date"] = datetime.now().strftime("%Y-%m-%d")  # Add the current date
    return df

# Save data to Google Sheets
def save_to_google_sheets(df, sheet_name="Weather Data"):
    if df is None or df.empty:
        print("No data to save.")
        return

    # Define the scope for Google Sheets and Google Drive APIs
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Write credentials.json dynamically from environment variable
    credentials_path = "google-credentials.json"
    with open(credentials_path, "w") as f:
        f.write(os.environ["GOOGLE_CREDENTIALS"])  # Fetch from GitHub Secrets

    # Authenticate using the credentials JSON file
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(credentials)

    try:
        # Open the Google Sheet by name
        sheet = client.open(sheet_name)
    except gspread.SpreadsheetNotFound:
        # If the sheet doesn't exist, create a new one
        sheet = client.create(sheet_name)

    # Select or create a worksheet
    try:
        worksheet = sheet.worksheet("Daily Data")
    except gspread.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title="Daily Data", rows="1000", cols="100")

    # Append new data to the worksheet
    # Clear the existing worksheet to avoid duplicates
    worksheet.clear()
    worksheet.append_row(["Station Code", "Station Name", "Max Temperature", "Date"])  # Headers
    for row in df.values.tolist():
        worksheet.append_row(row)
    # After creating or opening the sheet
    # Share the Google Sheet with your emails
    sheet.share('irteza.nayani200@gmail.com', perm_type='user', role='writer')

    print(f"Data successfully saved to Google Sheet: {sheet_name}")

# Main execution
if __name__ == "__main__":
    weather_data = scrape_weather_data()
    save_to_google_sheets(weather_data)
