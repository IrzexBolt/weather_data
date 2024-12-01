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

        # Ensure there are at least 4 columns (Station Code, Name, and Max Temp)
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

    # Fetch existing data from the Google Sheet
    existing_data = worksheet.get_all_values()

    # If the sheet is empty, write the static station data
    if not existing_data:
        # Write the station codes and names
        worksheet.append_row(["Station Code", "Station Name"])  # Headers for static data
        for _, row in df[["Station Code", "Station Name"]].drop_duplicates().iterrows():
            worksheet.append_row(row.tolist())
        # Add the first date column and max temperature data
        worksheet.update_cell(1, 3, df["Date"][0])  # Set the date as the first header
        for i, max_temp in enumerate(df["Max Temperature"], start=2):
            worksheet.update_cell(i, 3, max_temp)
        print(f"Initialized the sheet with station data and date: {df['Date'][0]}")
        return

    # Load existing data into a DataFrame
    headers = existing_data[0]  # First row is assumed to be headers
    existing_df = pd.DataFrame(existing_data[1:], columns=headers)

    # Check if today's date is already present
    today = datetime.now().strftime("%Y-%m-%d")
    if today in existing_df.columns:
        print(f"Data for {today} already exists. Skipping update.")
        return

    # Prepare the new data for today's date
    df_for_today = df[["Station Code", "Max Temperature"]].set_index("Station Code")
    df_for_today = df_for_today.rename(columns={"Max Temperature": today})

    # Merge the new data into the existing data
    existing_df = existing_df.set_index("Station Code")
    updated_df = existing_df.join(df_for_today, how="left").reset_index()

    # Update the worksheet with the new data
    worksheet.clear()
    worksheet.append_row(updated_df.columns.tolist())  # Write headers
    for row in updated_df.itertuples(index=False):
        worksheet.append_row(row)

    print(f"Added data for date: {today}")

# Main execution
if __name__ == "__main__":
    weather_data = scrape_weather_data()
    save_to_google_sheets(weather_data)
