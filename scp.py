import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

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

# Save data to a CSV file
def save_to_csv(df, filename="weather_data.csv"):
    if df is None or df.empty:
        print("No data to save.")
        return

    # Check if the file already exists
    if not os.path.exists(filename):
        # If the file doesn't exist, initialize with Station Code, Station Name, and first date
        print("Initializing new CSV file.")
        df_for_today = df[["Station Code", "Station Name", "Max Temperature"]]
        today = datetime.now().strftime("%Y-%m-%d")
        df_for_today = df_for_today.rename(columns={"Max Temperature": today})
        df_for_today.to_csv(filename, index=False)
        print(f"File initialized with data for {today}")
        return

    # Read the existing data from the CSV
    existing_df = pd.read_csv(filename)

    # Check if today's date already exists in the CSV
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

    # Save the updated DataFrame back to the CSV
    updated_df.to_csv(filename, index=False)
    print(f"Data for {today} successfully saved to {filename}")

# Main execution
if __name__ == "__main__":
    weather_data = scrape_weather_data()
    save_to_csv(weather_data)
