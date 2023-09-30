from datetime import datetime

import pandas as pd

# Sample data (replace this with your actual data)
data = pd.read_csv('D:\EPDE\Log\MRAPI_Error_09212023_00.csv',chunksize=1,skiprows=1)


# Create a dictionary to store the outage durations per store
outage_durations = {}

for entry in data:
    date_str, time_str = entry[0], entry[1]
    store_code = entry[2]

    # Combine date and time into a single datetime object
    timestamp = datetime.strptime(f"{date_str} {time_str}", "%m-%d-%Y %I:%M:%S %p")

    # Check if the store is already in the dictionary
    if store_code in outage_durations:
        # If the store is already in the dictionary, update the end time
        outage_durations[store_code]["end_time"] = timestamp
    else:
        # If the store is not in the dictionary, add it with start time
        outage_durations[store_code] = {"start_time": timestamp, "end_time": timestamp}

# Calculate the duration for each outage and accumulate it for each store
total_outage_duration = {}

for store_code, durations in outage_durations.items():
    start_time = durations["start_time"]
    end_time = durations["end_time"]
    duration = end_time - start_time

    # Add the duration to the total for this store
    if store_code in total_outage_duration:
        total_outage_duration[store_code] += duration
    else:
        total_outage_duration[store_code] = duration

# Print the total outage duration per store
for store_code, duration in total_outage_duration.items():
    print(f"Store: {store_code}, Total Outage Duration: {duration}")
