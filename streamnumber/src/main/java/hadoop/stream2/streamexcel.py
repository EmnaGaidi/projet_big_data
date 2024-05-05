import os
import subprocess
import pandas as pd
import time
import random

# Start the nc command
process = subprocess.Popen(["ncat", "-lk", "9999"], stdin=subprocess.PIPE)

excel_file_path = os.path.join(
    "..", "..", "..", "resources", "input", "Spotify_cleaned.xlsx"
)
# Open the Excel file
df = pd.read_excel(excel_file_path)

# Shuffle the DataFrame rows
df = df.sample(frac=1).reset_index(drop=True)

for index, row in df.iterrows():
    # Convert the row to a string and add a newline character
    data = ",".join(map(str, row.values)) + "\n"

    # Send the data
    process.stdin.write(data.encode())
    process.stdin.flush()
    print(f"Sent: {data.strip()}")
    time.sleep(1)

# Close the stdin pipe
process.stdin.close()

# Wait for the nc command to terminate
process.wait()
