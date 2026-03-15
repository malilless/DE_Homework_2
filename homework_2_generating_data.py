import json
import random
from datetime import datetime, timedelta
import os

# --- Configuration ---
num_calls = 50
start_call_id = 1
output_dir = "call_records"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Example short descriptions (pretend LLM-generated)
descriptions = [
    "Discussed project timeline.",
    "Followed up on previous issue.",
    "Customer requested more info.",
    "Resolved technical problem.",
    "Scheduled next meeting.",
    "Provided product demo.",
    "Answered billing question.",
    "Escalated issue to manager.",
    "Checked system status.",
    "Gave feedback on report."
]

for call_id in range(start_call_id, start_call_id + num_calls + 1):
    
    duration_sec = random.randint(30, 600)  # call duration 0.5-10 minutes
    short_description = random.choice(descriptions)

    # Create a call record
    call_record = {
        "call_id": call_id,
        "duration_sec": duration_sec,
        "short_description": short_description
    }
    
    # Write it into a JSON file
    with open(os.path.join(output_dir, f"{call_id}.json"), "w") as output_file:
        json.dump(call_record, output_file, indent=2)

print(f"Generated call records for {num_calls} calls in '{output_dir}' folder.")