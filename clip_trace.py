import json
import sys
import argparse
from pathlib import Path

def slice_trace(input_path: str, output_path: str, seconds: float):
    print(f"Loading {input_path}...")
    try:
        with open(input_path, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        print("Error: Invalid JSON file.")
        return

    events = data.get("traceEvents", [])
    if not events:
        print("No trace events found.")
        return

    # 1. Identify Time Range
    # VizTracer/Chrome Trace timestamps ('ts') are in microseconds
    valid_events = [e for e in events if "ts" in e]
    if not valid_events:
        print("No timestamped events found.")
        return

    max_ts = max(e["ts"] for e in valid_events)
    min_cutoff = max_ts - (seconds * 1_000_000)

    print(f"Total Events: {len(events)}")
    print(f"End Time: {max_ts}")
    print(f"Cutoff:   {min_cutoff} ({seconds} seconds lookback)")

    # 2. Filter Events
    # We keep all metadata events (no 'ts') + events within the window
    filtered_events = []
    for e in events:
        if "ts" not in e:
            filtered_events.append(e)  # Always keep metadata (process names, thread names)
        elif e["ts"] >= min_cutoff:
            filtered_events.append(e)

    data["traceEvents"] = filtered_events
    
    print(f"Remaining Events: {len(filtered_events)}")

    # 3. Save
    with open(output_path, "w") as f:
        json.dump(data, f)
    
    print(f"Successfully saved clipped trace to: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clip the tail end of a VizTracer JSON file.")
    parser.add_argument("input_file", help="Path to the large trace file")
    parser.add_argument("--out", default="clipped_trace.json", help="Output filename")
    parser.add_argument("--seconds", type=int, default=120, help="Seconds from the end to keep")
    
    args = parser.parse_args()
    slice_trace(args.input_file, args.out, args.seconds)