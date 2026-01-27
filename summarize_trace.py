import json
import argparse
from collections import defaultdict


def summarize_trace(trace_path, top_n=50):
    """
    Parses a VizTracer JSON file and prints a summary of function execution times.
    """
    print(f"Loading trace: {trace_path}...")
    try:
        with open(trace_path, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading trace: {e}")
        return

    events = data.get("traceEvents", [])
    if not events:
        print("No events found in trace.")
        return

    # Aggregation dictionary: name -> [count, total_duration]
    stats = defaultdict(lambda: [0, 0.0])

    for event in events:
        # We are interested in 'X' (Complete) events which have a duration 'dur'
        # 'dur' is usually in microseconds
        if event.get("ph") == "X" and "name" in event and "dur" in event:
            name = event["name"]
            duration = event["dur"]

            stats[name][0] += 1
            stats[name][1] += duration

    # Convert to list for sorting
    # Item format: (name, count, total_duration_ms, avg_duration_ms)
    summary_list = []
    for name, (count, total_dur_us) in stats.items():
        total_ms = total_dur_us / 1000.0
        avg_ms = total_ms / count if count > 0 else 0
        summary_list.append((name, count, total_ms, avg_ms))

    # Sort by Total Duration (descending)
    summary_list.sort(key=lambda x: x[2], reverse=True)

    print(f"\n--- Trace Summary (Top {top_n} by Duration) ---\n")
    print(
        f"{'Function Name':<50} | {'Count':<8} | {'Total (ms)':<12} | {'Avg (ms)':<10}"
    )
    print("-" * 90)

    for item in summary_list[:top_n]:
        name, count, total, avg = item
        # Truncate very long names
        display_name = (name[:47] + "..") if len(name) > 47 else name
        print(f"{display_name:<50} | {count:<8} | {total:<12.2f} | {avg:<10.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize a VizTracer JSON file.")
    parser.add_argument("trace_file", help="Path to the trace.json file")
    parser.add_argument(
        "--top", type=int, default=30, help="Number of top functions to show"
    )

    args = parser.parse_args()
    summarize_trace(args.trace_file, args.top)
