#!/usr/bin/env python3
"""Summarize an enhance JSON-lines log without printing chat identities."""

import argparse
import json
from datetime import datetime
from pathlib import Path


def summarize(path):
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    terminal_status = {r["id"]: r["data"].get("status") for r in records if r.get("event") == "complete"}
    events = []
    for record in records:
        if "source" in record and "time" in record:
            events.append(record)
        elif isinstance(record.get("data"), dict) and "time" in record["data"]:
            events.append({**record["data"], "request_id": record["id"], "time": record.get("time", record["data"]["time"])})
    active = {}
    previous = None
    overlap = 0.0
    active_seconds = [0.0, 0.0]
    totals = [0, 0]
    sent = groups = failures = cancelled = 0
    peaks = [0, 0]
    for event in events:
        timestamp = datetime.fromisoformat(event['time'].replace('Z', '+00:00'))
        current = [sum(count[index] for count in active.values()) for index in range(2)]
        if previous is not None:
            seconds = max(0, (timestamp - previous).total_seconds())
            for index in range(2):
                if current[index]:
                    active_seconds[index] += seconds
            if all(current):
                overlap += seconds
        previous = timestamp
        if 'active_downloads' in event:
            key = (event.get('request_id'), event['source'], event['message'], event['target'], event.get('topic'))
            active[key] = [event['active_downloads'], event['active_uploads']]
        for index in range(2):
            peaks[index] = max(peaks[index], sum(count[index] for count in active.values()))
        if event['event'] == 'sent':
            sent += len(event['message_ids'])
            groups += 1
        if event['event'] == 'done':
            totals[0] += event['downloaded']
            totals[1] += event['uploaded']
            is_cancelled = event.get('status', terminal_status.get(event.get('request_id'))) == 'cancelled'
            cancelled += is_cancelled
            failures += bool(event.get('error')) and not is_cancelled
    elapsed = 0 if len(events) < 2 else (
        datetime.fromisoformat(events[-1]['time'].replace('Z', '+00:00'))
        - datetime.fromisoformat(events[0]['time'].replace('Z', '+00:00'))
    ).total_seconds()
    return {
        'elapsed_seconds': round(elapsed, 3),
        'overlap_seconds': round(overlap, 3),
        'download_active_seconds': round(active_seconds[0], 3),
        'upload_active_seconds': round(active_seconds[1], 3),
        'download_active_mib_per_second': round(totals[0] / active_seconds[0] / 1024**2, 3) if active_seconds[0] else 0,
        'upload_active_mib_per_second': round(totals[1] / active_seconds[1] / 1024**2, 3) if active_seconds[1] else 0,
        'peak_downloads': peaks[0],
        'peak_uploads': peaks[1],
        'downloaded_bytes': totals[0],
        'uploaded_bytes': totals[1],
        'sent_groups': groups,
        'sent_media': sent,
        'failed_groups': failures,
        'cancelled_groups': cancelled,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('log', type=Path)
    print(json.dumps(summarize(parser.parse_args().log), indent=2))
