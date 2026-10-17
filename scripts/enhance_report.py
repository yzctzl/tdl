#!/usr/bin/env python3
"""Summarize an enhance JSON-lines log without printing chat identities."""

import argparse
import json
from datetime import datetime
from pathlib import Path


def summarize(path):
    events = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    active = {}
    previous = None
    overlap = 0.0
    totals = [0, 0]
    sent = groups = failures = 0
    peaks = [0, 0]
    for event in events:
        timestamp = datetime.fromisoformat(event['time'].replace('Z', '+00:00'))
        current = [sum(count[index] for count in active.values()) for index in range(2)]
        if previous is not None and all(current):
            overlap += max(0, (timestamp - previous).total_seconds())
        previous = timestamp
        if 'active_downloads' in event:
            key = (event['source'], event['message'], event['target'], event.get('topic'))
            active[key] = [event['active_downloads'], event['active_uploads']]
        for index in range(2):
            peaks[index] = max(peaks[index], sum(count[index] for count in active.values()))
        if event['event'] == 'sent':
            sent += len(event['message_ids'])
            groups += 1
        if event['event'] == 'done':
            totals[0] += event['downloaded']
            totals[1] += event['uploaded']
            failures += bool(event.get('error'))
    elapsed = 0 if len(events) < 2 else (
        datetime.fromisoformat(events[-1]['time'].replace('Z', '+00:00'))
        - datetime.fromisoformat(events[0]['time'].replace('Z', '+00:00'))
    ).total_seconds()
    return {
        'elapsed_seconds': round(elapsed, 3),
        'overlap_seconds': round(overlap, 3),
        'peak_downloads': peaks[0],
        'peak_uploads': peaks[1],
        'downloaded_bytes': totals[0],
        'uploaded_bytes': totals[1],
        'sent_groups': groups,
        'sent_media': sent,
        'failed_groups': failures,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('log', type=Path)
    print(json.dumps(summarize(parser.parse_args().log), indent=2))
