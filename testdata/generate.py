#!/usr/bin/env python3
"""Regenerate deterministic OpenMetrics fixture; timestamps are Unix seconds."""
from datetime import datetime, timezone
from pathlib import Path

START = int(datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp())
lines = []
for metric in ("synthetic_load", "synthetic_gap", "synthetic_late", "synthetic_tail"):
    lines.append(f"# TYPE {metric} gauge")
    for minute in range(61):
        if metric == "synthetic_gap" and 5 <= minute < 11:
            continue
        if metric == "synthetic_late" and minute < 12:
            continue
        high = {
            "synthetic_load": 2 <= minute < 5 or 10 <= minute < 17 or 25 <= minute < 45,
            "synthetic_gap": 2 <= minute < 20,
            "synthetic_late": 12 <= minute < 25,
            "synthetic_tail": minute >= 50,
        }[metric]
        lines.append(f'{metric}{{instance="fixture"}} {10 if high else 0} {START + minute * 60}')
lines.append("# TYPE synthetic_requests counter")
value = 0
for minute in range(61):
    value += 600 if 10 <= minute < 20 else 60
    lines.append(f'synthetic_requests_total{{instance="fixture"}} {value} {START + minute * 60}')
lines.append("# EOF")
Path(__file__).with_name("synthetic.om").write_text("\n".join(lines) + "\n")
