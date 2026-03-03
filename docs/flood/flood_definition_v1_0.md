# Flood Definition v1.0 (D) — ypf_alarms

## Context
This project detects alarm floods in OTMS (`dbo.ypf_alarms`) and classifies them into actionable types for production use.

## Unit of analysis
A **block** is a sequence of alarms ordered by `ALARMDATETIME` where a new block starts if the time gap between consecutive alarms is **> 5 minutes**.

## Features per block
- `start`, `end`
- `n`: number of alarms
- `duration_min`
- `max_rate`: maximum alarms/min within the block
- `unique_tags`: # unique `TAG_DESCRIPTION`
- `unique_msgs`: # unique `ALARM_DESCRIPTION` (NULL treated as `"__NULL__"`)
- `dominant_tag_share`: top tag count / `n`
- `dominant_msg_share`: top message count / `n`
- `prio1_share`: share of alarms with `PRIORITY = 1`

## Severity thresholds (baseline)
Based on historical percentiles:
- p99 ≈ 180 alarms/min
- p99.9 ≈ 345 alarms/min

Rules:
- `severe` if `max_rate >= 345`
- `medium` if `180 <= max_rate < 345`
- `none` if `max_rate < 180`

## Flood candidate (D)
A block is a `flood_candidate` if:
- `max_rate >= 180`
AND
- (`dominant_msg_share >= 0.70` OR `dominant_tag_share >= 0.85`)
AND
- `n >= 50`

## Flood types (v1.0)
Precedence order:
1) INFRASTRUCTURE_EVENT
2) CHATTERING_POINT
3) LOCAL_PROCESS_INSTABILITY
4) SUBSYSTEM_TRIP_EVENT
5) OTHER_OR_NO_FLOOD

### 1) INFRASTRUCTURE_EVENT
- `dominant_msg_share >= 0.70`
- `unique_tags >= 200`
- `severity in {medium, severe}`
Optional: dominant message contains infra tokens (ENABLED, COMM, NETWORK, I/O, RESET, SERVER, etc.)

Action (phase 1):
- notify + auto-incident if `severe` and `prio1_share >= 0.7`

### 2) CHATTERING_POINT
- `dominant_tag_share >= 0.95`
- `severity in {medium, severe}`

Action:
- notify + prioritize
- UI rate-limit/grouping
- suggest instrument/control review

### 3) LOCAL_PROCESS_INSTABILITY
- `unique_tags <= 15`
- `dominant_msg_share >= 0.70` (includes NULL dominance)
- `dominant_tag_share < 0.85`
- `severity in {medium, severe}`

Action:
- notify + prioritize
- auto-incident only if persistent/recurrent (policy to be defined)

### 4) SUBSYSTEM_TRIP_EVENT
- `unique_tags >= 50`
- `dominant_msg_share >= 0.50` (includes NULL dominance)
- `prio1_share >= 0.7`
- `severity in {medium, severe}`

Action:
- notify + auto-incident if severe
- do not suppress (real operational event)

### 5) OTHER_OR_NO_FLOOD
Everything else.

## Notes
- NULL `ALARM_DESCRIPTION` must be normalized as `"__NULL__"` for dominance metrics.
- Thresholds for `unique_tags` will be recalibrated using distribution plots per flood_type.