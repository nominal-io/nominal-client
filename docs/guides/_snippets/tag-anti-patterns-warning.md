:::{admonition} Avoid highly specific tags to maintain speed
:class: warning

Tags should have a few unique options compared to the total number of records; for example, only tens to hundreds of unique tag combinations to group data. Do not use tags for:
- **Continuous numeric values** like latitude, longitude, or measurements
- **Timestamps** or time-based identifiers
- **Unique IDs** like UUIDs or sequence numbers that change every record

Highly specific tags degrade query performance. If your data changes every point, it belongs in a **channel**, not a tag.

**Good tag examples:**
* `asset_id` or `motor_id`
*  `environment` (example: test, production, sim, or real)
* `test_stand_location_id`
:::
