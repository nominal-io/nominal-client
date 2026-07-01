# Claude Code — Project Instructions

## Conversation Compaction

When the conversation is compacted, the summary **must preserve**:

### Commands run — full fidelity
Include every command with all flags, exactly as run. Do not paraphrase or abbreviate. If a command wrote output to a file, record the output path.

Example (good):
```
uv run python sync_channels_example.py saronic_delta_sync_channels.yml --log-file /Users/sean.reidy/local_code/download_all_assets_2026-06-15.log
```

Example (bad):
> "ran the download script"

### Artifacts produced — names and paths
Record every file written, its exact path, what it contains, and its version/iteration if applicable. This includes:
- Log files and where they are
- Config files and their final state (phase, which assets, etc.)
- JSON state files and their version number
- Any output directories and their sizes

### Configuration state — before and after
For any config modified during the session, record the key fields as they stood at end-of-session. Do not just say "updated the config" — show what changed.

### Errors and resolutions
For every error encountered, record:
- The exact error message or symptom
- Root cause
- The fix applied

### Decisions — include the why
For every non-obvious decision, record not just what was decided but why. The "why" is what rots fastest and is hardest to reconstruct from code.

### Migration / ops state
For data migrations or operational runs, always preserve:
- Which assets/resources were processed
- Aggregate counts (points streamed, files downloaded, runs created, etc.)
- State file paths and version numbers
- Any assets explicitly excluded and why

## Security / ops constraints

- `sync_channel_wrapper.py` lives in `nominal/experimental/migration/channel_sync/` and is **committable**.
- Any saronic migration configs are **local-only** — do not commit or push these files.
  - `nominal_internal_tools/resource_migration/configs/saronic_delta_sync_channels.yml`

- Lydian channel sync configs are **local-only** — do not commit or push these files.
  - `nominal_internal_tools/resource_migration/configs/lydian_sync_channels_FT_2Ch.yml`
  - `nominal_internal_tools/resource_migration/configs/lydian_sync_channels_FT_1Ch.yml`

## Session archives

At the end of any significant ops/migration session, write a structured archive to:
```
~/.claude/projects/-Users-sean-reidy-git-nominal-client/archives/<topic>_<YYYY-MM-DD>.md
```

Include: objective, all commands run, artifacts produced, decisions made and why, final state, open items.
