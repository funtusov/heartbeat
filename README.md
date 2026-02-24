# @funtusov/heartbeat

Codex thread heartbeat CLI.

It periodically checks a Codex thread via `codex app-server`, and when the latest turn is terminal (`completed`, `interrupted`, `failed`) it starts a follow-up turn with your continuation prompt.

## Install

```bash
npm i -g @funtusov/heartbeat
heartbeat --help
```

Or run without installing:

```bash
npx @funtusov/heartbeat --help
```

## Requirements

- `codex` CLI installed and authenticated (`codex login`)
- Node.js >= 18

## Typical usage

```bash
# Every 15m for 8h on latest thread in current cwd
heartbeat --interval 15m --for 8h

# Explicit thread, run until tomorrow 7am
heartbeat --thread-id 019c909b-94c5-75b3-9797-ab5b5983d4c6 \
  --interval 15m \
  --until "tomorrow 7am"

# One dry-run cycle (no turn start)
heartbeat --once --dry-run
```

## Follow-up prompt

Default prompt:

> Continue working on this task. Pick the next highest-impact step, execute it, and report concise progress before stopping.

Override with `--prompt` or `--prompt-file`.

## Options

```text
--thread-id <id>
--cwd <path>
--source-kind <kind>      (repeatable)
--interval <dur>          e.g. 30s, 15m, 2h, 1d
--for <dur>
--until <time>            ISO, HH:MM, or "tomorrow 7am"
--prompt <text>
--prompt-file <path>
--dry-run
--once
--max-cycles <n>
--start-if-empty
--codex-bin <path>
--experimental-api
-h, --help
-v, --version
```
