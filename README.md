# @funtusov/heartbeat

Codex thread heartbeat CLI with local job tracking.

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

## Commands

### Start a background heartbeat

```bash
heartbeat start --thread-id 019c... --interval 15m --for 8h
```

### Show active/history jobs (table)

```bash
heartbeat status
```

Columns include:
- `STARTED`
- `EVERY`
- `THREAD`
- `RUNS` (times ran)
- `LAST RUN`
- `NEXT RUN`
- `LAST ACTION`

### Stop one/all jobs

```bash
heartbeat stop <job-id>
heartbeat stop --all
```

### View job logs

```bash
heartbeat logs <job-id>
heartbeat logs --tail 50
```

### Clear old non-running jobs

```bash
# Remove stopped/failed/exited/completed jobs and their logs
heartbeat clear

# Preview first
heartbeat clear --dry-run

# Only clear jobs older than 7 days
heartbeat clear --older-than 7d
```

### Foreground mode (legacy behavior)

```bash
heartbeat --interval 15m --until "tomorrow 7am"
# same as:
heartbeat run --interval 15m --until "tomorrow 7am"
```

## Follow-up prompt

Default prompt:

> Continue working on this task. Pick the next highest-impact step, execute it, and report concise progress before stopping.

Override with `--prompt` or `--prompt-file`.

## Notes

- Multiple concurrent heartbeat jobs are supported.
- Use `--yolo` with `run`/`start` to launch Codex as `codex --yolo app-server ...`.
- By default, the first cycle waits one full `--interval`; `--once` still runs immediately.
- Default local state path is `~/.heartbeat` (jobs + logs).
- `heartbeat start` does a preflight `thread/resume` check before daemonizing. If a thread id is stale/non-resumable, it fails immediately with remediation guidance.
- If `--thread-id` fails to resume, try `heartbeat run --once --dry-run` without `--thread-id` to auto-pick a resumable thread in your current `--cwd`.
- `heartbeat clear` only deletes non-running jobs; it does not stop running jobs.
