#!/usr/bin/env node

import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { createInterface } from "node:readline";
import process from "node:process";

const PACKAGE_JSON = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
);
const VERSION = PACKAGE_JSON.version;

const TERMINAL_TURN_STATUSES = new Set(["completed", "interrupted", "failed"]);
const SOURCE_KINDS = new Set([
  "cli",
  "vscode",
  "exec",
  "appServer",
  "subAgent",
  "subAgentReview",
  "subAgentCompact",
  "subAgentThreadSpawn",
  "subAgentOther",
  "unknown",
]);

const DEFAULT_PROMPT =
  "Continue working on this task. Pick the next highest-impact step, execute it, and report concise progress before stopping.";

function timestamp() {
  return new Date().toISOString();
}

function log(message) {
  console.log(`[${timestamp()}] ${message}`);
}

function fail(message, code = 1) {
  console.error(`heartbeat: ${message}`);
  process.exit(code);
}

function printHelp() {
  console.log(`heartbeat v${VERSION}

Periodic Codex thread heartbeat via codex app-server JSON-RPC.
If the last turn is terminal (completed/interrupted/failed), heartbeat starts a follow-up turn.

Usage:
  heartbeat [options]
  npx @funtusov/heartbeat [options]

Options:
  --thread-id <id>          Target thread ID (if omitted, pick latest matching thread)
  --cwd <path>              CWD filter for auto-thread selection (default: current dir)
  --source-kind <kind>      Optional source kind filter (repeatable)
  --interval <dur>          Poll interval (e.g. 30s, 15m, 2h) (default: 15m)
  --for <dur>               Run for this duration (e.g. 8h)
  --until <time>            Stop at this time (ISO, HH:MM, or 'tomorrow 7am')
  --prompt <text>           Follow-up prompt text
  --prompt-file <path>      Read follow-up prompt from file (overrides --prompt)
  --dry-run                 Never start turns, only print what would happen
  --once                    Execute one cycle and exit
  --max-cycles <n>          Exit after n cycles
  --start-if-empty          Start a turn if thread currently has no turns
  --codex-bin <path>        Codex executable path (default: codex)
  --experimental-api        Set initialize.capabilities.experimentalApi=true
  -h, --help                Show help
  -v, --version             Show version

Source kinds:
  cli, vscode, exec, appServer, subAgent, subAgentReview,
  subAgentCompact, subAgentThreadSpawn, subAgentOther, unknown

Examples:
  heartbeat --interval 15m --for 8h
  heartbeat --thread-id 019c... --interval 15m --until "tomorrow 7am"
  heartbeat --once --dry-run
`);
}

function splitOption(token) {
  const eq = token.indexOf("=");
  if (eq === -1) {
    return [token, null];
  }
  return [token.slice(0, eq), token.slice(eq + 1)];
}

function parseArgs(argv) {
  const cfg = {
    threadId: null,
    cwd: process.cwd(),
    sourceKinds: [],
    interval: "15m",
    runFor: null,
    until: null,
    prompt: DEFAULT_PROMPT,
    promptFile: null,
    dryRun: false,
    once: false,
    maxCycles: null,
    startIfEmpty: false,
    codexBin: "codex",
    experimentalApi: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const raw = argv[i];

    if (raw === "-h" || raw === "--help") {
      cfg.help = true;
      continue;
    }
    if (raw === "-v" || raw === "--version") {
      cfg.version = true;
      continue;
    }

    const [key, inlineValue] = splitOption(raw);

    const requireValue = () => {
      if (inlineValue !== null) {
        return inlineValue;
      }
      const next = argv[i + 1];
      if (!next || next.startsWith("-")) {
        throw new Error(`Missing value for ${key}`);
      }
      i += 1;
      return next;
    };

    switch (key) {
      case "--thread-id":
        cfg.threadId = requireValue();
        break;
      case "--cwd":
        cfg.cwd = requireValue();
        break;
      case "--source-kind": {
        const value = requireValue();
        if (!SOURCE_KINDS.has(value)) {
          throw new Error(`Invalid --source-kind '${value}'`);
        }
        cfg.sourceKinds.push(value);
        break;
      }
      case "--interval":
        cfg.interval = requireValue();
        break;
      case "--for":
        cfg.runFor = requireValue();
        break;
      case "--until":
        cfg.until = requireValue();
        break;
      case "--prompt":
        cfg.prompt = requireValue();
        break;
      case "--prompt-file":
        cfg.promptFile = requireValue();
        break;
      case "--dry-run":
        cfg.dryRun = true;
        break;
      case "--once":
        cfg.once = true;
        break;
      case "--max-cycles":
        cfg.maxCycles = requireValue();
        break;
      case "--start-if-empty":
        cfg.startIfEmpty = true;
        break;
      case "--codex-bin":
        cfg.codexBin = requireValue();
        break;
      case "--experimental-api":
        cfg.experimentalApi = true;
        break;
      default:
        throw new Error(`Unknown option '${raw}'`);
    }
  }

  return cfg;
}

function parseDuration(value) {
  const text = String(value).trim().toLowerCase();
  const match = text.match(/^(\d+(?:\.\d+)?)([smhd])$/);
  if (!match) {
    throw new Error(`Invalid duration '${value}'. Use formats like 30s, 15m, 2h, 1d.`);
  }
  const amount = Number.parseFloat(match[1]);
  const unit = match[2];
  const scale = { s: 1, m: 60, h: 3600, d: 86400 }[unit];
  const seconds = amount * scale;
  if (!Number.isFinite(seconds) || seconds <= 0) {
    throw new Error(`Invalid duration '${value}'.`);
  }
  return seconds;
}

function parseClock(token) {
  const text = String(token).trim().toLowerCase();
  const match = text.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$/);
  if (!match) {
    throw new Error(`Invalid clock time '${token}'.`);
  }

  let hour = Number.parseInt(match[1], 10);
  const minute = Number.parseInt(match[2] ?? "0", 10);
  const meridiem = match[3] ?? null;

  if (minute < 0 || minute > 59) {
    throw new Error(`Invalid minutes in '${token}'.`);
  }

  if (meridiem) {
    if (hour < 1 || hour > 12) {
      throw new Error(`Invalid hour in '${token}'.`);
    }
    if (meridiem === "am") {
      hour = hour === 12 ? 0 : hour;
    } else {
      hour = hour === 12 ? 12 : hour + 12;
    }
  } else if (hour < 0 || hour > 23) {
    throw new Error(`Invalid hour in '${token}'.`);
  }

  return { hour, minute };
}

function localDateWithClock(base, hour, minute) {
  return new Date(
    base.getFullYear(),
    base.getMonth(),
    base.getDate(),
    hour,
    minute,
    0,
    0,
  );
}

function parseUntil(value) {
  const now = new Date();
  const raw = String(value).trim();
  const lower = raw.toLowerCase();

  const tomorrowMatch = lower.match(/^tomorrow(?:\s+(.+))?$/);
  if (tomorrowMatch) {
    const { hour, minute } = parseClock(tomorrowMatch[1] ?? "00:00");
    const tomorrow = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
    return localDateWithClock(tomorrow, hour, minute);
  }

  try {
    const { hour, minute } = parseClock(raw);
    const target = localDateWithClock(now, hour, minute);
    if (target <= now) {
      target.setDate(target.getDate() + 1);
    }
    return target;
  } catch {
    // Ignore and try Date.parse fallback.
  }

  const ms = Date.parse(raw);
  if (Number.isNaN(ms)) {
    throw new Error(
      `Invalid --until '${value}'. Use ISO datetime, HH:MM, or 'tomorrow 7am'.`,
    );
  }
  return new Date(ms);
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

class AppServerClient {
  constructor({ codexBin, experimentalApi }) {
    this.nextId = 1;
    this.pending = new Map();
    this.closed = false;

    this.proc = spawn(codexBin, ["app-server", "--listen", "stdio://"], {
      stdio: ["pipe", "pipe", "pipe"],
      env: process.env,
    });

    if (!this.proc.stdin || !this.proc.stdout || !this.proc.stderr) {
      throw new Error("Failed to open stdio pipes for codex app-server.");
    }

    this.stdoutRl = createInterface({ input: this.proc.stdout, crlfDelay: Infinity });
    this.stderrRl = createInterface({ input: this.proc.stderr, crlfDelay: Infinity });

    this.stdoutRl.on("line", (line) => this.onStdoutLine(line));
    this.stderrRl.on("line", (line) => {
      const msg = String(line || "").trim();
      if (msg.length > 0) {
        log(`app-server stderr: ${msg}`);
      }
    });

    this.proc.on("exit", (code, signal) => {
      this.closed = true;
      const err = new Error(`app-server exited (code=${code}, signal=${signal ?? "none"}).`);
      for (const entry of this.pending.values()) {
        clearTimeout(entry.timeout);
        entry.reject(err);
      }
      this.pending.clear();
    });

    this.proc.on("error", (err) => {
      this.closed = true;
      for (const entry of this.pending.values()) {
        clearTimeout(entry.timeout);
        entry.reject(err);
      }
      this.pending.clear();
    });

    this.ready = this.request("initialize", {
      clientInfo: { name: "heartbeat", version: VERSION },
      capabilities: { experimentalApi: Boolean(experimentalApi) },
    }).then(() => {
      this.notify("initialized");
    });
  }

  onStdoutLine(line) {
    const raw = String(line || "").trim();
    if (raw.length === 0) {
      return;
    }

    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      log(`Skipping non-JSON app-server output: ${raw.slice(0, 200)}`);
      return;
    }

    if (Object.prototype.hasOwnProperty.call(msg, "id")) {
      const pending = this.pending.get(msg.id);
      if (!pending) {
        return;
      }
      clearTimeout(pending.timeout);
      this.pending.delete(msg.id);

      if (Object.prototype.hasOwnProperty.call(msg, "error")) {
        pending.reject(new Error(`${pending.method} failed: ${JSON.stringify(msg.error)}`));
      } else {
        pending.resolve(msg.result);
      }
      return;
    }

    this.onNotification(msg);
  }

  onNotification(msg) {
    if (msg?.method === "turn/completed") {
      const threadId = msg?.params?.threadId ?? "?";
      const status = msg?.params?.turn?.status ?? "unknown";
      log(`event turn/completed thread=${threadId} status=${status}`);
    }
  }

  write(payload) {
    if (this.closed) {
      throw new Error("app-server is closed.");
    }
    this.proc.stdin.write(`${JSON.stringify(payload)}\n`);
  }

  notify(method, params = undefined) {
    const payload = { method };
    if (params !== undefined) {
      payload.params = params;
    }
    this.write(payload);
  }

  request(method, params = undefined, timeoutMs = 30000) {
    if (this.closed) {
      return Promise.reject(new Error("app-server is closed."));
    }

    const id = this.nextId;
    this.nextId += 1;

    const payload = { id, method };
    if (params !== undefined) {
      payload.params = params;
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`${method} timed out after ${timeoutMs}ms`));
      }, timeoutMs);

      this.pending.set(id, { resolve, reject, timeout, method });

      try {
        this.write(payload);
      } catch (err) {
        clearTimeout(timeout);
        this.pending.delete(id);
        reject(err);
      }
    });
  }

  async close() {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.stdoutRl.close();
    this.stderrRl.close();

    if (!this.proc.killed) {
      this.proc.kill("SIGTERM");
    }

    await sleep(100);

    if (this.proc.exitCode === null && !this.proc.killed) {
      this.proc.kill("SIGKILL");
    }
  }
}

async function resolveThreadId(client, config) {
  if (config.threadId) {
    return config.threadId;
  }

  const params = {
    limit: 1,
    sortKey: "updated_at",
    archived: false,
    cwd: config.cwd,
  };

  if (config.sourceKinds.length > 0) {
    params.sourceKinds = config.sourceKinds;
  }

  const result = await client.request("thread/list", params);
  const data = Array.isArray(result?.data) ? result.data : [];
  if (data.length === 0) {
    throw new Error(
      "No matching threads found. Pass --thread-id explicitly or adjust --cwd/--source-kind.",
    );
  }

  const threadId = data[0]?.id;
  if (!threadId) {
    throw new Error("thread/list returned an entry without id.");
  }
  return threadId;
}

async function readThread(client, threadId) {
  const result = await client.request("thread/read", {
    threadId,
    includeTurns: true,
  });

  if (!result || typeof result !== "object" || typeof result.thread !== "object") {
    throw new Error("thread/read returned an unexpected payload.");
  }
  return result.thread;
}

async function startFollowUpTurn(client, threadId, prompt) {
  const result = await client.request("turn/start", {
    threadId,
    input: [{ type: "text", text: prompt, text_elements: [] }],
  });

  const turnId = result?.turn?.id;
  if (!turnId) {
    throw new Error("turn/start returned no turn.id.");
  }
  return String(turnId);
}

async function runCycle({ client, config, threadId, cycle }) {
  const thread = await readThread(client, threadId);
  const turns = Array.isArray(thread.turns) ? thread.turns : [];

  if (turns.length === 0) {
    log(`cycle=${cycle} thread=${threadId} has no turns`);
    if (!config.startIfEmpty) {
      return;
    }

    if (config.dryRun) {
      log("dry-run: would start initial turn because thread is empty");
      return;
    }

    const newTurnId = await startFollowUpTurn(client, threadId, config.prompt);
    log(`started turn=${newTurnId} (thread was empty)`);
    return;
  }

  const lastTurn = turns[turns.length - 1] ?? {};
  const lastTurnId = String(lastTurn.id ?? "?");
  const status = String(lastTurn.status ?? "unknown");
  const updatedAt = thread.updatedAt ?? "?";

  log(
    `cycle=${cycle} thread=${threadId} last_turn=${lastTurnId} status=${status} updatedAt=${updatedAt}`,
  );

  if (!TERMINAL_TURN_STATUSES.has(status)) {
    return;
  }

  if (config.dryRun) {
    log(`dry-run: would start follow-up turn after status=${status}`);
    return;
  }

  const newTurnId = await startFollowUpTurn(client, threadId, config.prompt);
  log(`started follow-up turn=${newTurnId} after status=${status}`);
}

async function main() {
  let raw;
  try {
    raw = parseArgs(process.argv.slice(2));
  } catch (err) {
    fail(err.message, 2);
  }

  if (raw.help) {
    printHelp();
    return;
  }

  if (raw.version) {
    console.log(VERSION);
    return;
  }

  if (raw.runFor && raw.until) {
    fail("Use either --for or --until, not both.", 2);
  }

  let intervalSec;
  let runForSec = null;
  let untilDate = null;
  let maxCycles = null;

  try {
    intervalSec = parseDuration(raw.interval);
    if (raw.runFor) {
      runForSec = parseDuration(raw.runFor);
    }
    if (raw.until) {
      untilDate = parseUntil(raw.until);
    }
    if (raw.maxCycles !== null) {
      maxCycles = Number.parseInt(raw.maxCycles, 10);
      if (!Number.isFinite(maxCycles) || maxCycles <= 0) {
        throw new Error("--max-cycles must be a positive integer.");
      }
    }
  } catch (err) {
    fail(err.message, 2);
  }

  let prompt = raw.prompt;
  if (raw.promptFile) {
    try {
      prompt = (await readFile(raw.promptFile, "utf8")).trim();
    } catch (err) {
      fail(`Could not read --prompt-file '${raw.promptFile}': ${err.message}`, 2);
    }
  }
  if (!prompt) {
    fail("Prompt cannot be empty.", 2);
  }

  const cwd = raw.cwd;
  const deadline =
    untilDate ??
    (runForSec !== null ? new Date(Date.now() + Math.round(runForSec * 1000)) : null);

  if (deadline && deadline <= new Date()) {
    fail("Deadline is already in the past.", 2);
  }

  const config = {
    threadId: raw.threadId,
    cwd,
    sourceKinds: raw.sourceKinds,
    intervalSec,
    prompt,
    dryRun: raw.dryRun,
    once: raw.once,
    maxCycles,
    startIfEmpty: raw.startIfEmpty,
    codexBin: raw.codexBin,
    experimentalApi: raw.experimentalApi,
  };

  let client;
  try {
    client = new AppServerClient({
      codexBin: config.codexBin,
      experimentalApi: config.experimentalApi,
    });

    await client.ready;

    const threadId = await resolveThreadId(client, config);
    log(
      `heartbeat started thread=${threadId} interval=${Math.round(config.intervalSec)}s dry_run=${config.dryRun} deadline=${deadline ? deadline.toISOString() : "none"}`,
    );

    let cycle = 0;
    for (;;) {
      cycle += 1;
      await runCycle({ client, config, threadId, cycle });

      if (config.once) {
        break;
      }
      if (config.maxCycles !== null && cycle >= config.maxCycles) {
        break;
      }

      if (deadline) {
        const remainingMs = deadline.getTime() - Date.now();
        if (remainingMs <= 0) {
          break;
        }
        const sleepMs = Math.min(config.intervalSec * 1000, remainingMs);
        await sleep(sleepMs);
      } else {
        await sleep(config.intervalSec * 1000);
      }
    }

    log("heartbeat finished");
  } catch (err) {
    fail(err.message, 1);
  } finally {
    if (client) {
      await client.close();
    }
  }
}

await main();
