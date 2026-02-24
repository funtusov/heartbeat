#!/usr/bin/env node

import { spawn } from "node:child_process";
import fs from "node:fs";
import {
  mkdir,
  readFile,
  readdir,
  rename,
  writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const PACKAGE_JSON = JSON.parse(
  fs.readFileSync(new URL("../package.json", import.meta.url), "utf8"),
);
const VERSION = PACKAGE_JSON.version;

const COMMANDS = new Set(["run", "start", "status", "stop", "logs"]);
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

function nowIso() {
  return new Date().toISOString();
}

function log(message) {
  console.log(`[${nowIso()}] ${message}`);
}

function fail(message, code = 1) {
  console.error(`heartbeat: ${message}`);
  process.exit(code);
}

function printHelp() {
  console.log(`heartbeat v${VERSION}

Codex thread heartbeat CLI.

Usage:
  heartbeat run [options]
  heartbeat start [options]
  heartbeat status [options]
  heartbeat stop <job-id|pid> [options]
  heartbeat logs [job-id|pid] [options]

Backward-compatible alias:
  heartbeat [run options]    (same as 'heartbeat run ...')

Run/Start options:
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
  --name <label>            Optional label for a started background job
  --state-dir <path>        State dir (default: ~/.heartbeat)

Status options:
  --state-dir <path>        State dir (default: ~/.heartbeat)
  --json                    JSON output

Stop options:
  --all                     Stop all running jobs
  --force                   SIGKILL instead of SIGTERM
  --state-dir <path>        State dir (default: ~/.heartbeat)

Logs options:
  --tail <n>               Show last n lines (default: 200)
  --state-dir <path>       State dir (default: ~/.heartbeat)

Global:
  -h, --help               Show help
  -v, --version            Show version

Source kinds:
  cli, vscode, exec, appServer, subAgent, subAgentReview,
  subAgentCompact, subAgentThreadSpawn, subAgentOther, unknown

Examples:
  heartbeat start --thread-id 019c... --interval 15m --for 8h
  heartbeat status
  heartbeat stop --all
  heartbeat logs
  heartbeat --interval 15m --until "tomorrow 7am"
`);
}

function splitOption(token) {
  const idx = token.indexOf("=");
  if (idx === -1) {
    return [token, null];
  }
  return [token.slice(0, idx), token.slice(idx + 1)];
}

function requireOptionValue(argv, state, key, inlineValue) {
  if (inlineValue !== null) {
    return inlineValue;
  }
  const next = argv[state.index + 1];
  if (!next || next.startsWith("-")) {
    throw new Error(`Missing value for ${key}`);
  }
  state.index += 1;
  return next;
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
    // Fallback to Date.parse below.
  }

  const ms = Date.parse(raw);
  if (Number.isNaN(ms)) {
    throw new Error(
      `Invalid --until '${value}'. Use ISO datetime, HH:MM, or 'tomorrow 7am'.`,
    );
  }
  return new Date(ms);
}

function defaultStateDir() {
  return path.join(os.homedir(), ".heartbeat");
}

function resolveStateDir(input) {
  return path.resolve(input || defaultStateDir());
}

function jobsDir(stateDir) {
  return path.join(stateDir, "jobs");
}

function logsDir(stateDir) {
  return path.join(stateDir, "logs");
}

function jobFilePath(stateDir, jobId) {
  return path.join(jobsDir(stateDir), `${jobId}.json`);
}

function compactPrompt(text, limit = 140) {
  if (!text) {
    return "";
  }
  if (text.length <= limit) {
    return text;
  }
  return `${text.slice(0, limit - 3)}...`;
}

async function ensureStateDirs(stateDir) {
  await mkdir(jobsDir(stateDir), { recursive: true });
  await mkdir(logsDir(stateDir), { recursive: true });
}

async function readJsonFile(filePath) {
  const raw = await readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function writeJsonAtomic(filePath, data) {
  const tmp = `${filePath}.tmp-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const body = `${JSON.stringify(data, null, 2)}\n`;
  await writeFile(tmp, body, "utf8");
  await rename(tmp, filePath);
}

async function readJobRecord(stateDir, jobId) {
  try {
    return await readJsonFile(jobFilePath(stateDir, jobId));
  } catch (err) {
    if (err && err.code === "ENOENT") {
      return null;
    }
    throw err;
  }
}

async function writeJobRecord(stateDir, jobId, record) {
  const next = {
    ...record,
    id: jobId,
    updatedAt: nowIso(),
  };
  await writeJsonAtomic(jobFilePath(stateDir, jobId), next);
  return next;
}

async function patchJobRecord(stateDir, jobId, patchFn) {
  const existing = (await readJobRecord(stateDir, jobId)) || { id: jobId };
  const next = patchFn(existing) || existing;
  return writeJobRecord(stateDir, jobId, next);
}

async function listJobRecords(stateDir) {
  let files;
  try {
    files = await readdir(jobsDir(stateDir), { withFileTypes: true });
  } catch (err) {
    if (err && err.code === "ENOENT") {
      return [];
    }
    throw err;
  }

  const records = [];
  for (const file of files) {
    if (!file.isFile() || !file.name.endsWith(".json")) {
      continue;
    }
    const filePath = path.join(jobsDir(stateDir), file.name);
    try {
      const record = await readJsonFile(filePath);
      if (record && typeof record === "object") {
        records.push(record);
      }
    } catch (err) {
      records.push({
        id: file.name.replace(/\.json$/, ""),
        status: "invalid",
        runtime: { lastError: `invalid json: ${err.message}` },
      });
    }
  }

  records.sort((a, b) => {
    const aTs = Date.parse(a.createdAt || a.startedAt || 0);
    const bTs = Date.parse(b.createdAt || b.startedAt || 0);
    return bTs - aTs;
  });

  return records;
}

function isPidAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return err && err.code !== "ESRCH";
  }
}

function deriveJobStatus(record) {
  const status = String(record.status || "unknown");
  const pid = Number(record.pid);
  const alive = isPidAlive(pid);

  if (alive) {
    return "running";
  }
  if (status === "running" || status === "starting" || status === "stopping") {
    return "exited";
  }
  return status;
}

function formatSeconds(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return "-";
  }
  if (seconds < 60) {
    return `${Math.round(seconds)}s`;
  }
  if (seconds < 3600) {
    return `${Math.round(seconds / 60)}m`;
  }
  if (seconds < 86400) {
    return `${Math.round(seconds / 3600)}h`;
  }
  return `${Math.round(seconds / 86400)}d`;
}

function formatDateShort(value) {
  if (!value) {
    return "-";
  }
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) {
    return "-";
  }
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}`;
}

function formatRelative(value) {
  if (!value) {
    return "-";
  }
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) {
    return "-";
  }
  const deltaSec = Math.round((d.getTime() - Date.now()) / 1000);
  const absSec = Math.abs(deltaSec);
  const pretty = formatSeconds(absSec);
  if (deltaSec === 0) {
    return "now";
  }
  return deltaSec > 0 ? `in ${pretty}` : `${pretty} ago`;
}

function pad(value, width) {
  const text = String(value ?? "");
  if (text.length >= width) {
    return text;
  }
  return `${text}${" ".repeat(width - text.length)}`;
}

function renderTable(columns, rows) {
  const widths = columns.map((col) => col.label.length);
  for (const row of rows) {
    for (let idx = 0; idx < columns.length; idx += 1) {
      const key = columns[idx].key;
      widths[idx] = Math.max(widths[idx], String(row[key] ?? "").length);
    }
  }

  const header = columns
    .map((col, idx) => pad(col.label, widths[idx]))
    .join("  ");
  const separator = widths.map((w) => "-".repeat(w)).join("  ");
  console.log(header);
  console.log(separator);

  for (const row of rows) {
    const line = columns
      .map((col, idx) => pad(row[col.key] ?? "", widths[idx]))
      .join("  ");
    console.log(line);
  }
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function sleepInterruptible(ms, shouldStopRef) {
  let remaining = ms;
  while (remaining > 0) {
    if (shouldStopRef()) {
      return;
    }
    const step = Math.min(remaining, 1000);
    await sleep(step);
    remaining -= step;
  }
}

function generateJobId() {
  const stamp = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
  const rand = Math.random().toString(36).slice(2, 8);
  return `hb-${stamp}-${rand}`;
}

function splitCommand(argv) {
  if (argv.length === 0) {
    return { command: "run", args: [] };
  }
  const first = argv[0];
  if (COMMANDS.has(first)) {
    return { command: first, args: argv.slice(1) };
  }
  return { command: "run", args: argv };
}

function parseRunOptions(argv, { allowInternal = false } = {}) {
  const raw = {
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
    name: null,
    stateDir: defaultStateDir(),
    _jobId: null,
    _logFile: null,
    help: false,
    version: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const state = { index };
    const token = argv[index];
    if (token === "-h" || token === "--help") {
      raw.help = true;
      continue;
    }
    if (token === "-v" || token === "--version") {
      raw.version = true;
      continue;
    }

    const [key, inline] = splitOption(token);
    const value = () => requireOptionValue(argv, state, key, inline);

    switch (key) {
      case "--thread-id":
        raw.threadId = value();
        break;
      case "--cwd":
        raw.cwd = value();
        break;
      case "--source-kind": {
        const source = value();
        if (!SOURCE_KINDS.has(source)) {
          throw new Error(`Invalid --source-kind '${source}'`);
        }
        raw.sourceKinds.push(source);
        break;
      }
      case "--interval":
        raw.interval = value();
        break;
      case "--for":
        raw.runFor = value();
        break;
      case "--until":
        raw.until = value();
        break;
      case "--prompt":
        raw.prompt = value();
        break;
      case "--prompt-file":
        raw.promptFile = value();
        break;
      case "--dry-run":
        raw.dryRun = true;
        break;
      case "--once":
        raw.once = true;
        break;
      case "--max-cycles":
        raw.maxCycles = value();
        break;
      case "--start-if-empty":
        raw.startIfEmpty = true;
        break;
      case "--codex-bin":
        raw.codexBin = value();
        break;
      case "--experimental-api":
        raw.experimentalApi = true;
        break;
      case "--name":
        raw.name = value();
        break;
      case "--state-dir":
        raw.stateDir = value();
        break;
      case "--_job-id":
        if (!allowInternal) {
          throw new Error(`Unknown option '${key}'`);
        }
        raw._jobId = value();
        break;
      case "--_log-file":
        if (!allowInternal) {
          throw new Error(`Unknown option '${key}'`);
        }
        raw._logFile = value();
        break;
      default:
        throw new Error(`Unknown option '${token}'`);
    }
    index = state.index;
  }

  return raw;
}

function normalizeRunConfig(raw) {
  if (raw.runFor && raw.until) {
    throw new Error("Use either --for or --until, not both.");
  }

  const intervalSec = parseDuration(raw.interval);
  const runForSec = raw.runFor ? parseDuration(raw.runFor) : null;
  const untilDate = raw.until ? parseUntil(raw.until) : null;
  const deadline =
    untilDate ??
    (runForSec !== null ? new Date(Date.now() + Math.round(runForSec * 1000)) : null);

  if (deadline && deadline <= new Date()) {
    throw new Error("Deadline is already in the past.");
  }

  let maxCycles = null;
  if (raw.maxCycles !== null) {
    maxCycles = Number.parseInt(raw.maxCycles, 10);
    if (!Number.isFinite(maxCycles) || maxCycles <= 0) {
      throw new Error("--max-cycles must be a positive integer.");
    }
  }

  return {
    threadId: raw.threadId,
    cwd: path.resolve(raw.cwd),
    sourceKinds: raw.sourceKinds.slice(),
    intervalText: raw.interval,
    intervalSec,
    runForText: raw.runFor,
    runForSec,
    untilText: raw.until,
    untilDate,
    deadline,
    prompt: raw.prompt,
    promptFile: raw.promptFile,
    dryRun: raw.dryRun,
    once: raw.once,
    maxCycles,
    startIfEmpty: raw.startIfEmpty,
    codexBin: raw.codexBin,
    experimentalApi: raw.experimentalApi,
    name: raw.name,
    stateDir: resolveStateDir(raw.stateDir),
    jobId: raw._jobId,
    logFile: raw._logFile,
  };
}

function parseStatusOptions(argv) {
  const raw = {
    stateDir: defaultStateDir(),
    json: false,
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const state = { index };
    const token = argv[index];
    if (token === "-h" || token === "--help") {
      raw.help = true;
      continue;
    }

    const [key, inline] = splitOption(token);
    const value = () => requireOptionValue(argv, state, key, inline);

    switch (key) {
      case "--state-dir":
        raw.stateDir = value();
        break;
      case "--json":
        raw.json = true;
        break;
      default:
        throw new Error(`Unknown option '${token}'`);
    }
    index = state.index;
  }

  return {
    ...raw,
    stateDir: resolveStateDir(raw.stateDir),
  };
}

function parseStopOptions(argv) {
  const raw = {
    target: null,
    all: false,
    force: false,
    stateDir: defaultStateDir(),
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const state = { index };
    const token = argv[index];

    if (token === "-h" || token === "--help") {
      raw.help = true;
      continue;
    }

    if (!token.startsWith("-")) {
      if (raw.target !== null) {
        throw new Error(`Unexpected extra argument '${token}'`);
      }
      raw.target = token;
      continue;
    }

    const [key, inline] = splitOption(token);
    const value = () => requireOptionValue(argv, state, key, inline);

    switch (key) {
      case "--all":
        raw.all = true;
        break;
      case "--force":
        raw.force = true;
        break;
      case "--state-dir":
        raw.stateDir = value();
        break;
      default:
        throw new Error(`Unknown option '${token}'`);
    }
    index = state.index;
  }

  if (!raw.all && !raw.target) {
    throw new Error("Specify <job-id|pid> or use --all.");
  }
  if (raw.all && raw.target) {
    throw new Error("Use either --all or an explicit <job-id|pid>, not both.");
  }

  return {
    ...raw,
    stateDir: resolveStateDir(raw.stateDir),
  };
}

function parseLogsOptions(argv) {
  const raw = {
    target: null,
    tail: 200,
    stateDir: defaultStateDir(),
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const state = { index };
    const token = argv[index];

    if (token === "-h" || token === "--help") {
      raw.help = true;
      continue;
    }

    if (!token.startsWith("-")) {
      if (raw.target !== null) {
        throw new Error(`Unexpected extra argument '${token}'`);
      }
      raw.target = token;
      continue;
    }

    const [key, inline] = splitOption(token);
    const value = () => requireOptionValue(argv, state, key, inline);

    switch (key) {
      case "--tail":
        raw.tail = Number.parseInt(value(), 10);
        if (!Number.isFinite(raw.tail) || raw.tail <= 0) {
          throw new Error("--tail must be a positive integer.");
        }
        break;
      case "--state-dir":
        raw.stateDir = value();
        break;
      default:
        throw new Error(`Unknown option '${token}'`);
    }
    index = state.index;
  }

  return {
    ...raw,
    stateDir: resolveStateDir(raw.stateDir),
  };
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
      for (const pending of this.pending.values()) {
        clearTimeout(pending.timeout);
        pending.reject(err);
      }
      this.pending.clear();
    });

    this.proc.on("error", (err) => {
      this.closed = true;
      for (const pending of this.pending.values()) {
        clearTimeout(pending.timeout);
        pending.reject(err);
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
  const id = data[0]?.id;
  if (!id) {
    throw new Error("thread/list returned an entry without id.");
  }
  return id;
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
      return {
        kind: "idle-empty",
        detail: "thread has no turns",
        lastTurnStatus: null,
        startedTurnId: null,
      };
    }
    if (config.dryRun) {
      log("dry-run: would start initial turn because thread is empty");
      return {
        kind: "dry-run-initial",
        detail: "would start initial turn",
        lastTurnStatus: null,
        startedTurnId: null,
      };
    }
    const startedTurnId = await startFollowUpTurn(client, threadId, config.prompt);
    log(`started turn=${startedTurnId} (thread was empty)`);
    return {
      kind: "started-initial",
      detail: `started ${startedTurnId}`,
      lastTurnStatus: null,
      startedTurnId,
    };
  }

  const lastTurn = turns[turns.length - 1] ?? {};
  const lastTurnId = String(lastTurn.id ?? "?");
  const status = String(lastTurn.status ?? "unknown");
  const updatedAt = thread.updatedAt ?? "?";
  log(
    `cycle=${cycle} thread=${threadId} last_turn=${lastTurnId} status=${status} updatedAt=${updatedAt}`,
  );

  if (!TERMINAL_TURN_STATUSES.has(status)) {
    return {
      kind: "idle-active",
      detail: `last turn still ${status}`,
      lastTurnStatus: status,
      startedTurnId: null,
    };
  }

  if (config.dryRun) {
    log(`dry-run: would start follow-up turn after status=${status}`);
    return {
      kind: "dry-run-followup",
      detail: `would start follow-up after ${status}`,
      lastTurnStatus: status,
      startedTurnId: null,
    };
  }

  const startedTurnId = await startFollowUpTurn(client, threadId, config.prompt);
  log(`started follow-up turn=${startedTurnId} after status=${status}`);
  return {
    kind: "started-followup",
    detail: `started ${startedTurnId} after ${status}`,
    lastTurnStatus: status,
    startedTurnId,
  };
}

function buildRunArgs(config, internal) {
  const args = [SCRIPT_PATH, "run"];
  if (config.threadId) {
    args.push("--thread-id", config.threadId);
  }
  args.push("--cwd", config.cwd);
  for (const source of config.sourceKinds) {
    args.push("--source-kind", source);
  }
  args.push("--interval", `${config.intervalSec}s`);
  if (config.runForSec !== null) {
    args.push("--for", `${config.runForSec}s`);
  }
  if (config.untilDate) {
    args.push("--until", config.untilDate.toISOString());
  }
  args.push("--prompt", config.prompt);
  if (config.dryRun) {
    args.push("--dry-run");
  }
  if (config.once) {
    args.push("--once");
  }
  if (config.maxCycles !== null) {
    args.push("--max-cycles", String(config.maxCycles));
  }
  if (config.startIfEmpty) {
    args.push("--start-if-empty");
  }
  args.push("--codex-bin", config.codexBin);
  if (config.experimentalApi) {
    args.push("--experimental-api");
  }
  if (config.name) {
    args.push("--name", config.name);
  }
  args.push("--state-dir", config.stateDir);
  args.push("--_job-id", internal.jobId);
  args.push("--_log-file", internal.logFile);
  return args;
}

function makeJobRecord(config, { jobId, logFile }) {
  const now = nowIso();
  return {
    id: jobId,
    name: config.name || null,
    status: "starting",
    pid: null,
    createdAt: now,
    startedAt: null,
    stoppedAt: null,
    threadId: config.threadId || null,
    cwd: config.cwd,
    intervalSec: config.intervalSec,
    deadline: config.deadline ? config.deadline.toISOString() : null,
    logFile,
    command: {
      threadId: config.threadId || null,
      cwd: config.cwd,
      sourceKinds: config.sourceKinds,
      interval: config.intervalText,
      intervalSec: config.intervalSec,
      runFor: config.runForText,
      until: config.untilText,
      promptPreview: compactPrompt(config.prompt),
      dryRun: config.dryRun,
      once: config.once,
      maxCycles: config.maxCycles,
      startIfEmpty: config.startIfEmpty,
      codexBin: config.codexBin,
      experimentalApi: config.experimentalApi,
      name: config.name || null,
    },
    runtime: {
      runs: 0,
      startedTurns: 0,
      lastRunAt: null,
      nextRunAt: null,
      lastAction: null,
      lastTurnStatus: null,
      lastStartedTurnId: null,
      lastError: null,
      resolvedThreadId: null,
    },
    updatedAt: now,
  };
}

async function initRunJobRecord(config) {
  if (!config.jobId) {
    return;
  }
  await ensureStateDirs(config.stateDir);
  await patchJobRecord(config.stateDir, config.jobId, (record) => {
    const base = {
      ...record,
      id: config.jobId,
      status: "running",
      pid: process.pid,
      startedAt: record.startedAt || nowIso(),
      stoppedAt: null,
      threadId: record.threadId || config.threadId || null,
      cwd: config.cwd,
      intervalSec: config.intervalSec,
      deadline: config.deadline ? config.deadline.toISOString() : null,
      logFile: config.logFile || record.logFile || null,
      runtime: {
        runs: 0,
        startedTurns: 0,
        lastRunAt: null,
        nextRunAt: null,
        lastAction: null,
        lastTurnStatus: null,
        lastStartedTurnId: null,
        lastError: null,
        resolvedThreadId: null,
        ...(record.runtime || {}),
      },
    };
    return base;
  });
}

async function updateRunJobCycle(config, update) {
  if (!config.jobId) {
    return;
  }
  await patchJobRecord(config.stateDir, config.jobId, (record) => {
    const runtime = { ...(record.runtime || {}) };
    runtime.runs = Number(runtime.runs || 0) + 1;
    runtime.lastRunAt = update.lastRunAt || runtime.lastRunAt || nowIso();
    runtime.nextRunAt = update.nextRunAt ?? null;
    runtime.lastAction = update.lastAction || runtime.lastAction || null;
    runtime.lastTurnStatus = update.lastTurnStatus ?? runtime.lastTurnStatus ?? null;
    runtime.lastStartedTurnId = update.lastStartedTurnId ?? runtime.lastStartedTurnId ?? null;
    runtime.lastError = update.lastError ?? runtime.lastError ?? null;
    runtime.resolvedThreadId = update.resolvedThreadId || runtime.resolvedThreadId || null;
    if (update.startedTurnIncrement) {
      runtime.startedTurns = Number(runtime.startedTurns || 0) + update.startedTurnIncrement;
    } else {
      runtime.startedTurns = Number(runtime.startedTurns || 0);
    }
    return {
      ...record,
      status: update.status || record.status || "running",
      pid: process.pid,
      threadId: update.resolvedThreadId || record.threadId || null,
      runtime,
    };
  });
}

async function finalizeRunJob(config, status, extra = {}) {
  if (!config.jobId) {
    return;
  }
  await patchJobRecord(config.stateDir, config.jobId, (record) => {
    const runtime = { ...(record.runtime || {}) };
    if (Object.prototype.hasOwnProperty.call(extra, "lastError")) {
      runtime.lastError = extra.lastError;
    }
    if (Object.prototype.hasOwnProperty.call(extra, "nextRunAt")) {
      runtime.nextRunAt = extra.nextRunAt;
    } else {
      runtime.nextRunAt = null;
    }
    if (extra.lastAction) {
      runtime.lastAction = extra.lastAction;
    }
    return {
      ...record,
      status,
      runtime,
      stoppedAt: nowIso(),
    };
  });
}

async function executeRun(config) {
  if (config.promptFile) {
    try {
      config.prompt = (await readFile(config.promptFile, "utf8")).trim();
    } catch (err) {
      throw new Error(`Could not read --prompt-file '${config.promptFile}': ${err.message}`);
    }
    if (!config.prompt) {
      throw new Error("Prompt cannot be empty.");
    }
  }

  let stopRequested = false;
  let stopSignal = null;
  const onSignal = (signal) => {
    stopRequested = true;
    stopSignal = signal;
  };

  process.on("SIGTERM", onSignal);
  process.on("SIGINT", onSignal);

  let client = null;
  try {
    await initRunJobRecord(config);

    client = new AppServerClient({
      codexBin: config.codexBin,
      experimentalApi: config.experimentalApi,
    });
    await client.ready;

    const threadId = await resolveThreadId(client, config);
    log(
      `heartbeat started thread=${threadId} interval=${Math.round(config.intervalSec)}s dry_run=${config.dryRun} deadline=${config.deadline ? config.deadline.toISOString() : "none"}`,
    );

    if (config.jobId) {
      await patchJobRecord(config.stateDir, config.jobId, (record) => ({
        ...record,
        status: "running",
        threadId,
        runtime: {
          ...(record.runtime || {}),
          resolvedThreadId: threadId,
        },
      }));
    }

    let cycle = 0;
    for (;;) {
      if (stopRequested) {
        break;
      }

      cycle += 1;
      const cycleStartedAt = nowIso();
      const action = await runCycle({ client, config, threadId, cycle });

      let shouldBreak = false;
      let nextRunAtIso = null;
      let sleepMs = 0;

      if (config.once) {
        shouldBreak = true;
      } else if (config.maxCycles !== null && cycle >= config.maxCycles) {
        shouldBreak = true;
      } else if (config.deadline) {
        const remainingMs = config.deadline.getTime() - Date.now();
        if (remainingMs <= 0) {
          shouldBreak = true;
        } else {
          sleepMs = Math.min(config.intervalSec * 1000, remainingMs);
          nextRunAtIso = new Date(Date.now() + sleepMs).toISOString();
        }
      } else {
        sleepMs = config.intervalSec * 1000;
        nextRunAtIso = new Date(Date.now() + sleepMs).toISOString();
      }

      await updateRunJobCycle(config, {
        status: "running",
        resolvedThreadId: threadId,
        lastRunAt: cycleStartedAt,
        nextRunAt: nextRunAtIso,
        lastAction: action.detail,
        lastTurnStatus: action.lastTurnStatus,
        lastStartedTurnId: action.startedTurnId,
        startedTurnIncrement: action.startedTurnId ? 1 : 0,
      });

      if (shouldBreak) {
        break;
      }

      await sleepInterruptible(sleepMs, () => stopRequested);
    }

    const endStatus = stopRequested ? "stopped" : "completed";
    const endAction = stopRequested
      ? `stopped by signal ${stopSignal || "unknown"}`
      : "finished";
    await finalizeRunJob(config, endStatus, { lastAction: endAction });
    log("heartbeat finished");
  } catch (err) {
    await finalizeRunJob(config, "error", {
      lastError: err.message,
      lastAction: "failed",
    });
    throw err;
  } finally {
    process.removeListener("SIGTERM", onSignal);
    process.removeListener("SIGINT", onSignal);
    if (client) {
      await client.close();
    }
  }
}

async function executeStart(config) {
  await ensureStateDirs(config.stateDir);

  if (config.promptFile) {
    try {
      config.prompt = (await readFile(config.promptFile, "utf8")).trim();
    } catch (err) {
      throw new Error(`Could not read --prompt-file '${config.promptFile}': ${err.message}`);
    }
    if (!config.prompt) {
      throw new Error("Prompt cannot be empty.");
    }
  }

  const jobId = generateJobId();
  const logFile = path.join(logsDir(config.stateDir), `${jobId}.log`);
  const initialRecord = makeJobRecord(config, { jobId, logFile });
  await writeJobRecord(config.stateDir, jobId, initialRecord);

  const runArgs = buildRunArgs(config, { jobId, logFile });
  const logFd = fs.openSync(logFile, "a");
  let child;

  try {
    child = spawn(process.execPath, runArgs, {
      detached: true,
      stdio: ["ignore", logFd, logFd],
      env: process.env,
    });
    child.unref();
  } catch (err) {
    fs.closeSync(logFd);
    await finalizeRunJob(
      { ...config, jobId, stateDir: config.stateDir },
      "error",
      { lastError: err.message, lastAction: "spawn failed" },
    );
    throw err;
  }

  fs.closeSync(logFd);

  await patchJobRecord(config.stateDir, jobId, (record) => ({
    ...record,
    status: "running",
    pid: child.pid,
    startedAt: record.startedAt || nowIso(),
  }));

  console.log(`Started heartbeat job ${jobId}`);
  console.log(`  pid:      ${child.pid}`);
  console.log(`  interval: ${Math.round(config.intervalSec)}s`);
  console.log(`  state:    ${config.stateDir}`);
  console.log(`  logs:     ${logFile}`);
  console.log(`  monitor:  heartbeat status --state-dir ${JSON.stringify(config.stateDir)}`);
  console.log(`  stop:     heartbeat stop ${jobId} --state-dir ${JSON.stringify(config.stateDir)}`);
}

function mapStatusRow(record) {
  const runtime = record.runtime || {};
  const state = deriveJobStatus(record);
  return {
    job: record.id || "-",
    state,
    started: formatDateShort(record.startedAt || record.createdAt),
    every: formatSeconds(Number(record.intervalSec)),
    thread: runtime.resolvedThreadId || record.threadId || "-",
    runs: String(runtime.runs || 0),
    lastRun: formatRelative(runtime.lastRunAt),
    nextRun: state === "running" ? formatRelative(runtime.nextRunAt) : "-",
    action: runtime.lastAction || "-",
    pid: Number.isInteger(Number(record.pid)) ? String(record.pid) : "-",
  };
}

async function executeStatus(config) {
  const jobs = await listJobRecords(config.stateDir);
  if (config.json) {
    const payload = jobs.map((job) => ({
      ...job,
      derivedStatus: deriveJobStatus(job),
      pidAlive: isPidAlive(Number(job.pid)),
    }));
    console.log(JSON.stringify(payload, null, 2));
    return;
  }

  if (jobs.length === 0) {
    console.log(`No heartbeat jobs found in ${config.stateDir}`);
    return;
  }

  const rows = jobs.map(mapStatusRow);
  renderTable(
    [
      { key: "job", label: "JOB" },
      { key: "state", label: "STATE" },
      { key: "started", label: "STARTED" },
      { key: "every", label: "EVERY" },
      { key: "thread", label: "THREAD" },
      { key: "runs", label: "RUNS" },
      { key: "lastRun", label: "LAST RUN" },
      { key: "nextRun", label: "NEXT RUN" },
      { key: "action", label: "LAST ACTION" },
      { key: "pid", label: "PID" },
    ],
    rows,
  );
}

function resolveStopTargets(jobs, config) {
  if (config.all) {
    return jobs.filter((job) => isPidAlive(Number(job.pid)));
  }
  const target = String(config.target);
  const byId = jobs.find((job) => String(job.id) === target);
  if (byId) {
    return [byId];
  }
  if (/^\d+$/.test(target)) {
    const pid = Number.parseInt(target, 10);
    const byPid = jobs.find((job) => Number(job.pid) === pid);
    if (byPid) {
      return [byPid];
    }
  }
  throw new Error(`No job found for '${target}'.`);
}

async function executeStop(config) {
  const jobs = await listJobRecords(config.stateDir);
  if (jobs.length === 0) {
    console.log(`No heartbeat jobs found in ${config.stateDir}`);
    return;
  }

  const targets = resolveStopTargets(jobs, config);
  if (targets.length === 0) {
    console.log("No running heartbeat jobs matched.");
    return;
  }

  const signal = config.force ? "SIGKILL" : "SIGTERM";
  for (const job of targets) {
    const pid = Number(job.pid);
    if (!isPidAlive(pid)) {
      await patchJobRecord(config.stateDir, job.id, (record) => ({
        ...record,
        status: "exited",
        stoppedAt: record.stoppedAt || nowIso(),
      }));
      console.log(`Job ${job.id} is not running.`);
      continue;
    }
    try {
      process.kill(pid, signal);
      await patchJobRecord(config.stateDir, job.id, (record) => ({
        ...record,
        status: signal === "SIGKILL" ? "stopped" : "stopping",
        stopRequestedAt: nowIso(),
        stopSignal: signal,
      }));
      console.log(`Sent ${signal} to job ${job.id} (pid ${pid})`);
    } catch (err) {
      console.log(`Failed to stop job ${job.id} (pid ${pid}): ${err.message}`);
    }
  }
}

function resolveLogsTarget(jobs, target) {
  if (!target) {
    return jobs[0] || null;
  }
  const byId = jobs.find((job) => String(job.id) === String(target));
  if (byId) {
    return byId;
  }
  if (/^\d+$/.test(String(target))) {
    const pid = Number.parseInt(String(target), 10);
    const byPid = jobs.find((job) => Number(job.pid) === pid);
    if (byPid) {
      return byPid;
    }
  }
  return null;
}

async function executeLogs(config) {
  const jobs = await listJobRecords(config.stateDir);
  if (jobs.length === 0) {
    console.log(`No heartbeat jobs found in ${config.stateDir}`);
    return;
  }

  const job = resolveLogsTarget(jobs, config.target);
  if (!job) {
    throw new Error(`No job found for '${config.target}'.`);
  }

  const logFile = job.logFile || path.join(logsDir(config.stateDir), `${job.id}.log`);
  let body;
  try {
    body = await readFile(logFile, "utf8");
  } catch (err) {
    if (err && err.code === "ENOENT") {
      console.log(`No log file for job ${job.id}: ${logFile}`);
      return;
    }
    throw err;
  }

  const lines = body.split(/\r?\n/).filter((line) => line.length > 0);
  const start = Math.max(0, lines.length - config.tail);
  console.log(`== logs for ${job.id} (${logFile}) ==`);
  for (let idx = start; idx < lines.length; idx += 1) {
    console.log(lines[idx]);
  }
}

async function main() {
  const argv = process.argv.slice(2);

  if (argv.includes("-v") || argv.includes("--version")) {
    console.log(VERSION);
    return;
  }

  if (argv.length === 0) {
    printHelp();
    return;
  }

  const { command, args } = splitCommand(argv);
  try {
    if (command === "run") {
      const raw = parseRunOptions(args, { allowInternal: true });
      if (raw.help) {
        printHelp();
        return;
      }
      if (raw.version) {
        console.log(VERSION);
        return;
      }
      const config = normalizeRunConfig(raw);
      await executeRun(config);
      return;
    }

    if (command === "start") {
      const raw = parseRunOptions(args, { allowInternal: false });
      if (raw.help) {
        printHelp();
        return;
      }
      const config = normalizeRunConfig(raw);
      await executeStart(config);
      return;
    }

    if (command === "status") {
      const config = parseStatusOptions(args);
      if (config.help) {
        printHelp();
        return;
      }
      await executeStatus(config);
      return;
    }

    if (command === "stop") {
      const config = parseStopOptions(args);
      if (config.help) {
        printHelp();
        return;
      }
      await executeStop(config);
      return;
    }

    if (command === "logs") {
      const config = parseLogsOptions(args);
      if (config.help) {
        printHelp();
        return;
      }
      await executeLogs(config);
      return;
    }

    throw new Error(`Unknown command '${command}'`);
  } catch (err) {
    fail(err.message, 1);
  }
}

await main();
