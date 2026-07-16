#!/usr/bin/env node
// Assemble the browser-e2e serve dir (same as run.mjs) and serve it under COI,
// WITHOUT launching puppeteer — for driving via a stable external browser / MCP
// Playwright when headless Chromium under puppeteer session-closes on the nested
// worker load. Prints the URL and stays up until Ctrl-C. The assembled dir is kept.
//   node serve.mjs [port]
import { execFileSync, spawn } from 'node:child_process';
import { existsSync, mkdtempSync, copyFileSync, mkdirSync, rmSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const HAYBARN = process.env.HAYBARN_WASM || join(process.env.HOME, 'Development/haybarn/haybarn-wasm');
const DIST = join(HAYBARN, 'packages/duckdb-wasm/dist');
const BINDINGS = join(HAYBARN, 'packages/duckdb-wasm/src/bindings');
const NM = join(HAYBARN, 'node_modules');
const VER_DIR = process.env.VGI_ENGINE_VERSION_DIR || 'unknown';
const EXT = process.env.VGI_EXT_WASM || join(HAYBARN, 'extensions/v1.5.4/wasm_threads/vgi.duckdb_extension.wasm');
const BRIDGE = join(HAYBARN, 'packages/duckdb-wasm-app/src/lib/vgi-webworker-bridge.ts');
const PORT = process.argv[2] || '8799';

const dir = mkdtempSync(join(tmpdir(), 'vgi-worker-serve-'));
for (const f of ['duckdb-browser.mjs', 'duckdb-browser-coi.worker.js', 'duckdb-browser-coi.pthread.worker.js'])
  copyFileSync(join(DIST, f), join(dir, f));
copyFileSync(join(BINDINGS, 'duckdb-coi.wasm'), join(dir, 'duckdb-coi.wasm'));
for (const f of ['vgi_worker.js', 'vgi_worker.wasm', 'vgi-worker-boot.js', 'ts-worker-boot.js', 'ts-worker-mod.js'])
  copyFileSync(join(HERE, '..', f), join(dir, f));
copyFileSync(BRIDGE, join(dir, 'vgi-webworker-bridge.ts'));
copyFileSync(join(HERE, 'index.html'), join(dir, 'index.html'));
// VGI_ENTRY overrides the bundled entry (default test-entry.mjs) — used to run a
// standalone probe (e.g. probe-throw.mjs) without touching the main suite.
const ENTRY = process.env.VGI_ENTRY || 'test-entry.mjs';
copyFileSync(join(HERE, ENTRY), join(dir, ENTRY));
mkdirSync(join(dir, 'extensions', VER_DIR, 'wasm_threads'), { recursive: true });
copyFileSync(EXT, join(dir, 'extensions', VER_DIR, 'wasm_threads', 'vgi.duckdb_extension.wasm'));
symlinkSync(NM, join(dir, 'node_modules'));
execFileSync(join(NM, '.bin/esbuild'),
  [ENTRY, '--bundle', '--format=esm', '--outfile=bundle.js', '--sourcemap=inline'],
  { cwd: dir, stdio: 'inherit' });

const server = spawn('python3', [join(HERE, 'coi-server.py'), PORT], { cwd: dir, stdio: 'inherit' });
console.log(`\nSERVE dir=${dir}\nSERVE url=http://127.0.0.1:${PORT}/index.html\n(poll window.__done / window.__result; Ctrl-C to stop)`);
const cleanup = () => { try { server.kill('SIGKILL'); } catch {} try { rmSync(dir, { recursive: true, force: true }); } catch {} };
process.on('SIGINT', () => { cleanup(); process.exit(0); });
process.on('exit', cleanup);
