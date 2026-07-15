#!/usr/bin/env node
// Committed browser e2e for the VGI `worker:` SAB transport. Assembles the built
// haybarn engine + VGI wasm extension + worker module into a temp COI-served dir,
// bundles test-entry.mjs (esbuild, arrow inlined), drives it in headless Chromium
// (puppeteer), and asserts: extension LOADs, direct vgi_table_function over
// worker:, ATTACH + discovery + catalog call, and 4 concurrent scans (multi-thread
// serve). Exits 0 = pass, 1 = fail, 2 = skipped (prerequisites missing).
//
// Prerequisites (all built by their own scripts — see README.md):
//   - haybarn engine COI + JS package (haybarn-wasm/packages/duckdb-wasm/dist + bindings)
//   - VGI wasm extension (haybarn-wasm/extensions/<ver>/wasm_threads/vgi.duckdb_extension.wasm)
//   - worker module (../vgi_worker.js + .wasm, built via ../build.sh)
//   - puppeteer + esbuild resolvable from haybarn-wasm/node_modules
// Env overrides: HAYBARN_WASM, VGI_EXT_WASM, VGI_ENGINE_VERSION_DIR (default "unknown").
import { createRequire } from 'node:module';
import { execFileSync, spawn } from 'node:child_process';
import { existsSync, mkdtempSync, copyFileSync, mkdirSync, writeFileSync, rmSync, symlinkSync } from 'node:fs';
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

function skip(msg) { console.log('SKIP browser-e2e: ' + msg); process.exit(2); }

// --- prerequisite check ---
const need = {
  'engine bundle': join(DIST, 'duckdb-browser.mjs'),
  'coi worker': join(DIST, 'duckdb-browser-coi.worker.js'),
  'coi pthread worker': join(DIST, 'duckdb-browser-coi.pthread.worker.js'),
  'engine wasm': join(BINDINGS, 'duckdb-coi.wasm'),
  'vgi extension': EXT,
  'worker module js': join(HERE, '..', 'vgi_worker.js'),
  'worker module wasm': join(HERE, '..', 'vgi_worker.wasm'),
  'worker boot': join(HERE, '..', 'vgi-worker-boot.js'),
  'bridge': BRIDGE,
  esbuild: join(NM, '.bin/esbuild'),
};
for (const [k, p] of Object.entries(need)) if (!existsSync(p)) skip(`missing ${k} (${p}) — build it first`);
let puppeteer;
try { puppeteer = createRequire(join(HAYBARN, 'package.json'))('puppeteer'); }
catch { skip('puppeteer not resolvable from haybarn-wasm/node_modules'); }

// --- assemble a temp serve dir ---
const dir = mkdtempSync(join(tmpdir(), 'vgi-worker-e2e-'));
let server, browser;
const cleanup = () => {
  try { if (browser) browser.close(); } catch {}
  try { if (server) server.kill('SIGKILL'); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
};
process.on('exit', cleanup);

try {
  for (const f of ['duckdb-browser.mjs', 'duckdb-browser-coi.worker.js', 'duckdb-browser-coi.pthread.worker.js'])
    copyFileSync(join(DIST, f), join(dir, f));
  copyFileSync(join(BINDINGS, 'duckdb-coi.wasm'), join(dir, 'duckdb-coi.wasm'));
  for (const f of ['vgi_worker.js', 'vgi_worker.wasm', 'vgi-worker-boot.js'])
    copyFileSync(join(HERE, '..', f), join(dir, f));
  copyFileSync(BRIDGE, join(dir, 'vgi-webworker-bridge.ts'));
  copyFileSync(join(HERE, 'index.html'), join(dir, 'index.html'));
  copyFileSync(join(HERE, 'test-entry.mjs'), join(dir, 'test-entry.mjs'));
  mkdirSync(join(dir, 'extensions', VER_DIR, 'wasm_threads'), { recursive: true });
  copyFileSync(EXT, join(dir, 'extensions', VER_DIR, 'wasm_threads', 'vgi.duckdb_extension.wasm'));
  // node_modules symlink so esbuild resolves the bare `apache-arrow` import.
  symlinkSync(NM, join(dir, 'node_modules'));
  execFileSync(join(NM, '.bin/esbuild'),
    ['test-entry.mjs', '--bundle', '--format=esm', '--outfile=bundle.js', '--sourcemap=inline'],
    { cwd: dir, stdio: 'inherit' });

  // --- serve with COI + drive headless Chromium ---
  const port = 8790 + Math.floor(Math.random() * 200);
  // Serve from the temp dir (cwd) using the source coi-server.py.
  server = spawn('python3', [join(HERE, 'coi-server.py'), String(port)], { cwd: dir, stdio: 'ignore' });
  await new Promise((r) => setTimeout(r, 1200));

  browser = await puppeteer.launch({
    headless: 'new',
    // Hardening for the many nested workers/pthreads this test spins up headless.
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--js-flags=--max-old-space-size=2048'],
    protocolTimeout: 180000,
  });
  const page = await browser.newPage();
  page.on('pageerror', (e) => console.error('[pageerror]', e.message));
  page.on('console', (m) => console.log('[console]', m.text()));
  page.on('requestfailed', (r) => console.log('[reqfail]', r.failure()?.errorText, r.url()));
  page.on('response', (r) => { if (r.status() === 404) console.log('[404]', r.url()); });
  await page.goto(`http://127.0.0.1:${port}/index.html`, { waitUntil: 'load' });
  try {
    await page.waitForFunction('window.__done === true', { timeout: 90000 });
  } catch (te) {
    const dbg = await page.evaluate('({coi: self.crossOriginIsolated, done: window.__done, result: window.__result, out: (document.getElementById("out")||{}).textContent})');
    console.error('[timeout] crossOriginIsolated=' + dbg.coi + ' done=' + dbg.done);
    console.error('[timeout] out:\n' + dbg.out);
    console.error('[timeout] result: ' + JSON.stringify(dbg.result));
    throw te;
  }
  const result = await page.evaluate('window.__result');

  console.log('result:', JSON.stringify(result, null, 2));
  const pass = result && result.coi && result.loaded && result.workerOk && result.multiBatchOk &&
    result.attachOk && result.concurrentOk && result.errorOk && result.postErrorOk;
  if (!pass) { console.error('FAIL: worker: transport e2e assertions not all green'); process.exit(1); }
  console.log('PASS: worker: transport e2e (LOAD + direct + multi-batch + ATTACH + concurrent + ' +
    'error + post-error)');
  process.exit(0);
} catch (e) {
  console.error('FAIL:', e && e.stack ? e.stack : e);
  process.exit(1);
}
