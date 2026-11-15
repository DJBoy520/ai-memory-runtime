/**
 * Per-test spool isolation.
 *
 * Some OpenClaw tests call plugin.register() without an explicit
 * persistenceDir. Without this module they would use the real user spool
 * directory (~/.local/share/openclaw-amr/sessions), leaving test records in
 * the operator's data. Import this module first in every test file that
 * registers the plugin.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach } from "node:test";

let currentDir = null;

function freshDir() {
  currentDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-amr-test-spool-"));
  process.env.AMR_SESSION_SPOOL_DIR = currentDir;
  return currentDir;
}

freshDir();

beforeEach(() => {
  freshDir();
});

afterEach(() => {
  if (!currentDir) return;
  try {
    fs.rmSync(currentDir, { recursive: true, force: true });
  } catch (_) {}
  currentDir = null;
});
