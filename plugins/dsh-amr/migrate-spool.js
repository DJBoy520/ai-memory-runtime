import path from "node:path";
import { importLegacySpool } from "./lib/sqlite_spool.js";

const args = process.argv.slice(2);
const position = args.indexOf("--spool-dir");
if (position < 0 || !args[position + 1] || args[position + 1].startsWith("--") || !args.includes("--writers-stopped")) {
  console.error("Usage: node migrate-spool.js --spool-dir ABSOLUTE_PATH --writers-stopped");
  process.exitCode = 2;
} else if (!path.isAbsolute(args[position + 1])) {
  console.error("spool directory must be an absolute path");
  process.exitCode = 2;
} else {
  try {
    const result = importLegacySpool({ persistenceDir: args[position + 1] });
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } catch (err) {
    console.error(`offline spool import failed: ${err.message}`);
    process.exitCode = 1;
  }
}
