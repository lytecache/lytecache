// Test helper for the cross-process events tests. Modes:
//   subscribe <pattern> <count> <outFile> <readyFile>
//   publish <count>
//   cursorsub <readyFile>   (handles "jobs" forever, saving its cursor; killed by the test)
const { writeFileSync } = require("node:fs");
const { LyteCache } = require("../../dist/index.cjs");

const [, , dbPath, mode, ...rest] = process.argv;
const cache = new LyteCache({ path: dbPath, sweepInterval: null });

async function main() {
  if (mode === "subscribe") {
    const [pattern, countRaw, outFile, readyFile] = rest;
    const sub = cache.watch(pattern);
    writeFileSync(readyFile, "ready");
    const got = [];
    for await (const e of sub) {
      got.push({ seq: e.seq, channel: e.channel, payload: e.payload });
      if (got.length === Number(countRaw)) break;
    }
    sub.close();
    writeFileSync(outFile, JSON.stringify(got));
  } else if (mode === "publish") {
    for (let i = 0; i < Number(rest[0]); i++) {
      cache.notify("orders:new", i);
      cache.notify("unrelated", i);
    }
  } else if (mode === "cursorsub") {
    const from = cache.loadCursor("worker");
    const sub = cache.watch("jobs", from === undefined ? {} : { fromSeq: from });
    writeFileSync(rest[0], "ready");
    for await (const e of sub) cache.saveCursor("worker", e.seq);
  }
  cache.close();
}
main().catch((err) => {
  console.error(err);
  process.exit(1);
});
