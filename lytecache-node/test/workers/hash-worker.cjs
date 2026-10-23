// Test helper: hset/hincrby from a separate OS process (cross-process lost-update tests).
const { LyteCache } = require("../../dist/index.cjs");

const [, , dbPath, mode, key, field, iterationsRaw] = process.argv;
const cache = new LyteCache({ path: dbPath, sweepInterval: null });
for (let i = 0; i < Number.parseInt(iterationsRaw, 10); i++) {
  if (mode === "hset") cache.hset(key, field, i);
  else cache.hincrby(key, field, 1);
}
cache.close();
