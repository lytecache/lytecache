import { fork, type ChildProcess } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import { afterEach, describe, expect, it } from "vitest";
import { GapError, LyteCache, type ChangeRecord, type Subscription } from "../src/index.js";
import { tempDbPath, tempDir } from "./helpers.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const opened: (LyteCache | Subscription)[] = [];

function cache(
  path = tempDbPath(),
  options: ConstructorParameters<typeof LyteCache>[0] = {},
): LyteCache {
  const c = new LyteCache({ path, sweepInterval: null, logger: () => {}, ...options });
  opened.push(c);
  return c;
}

function watch(c: LyteCache, ...args: Parameters<LyteCache["watch"]>): Subscription {
  const s = c.watch(...args);
  opened.push(s);
  return s;
}

afterEach(() => {
  for (const o of opened.splice(0)) o.close();
});

async function nextOk(s: Subscription) {
  const e = await s.next(5000);
  if (e === undefined) throw new Error("timed out waiting for an event");
  return e;
}

function eventsTableExists(path: string): boolean {
  const db = new Database(path, { readonly: true });
  try {
    return (
      db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'").get() !==
      undefined
    );
  } finally {
    db.close();
  }
}

async function waitFor(cond: () => boolean, timeoutMs = 20000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await sleep(10);
  }
}

describe("notify / watch", () => {
  it("delivers new events only by default, with seq/channel/namespace/payload/createdAt", async () => {
    const c = cache();
    c.notify("orders", "before");
    const s = watch(c, "orders");
    expect(await s.next(150)).toBeUndefined();
    const s1 = c.notify("orders", { id: 1 });
    const s2 = c.notify("orders");
    const e1 = await nextOk(s);
    const e2 = await nextOk(s);
    expect([e1.seq, e1.channel, e1.namespace, e1.payload]).toEqual([
      s1,
      "orders",
      "default",
      { id: 1 },
    ]);
    expect(e2.payload).toBeNull();
    expect(e1.createdAt).toBeGreaterThan(0);
    expect(s.lastSeq).toBe(s2);
  });

  it("fromSeq 0 replays everything retained; fromSeq N resumes after N", async () => {
    const c = cache();
    const seqs = [0, 1, 2, 3, 4].map((i) => c.notify("c", i));
    const all = watch(c, "c", { fromSeq: 0 });
    for (const seq of seqs) expect((await nextOk(all)).seq).toBe(seq);
    const after = watch(c, "c", { fromSeq: seqs[1] });
    for (const i of [2, 3, 4]) expect((await nextOk(after)).payload).toBe(i);
    expect(await after.next(150)).toBeUndefined();
  });

  it("async iteration, tryNext, and close ending iteration", async () => {
    const c = cache();
    const s = watch(c, "c");
    expect(s.tryNext()).toBeUndefined();
    c.notify("c", 1);
    await waitFor(() => (s as unknown as { queue: unknown[] }).queue.length > 0);
    expect(s.tryNext()?.payload).toBe(1);
    const got: unknown[] = [];
    const loop = (async () => {
      for await (const e of s) got.push(e.payload);
    })();
    c.notify("c", 2);
    await waitFor(() => got.length === 1);
    s.close();
    s.close();
    await loop;
    expect(got).toEqual([2]);
  });

  it("EventEmitter-style .on('event')", async () => {
    const c = cache();
    const s = watch(c, "c");
    const got: unknown[] = [];
    s.on("event", (e) => got.push(e.payload));
    c.notify("c", "a");
    c.notify("c", "b");
    await waitFor(() => got.length === 2);
    expect(got).toEqual(["a", "b"]);
  });

  it("total order across channels, fan-out, notifyMany", async () => {
    const c = cache();
    const a = watch(c, ["a", "b"]);
    const b = watch(c, "*");
    const seqs = c.notifyMany([
      ["a", 1],
      ["b", 2],
      ["a", 3],
    ]);
    expect(seqs).toEqual([...seqs].sort((x, y) => x - y));
    for (const s of [a, b]) for (let i = 0; i < 3; i++) expect((await nextOk(s)).seq).toBe(seqs[i]);
    expect(c.notifyMany([])).toEqual([]);
  });

  it("channel matching: exact, trailing glob, literal mid-star", async () => {
    const c = cache();
    const s = watch(c, ["cache:invalidate:*", "orders"]);
    const mid = watch(c, "a*b");
    const plan: [string, number][] = [
      ["cache:invalidate:user", 1],
      ["cache:invalidate:", 2],
      ["cache:invalidatex", 3],
      ["orders", 4],
      ["orders:new", 5],
      ["axb", 6],
    ];
    for (const [ch, p] of plan) c.notify(ch, p);
    for (const want of [1, 2, 4]) expect((await nextOk(s)).payload).toBe(want);
    expect(await s.next(200)).toBeUndefined();
    expect(await mid.next(50)).toBeUndefined();
  });

  it("validates channels; namespaces are isolated", async () => {
    const path = tempDbPath();
    const c = cache(path);
    for (const bad of ["", "x*", "__cache__", "__x"])
      expect(() => c.notify(bad, 1)).toThrow(TypeError);
    expect(() => c.watch([])).toThrow(TypeError);
    expect(() => c.watch("c", { fromSeq: -1 })).toThrow(TypeError);
    const other = cache(path, { namespace: "other" });
    const s = watch(c, "c");
    other.notify("c", 1);
    c.notify("c", 2);
    expect((await nextOk(s)).payload).toBe(2);
    expect(await s.next(150)).toBeUndefined();
  });

  it("payload round trip and spec-built fixture rows", async () => {
    const path = tempDbPath();
    const c = cache(path);
    const values: unknown[] = [
      Buffer.from("raw"),
      "text",
      42,
      -7,
      3.25,
      { a: [1, null] },
      [1, "x"],
      true,
      null,
    ];
    const s = watch(c, "t");
    for (const v of values) c.notify("t", v);
    for (const v of values) expect((await nextOk(s)).payload).toEqual(v);

    const db = new Database(path);
    db.exec(`INSERT INTO events (channel, namespace, payload, value_type, created_at) VALUES
      ('fx','default',CAST('bytes' AS BLOB),0,1),('fx','default',CAST('42' AS BLOB),2,1),
      ('fx','default',CAST('{"k":[1,2]}' AS BLOB),4,1),('fx','default',NULL,4,1),('fx','default',x'aced',6,1)`);
    const stored = db
      .prepare(
        "SELECT payload, value_type FROM events WHERE channel='t' AND value_type=4 ORDER BY seq LIMIT 1",
      )
      .get() as {
      payload: Buffer;
      value_type: number;
    };
    db.close();
    expect(stored.payload.toString()).toBe('{"a":[1,null]}');
    const fx = watch(c, "fx", { fromSeq: 0 });
    expect((await nextOk(fx)).payload).toEqual(Buffer.from("bytes"));
    expect((await nextOk(fx)).payload).toBe(42);
    expect((await nextOk(fx)).payload).toEqual({ k: [1, 2] });
    expect((await nextOk(fx)).payload).toBeNull();
    const java = await nextOk(fx);
    expect(() => java.payload).toThrow(/value_type=6/);
  });

  it("cursor helpers and gap detection", async () => {
    const c = cache(tempDbPath(), { eventRetention: 0.05 });
    expect(c.loadCursor("w")).toBeUndefined();
    c.saveCursor("w", 41);
    c.saveCursor("w", 42);
    expect(c.loadCursor("w")).toBe(42);
    const old = Array.from({ length: 10 }, (_, i) => c.notify("c", i));
    await sleep(100);
    const fresh = c.notify("c", 99);
    (c as unknown as { sweepOnce(): void }).sweepOnce();
    const s = c.watch("c", { fromSeq: old[3] });
    const err = await s.next(2000).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(GapError);
    const gap = err as GapError;
    expect([gap.requestedSeq, gap.oldestSeq, gap.resumeSeq]).toEqual([old[3], fresh, fresh - 1]);
    expect(s.isClosed).toBe(true);
    const ok = watch(c, "c", { fromSeq: gap.resumeSeq });
    expect((await nextOk(ok)).payload).toBe(99);
    expect((await nextOk(watch(c, "c", { fromSeq: 0 }))).seq).toBe(fresh);
    const ahead = c.watch("c", { fromSeq: fresh + 100 });
    const aheadErr = (await ahead.next(1000).catch((e: unknown) => e)) as GapError;
    expect(aheadErr.ahead).toBe(true);
  });

  it("detects a gap mid-stream and emits it as 'error' in push mode", async () => {
    const path = tempDbPath();
    const sub = cache(path);
    const pub = cache(path, { eventMaxCount: 5 });
    pub.notify("c", "start");
    const s = watch(sub, "c", { pollInterval: 1.5 });
    const errors: unknown[] = [];
    s.on("event", () => {});
    s.on("error", (e) => errors.push(e));
    await sleep(300);
    const seqs = Array.from({ length: 20 }, (_, i) => pub.notify("c", i));
    (pub as unknown as { sweepOnce(): void }).sweepOnce();
    await waitFor(() => errors.length === 1, 10000);
    expect(errors[0]).toBeInstanceOf(GapError);
    const kept = watch(pub, "c", { fromSeq: 0 });
    for (const want of seqs.slice(15)) expect((await nextOk(kept)).seq).toBe(want);
  });

  it("creates no events table and no timers until used; close stops everything", async () => {
    const path = tempDbPath();
    const timersBefore = process.getActiveResourcesInfo().filter((r) => r === "Timeout").length;
    const c = cache(path);
    for (let i = 0; i < 20; i++) c.set(`k${i}`, i);
    c.delete("k1");
    c.flush();
    (c as unknown as { sweepOnce(): void }).sweepOnce();
    expect(eventsTableExists(path)).toBe(false);
    expect(process.getActiveResourcesInfo().filter((r) => r === "Timeout").length).toBe(
      timersBefore,
    );
    c.notify("c", 1); // publishing alone starts no timer
    expect(process.getActiveResourcesInfo().filter((r) => r === "Timeout").length).toBe(
      timersBefore,
    );
    const a = c.watch("a");
    c.watch("b");
    a.close();
    c.close(); // closes the remaining subscription
    await sleep(20);
    expect(process.getActiveResourcesInfo().filter((r) => r === "Timeout").length).toBe(
      timersBefore,
    );
    const db = new Database(path, { readonly: true });
    const meta = Object.fromEntries(
      (db.prepare("SELECT k, v FROM meta").all() as { k: string; v: string }[]).map((r) => [
        r.k,
        r.v,
      ]),
    );
    db.close();
    expect(meta.schema_version).toBe("1");
    expect(meta.events_version).toBe("1");
  });

  it("an in-process notify wakes the watcher immediately", async () => {
    const c = cache();
    const s = watch(c, "c", { pollInterval: 5 });
    await sleep(100);
    const t = Date.now();
    c.notify("c", 1);
    await nextOk(s);
    expect(Date.now() - t).toBeLessThan(1000);
  });
});

function forkWorker(args: string[]): ChildProcess {
  return fork(join(__dirname, "workers", "events-worker.cjs"), args, { stdio: "inherit" });
}

function exited(child: ChildProcess): Promise<number> {
  return new Promise((resolve) => child.on("exit", (code) => resolve(code ?? -1)));
}

describe("notify / watch across processes", () => {
  it("delivers every event, in seq order, to two subscriber processes", async () => {
    const path = tempDbPath();
    cache(path).watch("x").close();
    const dir = tempDir();
    const n = 150;
    const subs = [0, 1].map((i) => {
      const child = forkWorker([
        path,
        "subscribe",
        "orders*",
        String(n),
        join(dir, `out${i}`),
        join(dir, `ready${i}`),
      ]);
      return {
        child,
        done: exited(child),
        out: join(dir, `out${i}`),
        ready: join(dir, `ready${i}`),
      };
    });
    for (const s of subs) await waitFor(() => existsSync(s.ready));
    expect(await exited(forkWorker([path, "publish", String(n)]))).toBe(0);
    for (const s of subs) {
      expect(await s.done).toBe(0);
      const got = JSON.parse(readFileSync(s.out, "utf8")) as {
        seq: number;
        channel: string;
        payload: number;
      }[];
      expect(got.map((e) => e.payload)).toEqual(Array.from({ length: n }, (_, i) => i));
      expect(got.every((e) => e.channel === "orders:new")).toBe(true);
      expect(got.map((e) => e.seq)).toEqual([...got.map((e) => e.seq)].sort((a, b) => a - b));
    }
  }, 60000);

  it("a killed subscriber resumes from its cursor and receives exactly what it missed", async () => {
    const path = tempDbPath();
    const c = cache(path);
    c.watch("jobs").close();
    const ready = join(tempDir(), "ready");
    const child = forkWorker([path, "cursorsub", ready]);
    try {
      await waitFor(() => existsSync(ready));
      const first = c.notify("jobs", "probe");
      await waitFor(() => c.loadCursor("worker") === first);
      const seqs = Array.from({ length: 40 }, (_, i) => c.notify("jobs", i));
      await waitFor(() => (c.loadCursor("worker") ?? 0) >= seqs[10]!);
      child.kill("SIGKILL");
      await exited(child);
      const saved = c.loadCursor("worker")!;
      for (let i = 40; i < 60; i++) seqs.push(c.notify("jobs", i));
      const s = watch(c, "jobs", { fromSeq: saved });
      for (const want of seqs.filter((x) => x > saved)) expect((await nextOk(s)).seq).toBe(want);
      expect(await s.next(200)).toBeUndefined();
    } finally {
      child.kill("SIGKILL");
    }
  }, 60000);
});

describe("onChange and publishChanges", () => {
  function collect(c: LyteCache, events?: ("set" | "delete" | "flush")[]) {
    const got: ChangeRecord[] = [];
    const unsubscribe = c.onChange((r) => void got.push(r), events ? { events } : {});
    return { got, unsubscribe };
  }

  it("a 2000-key setMany emits exactly one truncated record; small bulks carry keys", async () => {
    const c = cache();
    const { got } = collect(c);
    c.setMany(Object.fromEntries(Array.from({ length: 2000 }, (_, i) => [`k${i}`, i])));
    c.setMany({ a: 1, b: 2 });
    c.delete("a", "b", "a");
    c.delete("missing");
    c.flush();
    await sleep(50);
    expect(got).toEqual([
      { op: "set", namespace: "default", keys: null, count: 2000, truncated: true },
      { op: "set", namespace: "default", keys: ["a", "b"], count: 2, truncated: false },
      { op: "delete", namespace: "default", keys: ["a", "b"], count: 2, truncated: false },
      { op: "flush", namespace: "default", keys: null, count: 2000, truncated: true },
    ]);
  });

  it("every value write reports 'set'; no-ops and lock keys report nothing", async () => {
    const c = cache();
    const { got } = collect(c, ["set"]);
    c.set("a", 1);
    c.add("b", 1);
    c.add("b", 2);
    c.replace("a", 2);
    c.replace("missing", 2);
    c.getSet("a", 3);
    c.incr("n");
    c.hset("h", "f", 1);
    c.hsetnx("h", "f", 2);
    c.hincrby("h", "n");
    c.hdel("h", "nope");
    c.delete("a");
    c.lock("job").release();
    await sleep(50);
    expect(got.map((r) => r.keys![0])).toEqual(["a", "b", "a", "a", "n", "h", "h"]);
    expect(() => c.onChange(() => {}, { events: ["expire" as "set"] })).toThrow(TypeError);
  });

  it("is not called when the transaction rolls back", async () => {
    const path = tempDbPath();
    const c = cache(path);
    const db = new Database(path);
    db.exec(
      "CREATE TRIGGER boom BEFORE INSERT ON cache WHEN NEW.key = 'boom' BEGIN SELECT RAISE(ABORT, 'boom'); END",
    );
    db.close();
    const { got } = collect(c);
    expect(() => c.setMany({ a: 1, boom: 2 })).toThrow();
    await sleep(50);
    expect(got).toEqual([]);
    expect(c.exists("a")).toBe(false);
  });

  it("a throwing listener doesn't fail the write and later listeners still run; listeners may write; depth guard", async () => {
    const logs: string[] = [];
    const c = cache(tempDbPath(), { logger: (m) => logs.push(m) });
    c.onChange(() => {
      throw new Error("listener bug");
    });
    const { got } = collect(c);
    c.set("a", 1);
    await sleep(30);
    expect(got).toHaveLength(1);
    expect(logs.some((m) => m.includes("listener bug"))).toBe(true);

    const c2 = cache();
    c2.onChange((r) => {
      if (r.keys?.[0] === "src") c2.set("mirror", c2.get("src"));
    });
    c2.set("src", 42);
    await sleep(30);
    expect(c2.get("mirror")).toBe(42);

    const c3 = cache(tempDbPath(), { logger: (m) => logs.push(m) });
    let calls = 0;
    c3.onChange(
      () => {
        calls += 1;
        c3.incr("depth");
      },
      { events: ["set"] },
    );
    c3.set("start", 1);
    await sleep(200);
    expect(calls).toBe(8);
    expect(c3.get("depth")).toBe(8);
    expect(logs.some((m) => m.includes("levels deep"))).toBe(true);
  });

  it("unsubscribe takes effect immediately, even for queued records", async () => {
    const c = cache();
    const { got, unsubscribe } = collect(c);
    c.set("a", 1);
    unsubscribe();
    unsubscribe();
    await sleep(30);
    expect(got).toEqual([]);
  });

  it("publishChanges writes nothing when unset; publishes per operation when set", async () => {
    const path = tempDbPath();
    const plain = cache(path);
    plain.set("a", 1);
    plain.delete("a");
    expect(eventsTableExists(path)).toBe(false);
    const c = cache(path, { publishChanges: ["delete", "flush"] });
    const s = watch(c, "__cache__");
    c.set("a", 1);
    c.setMany(Object.fromEntries(Array.from({ length: 150 }, (_, i) => [`k${i}`, i])));
    c.delete("a", "missing");
    c.flush();
    expect((await nextOk(s)).payload).toEqual({
      op: "delete",
      namespace: "default",
      keys: ["a", "missing"],
      count: 1,
      truncated: false,
    });
    expect((await nextOk(s)).payload).toEqual({
      op: "flush",
      namespace: "default",
      keys: null,
      count: 150,
      truncated: true,
    });
    expect(await s.next(200)).toBeUndefined();
    expect(() => new LyteCache({ path, publishChanges: ["expire" as "set"] })).toThrow(TypeError);
  });

  it("the invalidation recipe runs as written", async () => {
    const path = tempDbPath();
    const workerA = cache(path);
    const workerB = cache(path);
    const localMemo = new Map([
      ["balance:42", 100],
      ["balance:7", 5],
    ]);
    {
      using sub = workerB.watch("invalidate");
      // worker A, after committing a DB change
      workerA.delete("balance:42");
      workerA.notify("invalidate", { key: "balance:42" });
      // workers B..N
      for await (const event of sub) {
        localMemo.delete((event.payload as { key: string }).key);
        break;
      }
    }
    expect([...localMemo.keys()]).toEqual(["balance:7"]);
  });
});
