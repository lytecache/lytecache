import { fork } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { LyteCache, SerializationError } from "../src/index.js";
import { tempDbPath } from "./helpers.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const WEIRD = [
  "a.b",
  "a[0]",
  "a]b",
  'q"uote',
  "back\\slash",
  '\\"',
  "sp ace",
  "ünï 字",
  "",
  "$",
  "*",
  "1",
  "2",
];

function raw(path: string, key: string) {
  const db = new Database(path, { readonly: true });
  try {
    return db
      .prepare(
        "SELECT value, value_type AS vt, expires_at AS exp, size_bytes AS size, length(value) AS len, last_accessed AS la, access_count AS ac FROM cache WHERE namespace='default' AND key=?",
      )
      .get(key) as
      | {
          value: Buffer;
          vt: number;
          exp: number | null;
          size: number;
          len: number;
          la: number;
          ac: number;
        }
      | undefined;
  } finally {
    db.close();
  }
}

function insertRaw(path: string, key: string, blob: Buffer, vt: number): void {
  const db = new Database(path);
  const now = Date.now();
  db.prepare(
    "INSERT INTO cache (namespace, key, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes) VALUES ('default', ?, ?, ?, ?, NULL, ?, 0, ?)",
  ).run(key, blob, vt, now, now, blob.length);
  db.close();
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

for (const mode of ["json1", "fallback"] as const) {
  describe(`hashes (${mode})`, () => {
    let path: string;
    let c: LyteCache;
    beforeEach(() => {
      if (mode === "fallback") process.env.LYTECACHE_FORCE_JSON_FALLBACK = "1";
      else delete process.env.LYTECACHE_FORCE_JSON_FALLBACK;
      path = tempDbPath();
      c = new LyteCache({ path, sweepInterval: null, logger: () => {} });
      expect(c.hashMode).toBe(mode);
      expect(c.stats().hashMode).toBe(mode);
    });
    afterEach(() => {
      c.close();
      delete process.env.LYTECACHE_FORCE_JSON_FALLBACK;
    });

    it("round-trips natural types and reports created", () => {
      const values: Record<string, unknown> = {
        s: "text",
        i: 42,
        f: 1.5,
        t: true,
        n: null,
        o: { x: [1, 2] },
        l: [1, "a"],
      };
      for (const [f, v] of Object.entries(values)) expect(c.hset("h", f, v)).toBe(true);
      expect(c.hset("h", "s", "again")).toBe(false);
      expect(c.hget("h", "s")).toBe("again");
      expect(c.hget("h", "o")).toEqual({ x: [1, 2] });
      expect(c.hget("h", "n", "dflt")).toBeNull();
      expect(c.hget("h", "missing", "dflt")).toBe("dflt");
      expect(c.get("h")).toEqual({ ...values, s: "again" });
    });

    it("is a view over ordinary JSON values", () => {
      c.set("u", { name: "Ada" });
      expect(c.hget("u", "name")).toBe("Ada");
      c.hset("u", "status", "active");
      expect(c.get("u")).toEqual({ name: "Ada", status: "active" });
    });

    it("hsetMany / hmget / hdel / hkeys / hvals / hlen / hexists", () => {
      c.hsetMany("h", { a: 1, b: "two", c: null });
      expect(c.hmget("h", ["b", "nope", "a"])).toEqual(["two", undefined, 1]);
      expect(c.hkeys("h")).toEqual(["a", "b", "c"]);
      expect(c.hvals("h")).toEqual([1, "two", null]);
      expect(c.hlen("h")).toBe(3);
      expect(c.hexists("h", "c")).toBe(true);
      expect(c.hdel("h", "a", "nope", "a")).toBe(1);
      expect(c.hdel("h", "b", "c")).toBe(2);
      expect(c.get("h")).toEqual({});
      expect(c.hgetAll("missing")).toEqual({});
    });

    it("hsetMany beyond SQLite's limits", () => {
      const m: Record<string, number> = {};
      for (let i = 0; i < 1234; i++) m[`f${i}`] = i;
      c.hsetMany("big", m);
      expect(c.hlen("big")).toBe(1234);
      expect(c.hget("big", "f777")).toBe(777);
    });

    it("hsetnx", () => {
      expect(c.hsetnx("h", "a", 1)).toBe(true);
      expect(c.hsetnx("h", "a", 2)).toBe(false);
      expect(c.hget("h", "a")).toBe(1);
    });

    it("reads on a missing key create nothing", () => {
      expect(c.hget("missing", "f")).toBeUndefined();
      expect(c.hkeys("missing")).toEqual([]);
      expect(c.hlen("missing")).toBe(0);
      expect(c.hdel("missing", "f")).toBe(0);
      expect(raw(path, "missing")).toBeUndefined();
    });

    it("creates missing keys with no expiry unless a ttl is given; ttl ignored for existing keys", () => {
      c.hset("plain", "f", 1);
      expect(c.ttl("plain")).toBe(-1);
      c.hset("t", "f", 1, { ttl: 100 });
      const before = c.ttl("t")!;
      c.hset("t", "g", 2, { ttl: 5 });
      expect(c.ttl("t")!).toBeGreaterThan(50);
      expect(c.ttl("t")!).toBeLessThanOrEqual(before);
    });

    it("never resurrects an expired key", async () => {
      c.set("h", { old: 1 }, { ttl: 0.01 });
      await sleep(40);
      expect(c.hget("h", "old")).toBeUndefined();
      expect(c.hsetnx("h", "new", 1)).toBe(true);
      expect(c.hgetAll("h")).toEqual({ new: 1 });
      expect(c.ttl("h")).toBe(-1);
      c.set("n", { n: 100 }, { ttl: 0.01 });
      await sleep(40);
      expect(c.hincrby("n", "n")).toBe(1);
    });

    it.each([
      [Buffer.from([0, 1]), 0],
      [Buffer.from("hello"), 1],
      [Buffer.from("42"), 2],
      [Buffer.from("3.14"), 3],
      [Buffer.from([0x80, 4]), 5],
      [Buffer.from([0xac, 0xed]), 6],
    ])("rejects value_type %#", (blob, vt) => {
      insertRaw(path, "k", blob, vt);
      const ops = [
        () => c.hset("k", "f", 1),
        () => c.hsetMany("k", { f: 1 }),
        () => c.hsetnx("k", "f", 1),
        () => c.hincrby("k", "f"),
        () => c.hincrbyfloat("k", "f", 1),
        () => c.hdel("k", "f"),
        () => c.hget("k", "f"),
        () => c.hgetAll("k"),
        () => c.hkeys("k"),
        () => c.hvals("k"),
        () => c.hlen("k"),
      ];
      for (const op of ops) expect(op).toThrow(new RegExp(`value_type=${vt}`));
      for (const op of ops) expect(op).toThrow(SerializationError);
      const r = raw(path, "k")!;
      expect(r.value.equals(blob)).toBe(true);
      expect(r.vt).toBe(vt);
    });

    it("rejects non-object roots and malformed JSON, untouched", () => {
      c.set("arr", [1, 2]);
      expect(() => c.hset("arr", "f", 1)).toThrow(/not a JSON object/);
      c.set("bool", true);
      expect(() => c.hget("bool", "f")).toThrow(/not a JSON object/);
      insertRaw(path, "bad", Buffer.from("{not json"), 4);
      expect(() => c.hset("bad", "f", 1)).toThrow(/malformed JSON/);
      expect(raw(path, "bad")!.value.toString()).toBe("{not json");
      expect(() => c.hset("h", "f", Buffer.from("x"))).toThrow(SerializationError);
      expect(() => c.hset("h", "f", Number.NaN)).toThrow(SerializationError);
    });

    it.each(WEIRD)("field name %j addresses exactly one top-level key", (field) => {
      c.set("h", { a: { b: "decoy" }, other: 0 });
      expect(c.hset("h", field, "v")).toBe(true);
      expect(c.hget("h", field)).toBe("v");
      expect(c.hexists("h", field)).toBe(true);
      const doc = c.get("h") as Record<string, unknown>;
      expect(doc[field]).toBe("v");
      expect(doc.a).toEqual({ b: "decoy" });
      expect(c.hkeys("h")).toContain(field);
      expect(c.hincrby("h", `${field}#n`, 2)).toBe(2);
      expect(c.hdel("h", field)).toBe(1);
      expect(c.hget("h", field)).toBeUndefined();
    });

    it("keeps document order, including integer-like keys", () => {
      c.set("h", {});
      for (const f of ["z", "2", "a", "1"]) c.hset("h", f, f);
      expect(c.hkeys("h")).toEqual(["z", "2", "a", "1"]);
    });

    it("preserves TTL, tracks size_bytes and counts one access per call", async () => {
      c.set("h", { a: 1 }, { ttl: 60 });
      const r0 = raw(path, "h")!;
      await sleep(5);
      const ops = [
        () => c.hset("h", "b", "字字字字"),
        () => c.hsetMany("h", { c: 3, d: [1, 2] }),
        () => c.hsetnx("h", "e", 4),
        () => c.hincrby("h", "n", 7),
        () => c.hincrbyfloat("h", "x", 1.25),
        () => c.hdel("h", "a", "c"),
      ];
      for (const op of ops) {
        op();
        const r = raw(path, "h")!;
        expect(r.exp).toBe(r0.exp);
        expect(r.size).toBe(r.len);
        expect(r.size).toBe(r.value.length);
      }
      const r = raw(path, "h")!;
      expect(r.ac).toBe(r0.ac + ops.length);
      expect(r.la).toBeGreaterThan(r0.la);
      c.hget("h", "b");
      c.hgetAll("h");
      expect(raw(path, "h")!.ac).toBe(r.ac); // reads are buffered, not written
    });

    it("hincrby: integer-ness, type errors, int64 boundary", () => {
      expect(c.hincrby("h", "n")).toBe(1);
      expect(c.hincrby("h", "n", 4)).toBe(5);
      expect(c.hincrby("h", "n", -10)).toBe(-5);
      expect(raw(path, "h")!.value.toString()).toBe('{"n":-5}');
      for (const bad of ["5", true, null, { a: 1 }, [1], 1.5]) {
        c.hset("h", "bad", bad);
        expect(() => c.hincrby("h", "bad")).toThrow(TypeError);
      }
      c.hset("h", "max", 9223372036854775806n);
      expect(c.hincrby("h", "max")).toBe(9223372036854775807n);
      const before = raw(path, "h")!.value.toString();
      expect(() => c.hincrby("h", "max")).toThrow(RangeError);
      expect(raw(path, "h")!.value.toString()).toBe(before);
      c.hset("h", "min", -9223372036854775807n);
      expect(c.hincrby("h", "min", -1)).toBe(-9223372036854775808n);
      expect(() => c.hincrby("h", "min", -1)).toThrow(RangeError);
      expect(() => c.hincrby("h", "x", 2n ** 63n)).toThrow(RangeError);
      expect(() => c.hincrby("h", "x", 1.5)).toThrow(TypeError);
    });

    it("hincrbyfloat", () => {
      expect(c.hincrbyfloat("h", "x", 0.1)).toBe(0.1);
      expect(c.hincrbyfloat("h", "x", 0.2)).toBe(0.1 + 0.2);
      c.hset("h", "i", 2);
      expect(c.hincrbyfloat("h", "i", 0.5)).toBe(2.5);
      c.hset("h", "s", "str");
      expect(() => c.hincrbyfloat("h", "s", 1)).toThrow(TypeError);
      c.hset("h", "huge", 1.7e308);
      expect(() => c.hincrbyfloat("h", "huge", 1.7e308)).toThrow(RangeError);
    });

    it("stores canonical compact JSON and reads other encoders' formatting", () => {
      c.hset("h", "name", "Adé");
      c.hset("h", "n", 1);
      expect(raw(path, "h")!.value.toString()).toBe('{"name":"Adé","n":1}');
      insertRaw(
        path,
        "foreign",
        Buffer.from('{ "a" : 1,\n "b.c": [true, null], "u": "\\u00e9" }'),
        4,
      );
      expect(c.hincrby("foreign", "a")).toBe(2);
      expect(c.get("foreign")).toEqual({ a: 2, "b.c": [true, null], u: "é" });
    });

    it("loses no updates across processes", async () => {
      c.set("shared", { other: "untouched" });
      const run = (args: string[]) =>
        new Promise<number>((resolve, reject) => {
          const child = fork(join(__dirname, "workers", "hash-worker.cjs"), [path, ...args], {
            stdio: "inherit",
            env: { ...process.env },
          });
          child.on("error", reject);
          child.on("exit", (code) => resolve(code ?? -1));
        });
      const codes = await Promise.all([
        ...[0, 1, 2, 3].map((i) => run(["hset", "shared", `f${i}`, "100"])),
        ...[0, 1, 2, 3].map(() => run(["hincrby", "shared", "n", "100"])),
      ]);
      expect(codes.every((x) => x === 0)).toBe(true);
      expect(c.hgetAll("shared")).toEqual({
        other: "untouched",
        f0: 99,
        f1: 99,
        f2: 99,
        f3: 99,
        n: 400,
      });
    }, 60000);
  });
}
