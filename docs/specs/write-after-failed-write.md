# Test spec: a write after a failed write must be committed

**Applies to:** every lytecache implementation (Python, Java, Node.js, Go, PHP).
**Status:** required regression test. Add it to each suite as written here.

## Why this test exists

In lytecache-php 0.2.0, one failed write could silently lose every later write.
A write failed inside its transaction (in the reproduction, a constraint
violation). `pdo_sqlite` left the cached prepared statement un-reset. The next
`bindValue()` + `execute()` on that statement then returned `false` **without
raising**, even with exceptions enabled. The library never checked the return
value, so `set()` returned normally and stored nothing. This held for every
later write through that statement, on every PHP version from 8.2 to 8.5. It
was fixed in 0.2.1.

The pattern isn't PHP-specific: any implementation that **reuses a prepared
statement after a failed or rolled-back execution** can hit it, depending on
what its driver does on error. Every implementation must prove it doesn't.

## Current status (audited 2026-10-23)

Each implementation was run through the full matrix below against the code at
`main`.

| Implementation | Driver | SQLite | Result | Why it's safe (or wasn't) |
|---|---|---|---|---|
| PHP 0.2.0 | `pdo_sqlite` | 3.46.1, 3.53 | **lost writes** | Cached `PDOStatement`, not reset after a failed `execute()`; return value ignored |
| PHP 0.2.1+ | `pdo_sqlite` | 3.44–3.53 | 25/25 | Statement class resets before every execute and throws on a `false` return |
| Python | `sqlite3` (stdlib) | 3.53.3 | 25/25 | The module's internal statement cache resets statements after errors |
| Node.js | `better-sqlite3` | 3.49.2 | 25/25 | `prepare()` per call; better-sqlite3 resets a statement after every run, including a failed one |
| Go | `database/sql` + `modernc.org/sqlite` | 3.53.2 | 25/25 | Statements prepared per transaction (`tx.Prepare`) and closed with it |
| Java | `sqlite-jdbc` | 3.53.4 | 25/25 | `prepareStatement()` per call in try-with-resources; no statement reuse |

"Safe today" depends on driver behaviour you don't control and on caching you
might add later (statement caching is the obvious optimisation). That's why the
test is required everywhere, not only in PHP.

## The test

### Setup, per case

1. Create a fresh cache file at a temporary path. Disable the background
   sweeper (in Java use a long interval: `sweepInterval(Duration.ZERO)` throws).
2. Open a **second, independent** raw SQLite connection to the same file (not
   through lytecache), and through it create this trigger:

   ```sql
   CREATE TRIGGER fail_boom BEFORE INSERT ON cache
   WHEN NEW.key = 'boom'
   BEGIN SELECT RAISE(ABORT, 'forced failure'); END
   ```

   It makes any write that inserts the key `boom` fail inside its transaction,
   the same way a real constraint violation or trigger would.
3. Run one **failing operation** (table below) and assert that it raised the
   implementation's database error. If it didn't raise, the test is broken;
   fail it.
4. Run one **following operation** on the key `after`, through the **same
   cache instance**. Run it **twice**: the second reuse of a statement must
   work too.
5. Assert, in order:
   - **Committed:** through the raw connection from step 2, `SELECT count(*)
     FROM cache WHERE key = 'after'` returns `1`. Never assert this through the
     cache instance under test; a connection can see its own uncommitted data.
   - **Readable:** the cache instance returns the expected value for `after`.
   - **Durable:** after closing the cache and opening a new instance on the
     same file, it returns the expected value.

### The matrix: 5 failing operations x 5 following operations = 25 cases

| Failing operation (on `boom`) | Following operation (on `after`, run twice) | Expected value of `after` |
|---|---|---|
| `set("boom", "x")` | `set("after", "v")` | `"v"` |
| `set_many({"ok": 1, "boom": 2})` | `set_many({"after": "v"})` | `"v"` |
| `add("boom", "x")` | `add("after", "v")` | `"v"` (second `add` returns false; the value stays) |
| `incr("boom")` | `incr("after", 7)` | `14` (two increments) |
| `hset("boom", "f", 1)` | `hset("after", "f", "v")` | `{"f": "v"}` |

Use each language's own names (`setMany`/`SetMany`/`setAll`, `incr`/`Incr`,
`hset`/`HSet`). Every pairing must be covered, not just the diagonal. In PHP the
bug showed up only when the *same* SQL ran again, but which statements are
shared is an implementation detail the test must not assume.

### Three more cases

- **The failed batch leaves nothing.** After `set_many({"ok-before": 1, "boom":
  2})` fails, the raw connection sees no `ok-before` row: a rolled-back batch is
  never partially applied.
- **A re-entrant write.** After a failed `set_many`, register a change listener
  that, on a `set` of `src`, writes `mirror` with the value of `src`. Then
  `set("src", 42)`. Both `src` and `mirror` must be committed (raw
  connection). This was the first symptom found in PHP.
- **The statement is still usable.** In the PHP bug, the wrong behaviour after
  a failure wasn't always silence: a following `incr` raised "not numeric" on a
  key that didn't exist, and a following `hset` raised "API misuse". Any error
  from a following operation fails the test; report the error text.

### Rules

- **No version gating.** The test runs on every runtime and SQLite version in
  the matrix. It describes behaviour that must hold everywhere.
- **Failure messages name the pair**, for example `set fails, then incr: not
  committed`, so a regression points straight at the statement involved.
- **Prove it bites.** When adding it, temporarily break the implementation (for
  example, cache one statement and skip its reset) and confirm the test fails.
  A test that has never failed hasn't shown it can.

## Pseudocode

```text
TRIGGER = "CREATE TRIGGER fail_boom BEFORE INSERT ON cache WHEN NEW.key = 'boom'
           BEGIN SELECT RAISE(ABORT, 'forced failure'); END"

for (failName, fail) in FAILING_OPS:
  for (nextName, next, key, expected) in FOLLOWING_OPS:
    path  = new temp file
    cache = open lytecache(path, no sweeper)
    raw   = open raw sqlite connection(path)
    raw.exec(TRIGGER)

    expect_database_error(fail(cache))            # else: test is broken
    next(cache); next(cache)                      # any error here fails the test

    assert raw.query("SELECT count(*) FROM cache WHERE key = ?", key) == 1,
           "{failName} fails, then {nextName}: not committed"
    assert cache.get(key) == expected
    cache.close()
    assert open lytecache(path).get(key) == expected
```

## What to check in each codebase

When reviewing an implementation, or adding statement caching to one:

1. **Where are statements prepared, and are any reused?** Look for a statement
   cache (a map from SQL text to a prepared statement), or a statement held on
   an object across calls.
2. **What does the driver do with a statement after a failed step?** Some reset
   it, and some (`pdo_sqlite`) leave it in an error state that the next
   execution doesn't clear.
3. **Is every reused statement reset unconditionally before it runs?** Resetting
   "on the error path" depends on finding every error path; resetting before
   each execution doesn't.
4. **Is the driver's success signal checked?** If an execute call can report
   failure through a return value instead of an exception, treat that as an
   error. A write must never fail silently.
5. **After a rollback, is anything left that the next transaction will reuse?**
   Statements, cursors, or a transaction flag the library tracks itself.

Per implementation, at the time of the audit:

- **Python** (`lytecache-python/src/lytecache/core.py`): one connection per
  thread (`threading.local`), and the `sqlite3` module's own statement cache.
  Nothing lytecache-level is reused. Watch for any future explicit statement
  objects.
- **Node.js** (`lytecache-node/src/core.ts`): `this.db.prepare(...)` at each call
  site. Adding a prepared-statement cache would be safe with better-sqlite3's
  reset-on-run, but the test must stay to prove it.
- **Go** (`lytecache-go/kv.go`, `sweep.go`): `tx.Prepare` scoped to one
  transaction. A `db.Prepare` held on `Cache` would be the risky change.
- **Java** (`LyteCache.java`): `conn.prepareStatement(...)` in
  try-with-resources at each call site. A `PreparedStatement` field or cache
  would be the risky change.
- **PHP** (`lytecache-php/src/Support/Statement.php`): fixed by resetting every
  statement before execution (`PDO::ATTR_STATEMENT_CLASS`) and throwing on a
  `false` return. The reference test is
  `tests/Feature/WriteAfterFailedWriteTest.php`.
