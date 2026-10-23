package io.lytecache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** notify / watch (local, polled, durable events) and onChange / publishChanges. */
public class EventsTest {
    @TempDir
    Path dir;

    private final List<AutoCloseable> opened = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : opened) {
            c.close();
        }
    }

    private Path db() {
        return dir.resolve("cache.db");
    }

    private LyteCache open(LyteCache.Builder b) {
        LyteCache c = b.path(db()).sweepInterval(null).build();
        opened.add(c);
        return c;
    }

    private LyteCache open() {
        return open(LyteCache.builder());
    }

    private Subscription watch(LyteCache c, WatchOptions o, String... patterns) {
        Subscription s = c.watch(o, patterns);
        opened.add(s);
        return s;
    }

    private static Event next(Subscription s) {
        Event e = s.poll(Duration.ofSeconds(5));
        assertThat(e).as("expected an event").isNotNull();
        return e;
    }

    private static void waitFor(BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (!cond.getAsBoolean()) {
            assertThat(System.currentTimeMillis()).as("timed out").isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    private boolean eventsTableExists() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db()); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'")) {
            return rs.next();
        }
    }

    private static List<Thread> threadsNamed(String name) {
        List<Thread> out = new ArrayList<>();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().equals(name) && t.isAlive()) {
                out.add(t);
            }
        }
        return out;
    }

    @Test
    void deliversOnlyNewEventsByDefault() {
        LyteCache c = open();
        c.notify("orders", "before");
        Subscription s = watch(c, WatchOptions.defaults(), "orders");
        assertThat(s.poll(Duration.ofMillis(150))).isNull();
        long s1 = c.notify("orders", Map.of("id", 1));
        long s2 = c.notify("orders", null);
        Event e1 = next(s);
        Event e2 = next(s);
        assertThat(e1.seq()).isEqualTo(s1);
        assertThat(e1.channel()).isEqualTo("orders");
        assertThat(e1.namespace()).isEqualTo("default");
        assertThat(e1.payload()).isEqualTo(Map.of("id", 1));
        assertThat(e2.isNullPayload()).isTrue();
        assertThat(e2.payload()).isNull();
        assertThat(s.lastSeq()).isEqualTo(s2);
    }

    @Test
    void fromSeqZeroReplaysAndFromSeqNResumes() {
        LyteCache c = open();
        List<Long> seqs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            seqs.add(c.notify("c", (long) i));
        }
        Subscription all = watch(c, WatchOptions.defaults().fromSeq(0), "c");
        for (long seq : seqs) {
            assertThat(next(all).seq()).isEqualTo(seq);
        }
        Subscription after = watch(c, WatchOptions.defaults().fromSeq(seqs.get(1)), "c");
        for (long i = 2; i < 5; i++) {
            assertThat(next(after).payload()).isEqualTo(i);
        }
        assertThat(after.poll(Duration.ofMillis(150))).isNull();
    }

    @Test
    void blockingIterationTryNextAndPushMode() throws Exception {
        LyteCache c = open();
        Subscription s = watch(c, WatchOptions.defaults(), "c");
        assertThat(s.tryNext()).isNull();
        List<Object> got = new CopyOnWriteArrayList<>();
        Thread consumer = new Thread(() -> {
            for (Event e : s) {
                got.add(e.payload());
            }
        });
        consumer.start();
        c.notify("c", "x");
        waitFor(() -> got.size() == 1);
        s.close();
        s.close();
        consumer.join(5000);
        assertThat(consumer.isAlive()).isFalse();
        assertThat(got).containsExactly("x");

        List<Object> pushed = new CopyOnWriteArrayList<>();
        Subscription push = watch(c, WatchOptions.defaults(), "p").onEvent(e -> pushed.add(e.payload()));
        c.notify("p", "a");
        c.notify("p", "b");
        waitFor(() -> pushed.size() == 2);
        assertThat(pushed).containsExactly("a", "b");
        assertThat(push.tryNext()).isNull();
    }

    @Test
    void orderingFanOutNotifyManyAndMatching() {
        LyteCache c = open();
        Subscription a = watch(c, WatchOptions.defaults(), "a", "b");
        Subscription star = watch(c, WatchOptions.defaults(), "*");
        List<Long> seqs = c.notifyMany(List.of(new Notification("a", 1), new Notification("b", 2), new Notification("a", 3)));
        assertThat(seqs).isSorted().hasSize(3);
        for (Subscription s : List.of(a, star)) {
            for (long seq : seqs) {
                assertThat(next(s).seq()).isEqualTo(seq);
            }
        }
        Subscription glob = watch(c, WatchOptions.defaults(), "cache:invalidate:*", "orders");
        Subscription mid = watch(c, WatchOptions.defaults(), "a*b");
        c.notify("cache:invalidate:user", 1L);
        c.notify("cache:invalidate:", 2L);
        c.notify("cache:invalidatex", 3L);
        c.notify("orders", 4L);
        c.notify("orders:new", 5L);
        c.notify("axb", 6L);
        for (long want : new long[] {1, 2, 4}) {
            assertThat(next(glob).payload()).isEqualTo(want);
        }
        assertThat(glob.poll(Duration.ofMillis(200))).isNull();
        assertThat(mid.poll(Duration.ofMillis(50))).isNull();
    }

    @Test
    void validatesChannelsAndIsolatesNamespaces() {
        LyteCache c = open();
        for (String bad : new String[] {"", "x*", "__cache__", "__x"}) {
            assertThatThrownBy(() -> c.notify(bad, 1)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(c::watch).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WatchOptions.defaults().fromSeq(-1)).isInstanceOf(IllegalArgumentException.class);
        LyteCache other = open(LyteCache.builder().namespace("other"));
        Subscription s = watch(c, WatchOptions.defaults(), "c");
        other.notify("c", 1L);
        c.notify("c", 2L);
        assertThat(next(s).payload()).isEqualTo(2L);
        assertThat(s.poll(Duration.ofMillis(150))).isNull();
    }

    @Test
    void payloadTypesAndSpecFixtureRows() throws Exception {
        LyteCache c = open();
        Subscription s = watch(c, WatchOptions.defaults(), "t");
        c.notify("t", "raw".getBytes());
        c.notify("t", "text");
        c.notify("t", -7L);
        c.notify("t", 3.25);
        c.notify("t", Map.of("a", List.of(1)));
        assertThat((byte[]) next(s).payload()).isEqualTo("raw".getBytes());
        assertThat(next(s).payload()).isEqualTo("text");
        assertThat(next(s).payload()).isEqualTo(-7L);
        assertThat(next(s).payload()).isEqualTo(3.25);
        assertThat(next(s).payload()).isEqualTo(Map.of("a", List.of(1)));
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + db()); Statement st = conn.createStatement()) {
            st.execute("INSERT INTO events (channel, namespace, payload, value_type, created_at) VALUES "
                    + "('fx','default',CAST('bytes' AS BLOB),0,1),('fx','default',CAST('42' AS BLOB),2,1),"
                    + "('fx','default',CAST('{\"k\":[1,2]}' AS BLOB),4,1),('fx','default',NULL,4,1),('fx','default',x'aced',6,1)");
            try (ResultSet rs = st.executeQuery("SELECT payload, value_type FROM events WHERE channel='t' AND value_type=4")) {
                rs.next();
                assertThat(new String(rs.getBytes(1))).isEqualTo("{\"a\":[1]}");
            }
        }
        Subscription fx = watch(c, WatchOptions.defaults().fromSeq(0), "fx");
        assertThat((byte[]) next(fx).payload()).isEqualTo("bytes".getBytes());
        assertThat(next(fx).payload()).isEqualTo(42L);
        assertThat(next(fx).payload()).isEqualTo(Map.of("k", List.of(1, 2)));
        assertThat(next(fx).payload()).isNull();
        Event java = next(fx);
        assertThatThrownBy(java::payload).isInstanceOf(SerializationException.class);
    }

    @Test
    void cursorHelpersAndGapDetection() throws Exception {
        LyteCache c = open(LyteCache.builder().eventRetention(Duration.ofMillis(50)));
        assertThat(c.loadCursor("w")).isEmpty();
        c.saveCursor("w", 41);
        c.saveCursor("w", 42);
        assertThat(c.loadCursor("w").getAsLong()).isEqualTo(42);
        List<Long> old = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            old.add(c.notify("c", (long) i));
        }
        Thread.sleep(100);
        long fresh = c.notify("c", 99L);
        sweep(c);
        Subscription s = c.watch(WatchOptions.defaults().fromSeq(old.get(3)), "c");
        GapException gap = (GapException) catchThrown(() -> s.poll(Duration.ofSeconds(2)));
        assertThat(gap.requestedSeq()).isEqualTo(old.get(3));
        assertThat(gap.oldestSeq()).isEqualTo(fresh);
        assertThat(gap.resumeSeq()).isEqualTo(fresh - 1);
        assertThat(s.isClosed()).isTrue();
        assertThat(next(watch(c, WatchOptions.defaults().fromSeq(gap.resumeSeq()), "c")).payload()).isEqualTo(99L);
        assertThat(next(watch(c, WatchOptions.defaults().fromSeq(0), "c")).seq()).isEqualTo(fresh);
        Subscription ahead = c.watch(WatchOptions.defaults().fromSeq(fresh + 100), "c");
        assertThat(((GapException) catchThrown(() -> ahead.poll(Duration.ofSeconds(1)))).ahead()).isTrue();
    }

    @Test
    void gapDetectedMidStreamAndCountCap() throws Exception {
        LyteCache sub = open();
        LyteCache pub = open(LyteCache.builder().eventMaxCount(5));
        pub.notify("c", "start");
        Subscription s = watch(sub, WatchOptions.defaults().pollInterval(Duration.ofMillis(1500)), "c");
        Thread.sleep(300);
        List<Long> seqs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            seqs.add(pub.notify("c", (long) i));
        }
        sweep(pub);
        assertThat(catchThrown(() -> s.poll(Duration.ofSeconds(10)))).isInstanceOf(GapException.class);
        Subscription kept = watch(pub, WatchOptions.defaults().fromSeq(0), "c");
        for (long want : seqs.subList(15, 20)) {
            assertThat(next(kept).seq()).isEqualTo(want);
        }
    }

    @Test
    void noTableOrThreadUntilUsedAndCloseStopsEverything() throws Exception {
        LyteCache c = LyteCache.builder().path(db()).sweepInterval(null).build();
        for (int i = 0; i < 20; i++) {
            c.set("k" + i, (long) i);
        }
        c.delete("k1");
        c.flush();
        sweep(c);
        assertThat(eventsTableExists()).isFalse();
        assertThat(threadsNamed("lytecache-watch")).isEmpty();
        assertThat(threadsNamed("lytecache-dispatch")).isEmpty();
        c.notify("c", 1);
        assertThat(threadsNamed("lytecache-watch")).isEmpty();
        Subscription a = c.watch("a");
        c.watch("b");
        c.onChange(r -> { });
        assertThat(threadsNamed("lytecache-watch")).hasSize(2);
        assertThat(threadsNamed("lytecache-dispatch")).hasSize(1);
        a.close();
        c.close(); // closes the other subscription and the dispatcher
        waitFor(() -> threadsNamed("lytecache-watch").isEmpty() && threadsNamed("lytecache-dispatch").isEmpty());
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + db()); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT k, v FROM meta")) {
            Map<String, String> meta = new HashMap<>();
            while (rs.next()) {
                meta.put(rs.getString(1), rs.getString(2));
            }
            assertThat(meta).containsEntry("schema_version", "1").containsEntry("events_version", "1");
        }
    }

    @Test
    void inProcessNotifyWakesImmediately() throws Exception {
        LyteCache c = open();
        Subscription s = watch(c, WatchOptions.defaults().pollInterval(Duration.ofSeconds(5)), "c");
        Thread.sleep(100);
        long t = System.currentTimeMillis();
        c.notify("c", 1);
        next(s);
        assertThat(System.currentTimeMillis() - t).isLessThan(1000);
    }

    @Test
    void crossProcessDeliveryToTwoSubscribers() throws Exception {
        open().watch("x").close();
        int n = 120;
        List<Process> subs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            subs.add(HashTest.startJava("json1", EventWorker.class, db().toString(), "subscribe", "orders*", String.valueOf(n),
                    dir.resolve("out" + i).toString(), dir.resolve("ready" + i).toString()));
        }
        for (int i = 0; i < 2; i++) {
            Path ready = dir.resolve("ready" + i);
            waitFor(() -> Files.exists(ready));
        }
        Process pub = HashTest.startJava("json1", EventWorker.class, db().toString(), "publish", String.valueOf(n));
        assertThat(pub.waitFor(60, TimeUnit.SECONDS)).isTrue();
        assertThat(pub.exitValue()).isZero();
        for (int i = 0; i < 2; i++) {
            assertThat(subs.get(i).waitFor(60, TimeUnit.SECONDS)).isTrue();
            assertThat(subs.get(i).exitValue()).isZero();
            List<String> lines = Files.readAllLines(dir.resolve("out" + i));
            assertThat(lines).hasSize(n);
            long prev = 0;
            for (int j = 0; j < n; j++) {
                String[] f = lines.get(j).split(" ");
                assertThat(f[1]).isEqualTo("orders:new");
                assertThat(f[2]).isEqualTo(String.valueOf(j));
                assertThat(Long.parseLong(f[0])).isGreaterThan(prev);
                prev = Long.parseLong(f[0]);
            }
        }
    }

    @Test
    void killedSubscriberResumesFromItsCursor() throws Exception {
        LyteCache c = open();
        c.watch("jobs").close();
        Path ready = dir.resolve("ready");
        Process p = HashTest.startJava("json1", EventWorker.class, db().toString(), "cursorsub", ready.toString());
        try {
            waitFor(() -> Files.exists(ready));
            long first = c.notify("jobs", "probe");
            waitFor(() -> c.loadCursor("worker").orElse(0) == first);
            List<Long> seqs = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                seqs.add(c.notify("jobs", (long) i));
            }
            waitFor(() -> c.loadCursor("worker").orElse(0) >= seqs.get(10));
            p.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
            long saved = c.loadCursor("worker").getAsLong();
            for (int i = 40; i < 60; i++) {
                seqs.add(c.notify("jobs", (long) i));
            }
            Subscription s = watch(c, WatchOptions.defaults().fromSeq(saved), "jobs");
            for (long want : seqs) {
                if (want > saved) {
                    assertThat(next(s).seq()).isEqualTo(want);
                }
            }
            assertThat(s.poll(Duration.ofMillis(200))).isNull();
        } finally {
            p.destroyForcibly();
        }
    }

    @Test
    void notifyStreamAlongsideWritesLosesNothing() throws Exception {
        LyteCache c = open();
        c.watch("x").close();
        List<Process> procs = new ArrayList<>();
        procs.add(HashTest.startJava("json1", EventWorker.class, db().toString(), "publish", "200"));
        for (int i = 0; i < 2; i++) {
            procs.add(HashTest.startJava("json1", IncrWorker.class, db().toString(), "counter", "200"));
        }
        for (Process p : procs) {
            assertThat(p.waitFor(120, TimeUnit.SECONDS)).isTrue();
            assertThat(p.exitValue()).isZero();
        }
        assertThat(c.getLong("counter")).isEqualTo(400L);
        Subscription s = watch(c, WatchOptions.defaults().fromSeq(0), "orders:new");
        for (long i = 0; i < 200; i++) {
            assertThat(next(s).payload()).isEqualTo(i);
        }
    }

    // -- onChange / publishChanges ---------------------------------------------------------

    @Test
    void bulkEmitsOneTruncatedRecordAndSmallBulksCarryKeys() throws Exception {
        LyteCache c = open();
        List<ChangeRecord> got = new CopyOnWriteArrayList<>();
        c.onChange(got::add);
        Map<String, String> big = new LinkedHashMap<>();
        for (int i = 0; i < 2000; i++) {
            big.put("k" + i, "v");
        }
        c.setAll(big, null);
        Map<String, String> small = new LinkedHashMap<>();
        small.put("a", "1");
        small.put("b", "2");
        c.setAll(small, null);
        c.delete("a", "b", "a");
        c.delete("missing");
        c.flush();
        waitFor(() -> got.size() == 4);
        Thread.sleep(50);
        assertThat(got).containsExactly(new ChangeRecord(ChangeOp.SET, "default", null, 2000, true),
                new ChangeRecord(ChangeOp.SET, "default", List.of("a", "b"), 2, false),
                new ChangeRecord(ChangeOp.DELETE, "default", List.of("a", "b"), 2, false),
                new ChangeRecord(ChangeOp.FLUSH, "default", null, 2000, true));
    }

    @Test
    void everyValueWriteReportsSetAndNoOpsReportNothing() throws Exception {
        LyteCache c = open();
        List<ChangeRecord> got = new CopyOnWriteArrayList<>();
        c.onChange(got::add, EnumSet.of(ChangeOp.SET));
        c.set("a", 1L);
        c.add("b", 1L, null);
        c.add("b", 2L, null);
        c.replace("a", 2L, null);
        c.replace("missing", 2L, null);
        c.getSet("a", "3");
        c.incr("n");
        c.hset("h", "f", 1);
        c.hsetnx("h", "f", 2);
        c.hincrBy("h", "n", 1);
        c.hdel("h", "nope");
        c.delete("a");
        try (CacheLock ignored = c.lock("job", Duration.ofSeconds(1))) {
            assertThat(ignored).isNotNull();
        }
        Thread.sleep(200);
        List<String> keys = new ArrayList<>();
        for (ChangeRecord r : got) {
            keys.add(r.keys().get(0));
        }
        assertThat(keys).containsExactly("a", "b", "a", "a", "n", "h", "h");
    }

    @Test
    void notCalledOnRollbackThrowingListenerReentrancyAndDepthGuard() throws Exception {
        LyteCache c = open();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + db()); Statement st = conn.createStatement()) {
            st.execute("CREATE TRIGGER boom BEFORE INSERT ON cache WHEN NEW.key = 'boom' BEGIN SELECT RAISE(ABORT, 'boom'); END");
        }
        List<ChangeRecord> got = new CopyOnWriteArrayList<>();
        c.onChange(r -> {
            throw new IllegalStateException("listener bug");
        });
        c.onChange(got::add);
        Map<String, String> m = new LinkedHashMap<>();
        m.put("a", "1");
        m.put("boom", "2");
        assertThatThrownBy(() -> c.setAll(m, null)).isInstanceOf(LyteCacheException.class);
        Thread.sleep(100);
        assertThat(got).isEmpty();
        assertThat(c.exists("a")).isFalse();
        c.set("ok", 1L); // a throwing listener does not fail the write; later listeners still run
        waitFor(() -> got.size() == 1);

        CountDownLatch mirrored = new CountDownLatch(1);
        c.onChange(r -> {
            if (r.keys() != null && r.keys().equals(List.of("src"))) {
                c.set("mirror", c.getLong("src"));
                mirrored.countDown();
            }
        }, EnumSet.of(ChangeOp.SET));
        c.set("src", 42L);
        assertThat(mirrored.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(c.getLong("mirror")).isEqualTo(42L);

        LyteCache d = LyteCache.builder().path(dir.resolve("depth.db")).sweepInterval(null).build();
        opened.add(d);
        AtomicInteger calls = new AtomicInteger();
        d.onChange(r -> {
            calls.incrementAndGet();
            d.incr("depth");
        }, EnumSet.of(ChangeOp.SET));
        d.set("start", 1L);
        Thread.sleep(1000);
        assertThat(calls.get()).isEqualTo(8);
        assertThat(d.getLong("depth")).isEqualTo(8L);
    }

    @Test
    void unsubscribeTakesEffectImmediately() throws Exception {
        LyteCache c = open();
        CountDownLatch gate = new CountDownLatch(1);
        c.onChange(r -> {
            try {
                gate.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        List<ChangeRecord> got = new CopyOnWriteArrayList<>();
        Runnable unsubscribe = c.onChange(got::add);
        c.set("a", 1L);
        unsubscribe.run();
        unsubscribe.run();
        gate.countDown();
        Thread.sleep(100);
        assertThat(got).isEmpty();
    }

    @Test
    void publishChangesPerOperation() throws Exception {
        LyteCache plain = open();
        plain.set("a", 1L);
        plain.delete("a");
        assertThat(eventsTableExists()).isFalse();
        LyteCache c = open(LyteCache.builder().publishChanges(ChangeOp.DELETE, ChangeOp.FLUSH));
        Subscription s = watch(c, WatchOptions.defaults(), "__cache__");
        c.set("a", 1L);
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < 150; i++) {
            m.put("k" + i, "v");
        }
        c.setAll(m, null);
        c.delete("a", "missing");
        c.flush();
        Map<String, Object> del = new LinkedHashMap<>();
        del.put("op", "delete");
        del.put("namespace", "default");
        del.put("keys", List.of("a", "missing"));
        del.put("count", 1);
        del.put("truncated", false);
        assertThat(next(s).payload()).isEqualTo(del);
        Map<String, Object> fl = new LinkedHashMap<>();
        fl.put("op", "flush");
        fl.put("namespace", "default");
        fl.put("keys", null);
        fl.put("count", 150);
        fl.put("truncated", true);
        assertThat(next(s).payload()).isEqualTo(fl);
        assertThat(s.poll(Duration.ofMillis(200))).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void invalidationRecipeRunsAsWritten() {
        LyteCache workerA = open();
        LyteCache workerB = open();
        Map<String, Integer> localMemo = new HashMap<>(Map.of("balance:42", 100, "balance:7", 5));

        // worker A, after committing a DB change
        workerA.delete("balance:42");
        try (Subscription sub = workerB.watch("invalidate")) {
            workerA.notify("invalidate", Map.of("key", "balance:42"));
            // workers B..N
            for (Event e : sub) {
                localMemo.remove(e.payload(Map.class).get("key"));
                break;
            }
        }
        assertThat(localMemo).containsOnlyKeys("balance:7");
    }

    private static void sweep(LyteCache c) throws Exception {
        java.lang.reflect.Method m = LyteCache.class.getDeclaredMethod("sweep");
        m.setAccessible(true);
        m.invoke(c);
    }

    private static Throwable catchThrown(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            return t;
        }
        throw new AssertionError("expected an exception");
    }
}
