package lytecache_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	lytecache "github.com/lytecache/lytecache-go"
)

// TestHelperProcess is not itself a real test: it's re-exec'd as a child
// OS process by the multi-process tests below (the standard os/exec
// test-helper pattern -- see e.g. the Go standard library's os/exec
// tests). Run normally, as part of the suite, it's a no-op.
func TestHelperProcess(_ *testing.T) {
	if os.Getenv("LYTECACHE_WANT_HELPER_PROCESS") != "1" {
		return
	}

	path := os.Getenv("LYTECACHE_HELPER_PATH")
	c, err := lytecache.New(lytecache.WithPath(path), lytecache.WithSweepInterval(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper: New:", err)
		os.Exit(1)
	}

	switch os.Getenv("LYTECACHE_HELPER_MODE") {
	case "incr":
		key := os.Getenv("LYTECACHE_HELPER_KEY")
		iterations, _ := strconv.Atoi(os.Getenv("LYTECACHE_HELPER_ITERATIONS"))
		for i := 0; i < iterations; i++ {
			if _, err := c.Incr(key, 1); err != nil {
				fmt.Fprintln(os.Stderr, "helper: Incr:", err)
				os.Exit(1)
			}
		}
	case "lock":
		logPath := os.Getenv("LYTECACHE_HELPER_LOG")
		lockName := os.Getenv("LYTECACHE_HELPER_KEY")
		lock, err := c.Lock(lockName, 10*time.Second)
		if err != nil {
			fmt.Fprintln(os.Stderr, "helper: Lock:", err)
			os.Exit(1)
		}
		appendLogLine(logPath, "START")
		time.Sleep(50 * time.Millisecond) // hold the lock long enough to make an overlap detectable
		appendLogLine(logPath, "END")
		if err := lock.Release(); err != nil {
			fmt.Fprintln(os.Stderr, "helper: Release:", err)
			os.Exit(1)
		}
	case "hset", "hincrby":
		key := os.Getenv("LYTECACHE_HELPER_KEY")
		field := os.Getenv("LYTECACHE_HELPER_FIELD")
		iterations, _ := strconv.Atoi(os.Getenv("LYTECACHE_HELPER_ITERATIONS"))
		for i := 0; i < iterations; i++ {
			var err error
			if os.Getenv("LYTECACHE_HELPER_MODE") == "hset" {
				_, err = c.HSet(key, field, i)
			} else {
				_, err = c.HIncrBy(key, field, 1)
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, "helper:", err)
				os.Exit(1)
			}
		}
	case "publish":
		n, _ := strconv.Atoi(os.Getenv("LYTECACHE_HELPER_ITERATIONS"))
		for i := 0; i < n; i++ {
			for _, ch := range []string{"orders:new", "unrelated"} {
				if _, err := c.Notify(ch, i); err != nil {
					fmt.Fprintln(os.Stderr, "helper: Notify:", err)
					os.Exit(1)
				}
			}
		}
	case "subscribe":
		n, _ := strconv.Atoi(os.Getenv("LYTECACHE_HELPER_ITERATIONS"))
		sub, err := c.Watch([]string{"orders*"})
		if err != nil {
			fmt.Fprintln(os.Stderr, "helper: Watch:", err)
			os.Exit(1)
		}
		appendLogLine(os.Getenv("LYTECACHE_HELPER_READY"), "ready")
		for i := 0; i < n; i++ {
			e, err := sub.Next(context.Background())
			if err != nil {
				fmt.Fprintln(os.Stderr, "helper: Next:", err)
				os.Exit(1)
			}
			var p int
			_ = e.Decode(&p)
			appendLogLine(os.Getenv("LYTECACHE_HELPER_LOG"), fmt.Sprintf("%d %s %d", e.Seq, e.Channel, p))
		}
		_ = sub.Close()
	case "cursorsub":
		// Handles events forever, saving its cursor after each; killed by the test.
		from, found, _ := c.LoadCursor("worker")
		var opts []lytecache.WatchOption
		if found {
			opts = append(opts, lytecache.FromSeq(from))
		}
		sub, err := c.Watch([]string{"jobs"}, opts...)
		if err != nil {
			fmt.Fprintln(os.Stderr, "helper: Watch:", err)
			os.Exit(1)
		}
		appendLogLine(os.Getenv("LYTECACHE_HELPER_READY"), "ready")
		for e := range sub.Events() {
			if err := c.SaveCursor("worker", e.Seq); err != nil {
				fmt.Fprintln(os.Stderr, "helper: SaveCursor:", err)
				os.Exit(1)
			}
		}
	default:
		fmt.Fprintln(os.Stderr, "helper: unknown LYTECACHE_HELPER_MODE")
		os.Exit(1)
	}

	if err := c.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "helper: Close:", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// appendLogLine appends one line to path. A single Write of a short line is
// atomic under POSIX O_APPEND, so concurrent processes' lines never
// interleave mid-line -- what we actually rely on for the mutual-exclusion
// check in TestMultiProcessLockMutualExclusion.
func appendLogLine(path, line string) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper: opening log:", err)
		os.Exit(1)
	}
	_, writeErr := f.WriteString(line + "\n")
	closeErr := f.Close()
	if writeErr != nil {
		fmt.Fprintln(os.Stderr, "helper: writing log:", writeErr)
		os.Exit(1)
	}
	if closeErr != nil {
		fmt.Fprintln(os.Stderr, "helper: closing log:", closeErr)
		os.Exit(1)
	}
}

func runHelperProcess(t *testing.T, env ...string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(append([]string{}, os.Environ()...), "LYTECACHE_WANT_HELPER_PROCESS=1")
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("helper process failed: %v\n%s", err, out)
	}
}

func tempLogPath(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "lock-log-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	return f.Name()
}

// TestMultiProcessIncrAtomicity re-execs the test binary as several real OS
// processes, all hammering Incr on one key in one shared file. Incr's
// single-UPSERT design must keep the final total exact across processes,
// not just goroutines.
func TestMultiProcessIncrAtomicity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}
	path := tempDBPath(t)

	const processes = 4
	const iterationsEach = 50

	var wg sync.WaitGroup
	for i := 0; i < processes; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runHelperProcess(t,
				"LYTECACHE_HELPER_MODE=incr",
				"LYTECACHE_HELPER_PATH="+path,
				"LYTECACHE_HELPER_KEY=counter",
				"LYTECACHE_HELPER_ITERATIONS="+strconv.Itoa(iterationsEach),
			)
		}()
	}
	wg.Wait()

	c, err := lytecache.New(lytecache.WithPath(path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	got, _, err := c.GetInt64("counter")
	if err != nil {
		t.Fatal(err)
	}
	want := int64(processes * iterationsEach)
	if got != want {
		t.Fatalf("expected exact cross-process counter total %d, got %d", want, got)
	}
}

// TestMultiProcessLockMutualExclusion re-execs the test binary as several
// real OS processes, all racing to acquire the same named lock. Each
// holder logs a START/END pair to a shared file; overlapping START/END
// pairs across processes would mean two processes held the lock at once.
func TestMultiProcessLockMutualExclusion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}
	path := tempDBPath(t)
	logPath := tempLogPath(t)

	const processes = 4
	var wg sync.WaitGroup
	for i := 0; i < processes; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runHelperProcess(t,
				"LYTECACHE_HELPER_MODE=lock",
				"LYTECACHE_HELPER_PATH="+path,
				"LYTECACHE_HELPER_KEY=resource",
				"LYTECACHE_HELPER_LOG="+logPath,
			)
		}()
	}
	wg.Wait()

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != processes*2 {
		t.Fatalf("expected %d log lines, got %d: %v", processes*2, len(lines), lines)
	}

	held := false
	for _, line := range lines {
		switch line {
		case "START":
			if held {
				t.Fatal("observed a START while the lock was already held -- mutual exclusion violated")
			}
			held = true
		case "END":
			if !held {
				t.Fatal("observed an END without a matching START")
			}
			held = false
		}
	}
}
