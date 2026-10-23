package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNotifyWatchEvents(t *testing.T) {
	db := tempDBPath(t)
	runCLI(t, "", "--db", db, "set", "seed", "1")

	r := runCLI(t, "", "--db", db, "events")
	if r.code != exitSuccess || !strings.Contains(r.stdout, "no events table") {
		t.Fatalf("events before use: %+v", r)
	}

	for _, args := range [][]string{
		{"notify", "invalidate", `{"key":"balance:42"}`},
		{"notify", "invalidate:user", "42"},
		{"notify", "other", "plain text"},
		{"notify", "empty"},
	} {
		if r := runCLI(t, "", append([]string{"--db", db}, args...)...); r.code != exitSuccess {
			t.Fatalf("%v: %+v", args, r)
		}
	}

	r = runCLI(t, "", "--db", db, "watch", "invalidate*", "--from-seq", "0", "--count", "2")
	if r.code != exitSuccess {
		t.Fatalf("watch: %+v", r)
	}
	lines := strings.Split(strings.TrimSpace(r.stdout), "\n")
	if len(lines) != 2 {
		t.Fatalf("watch lines: %q", r.stdout)
	}
	var first eventLine
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first.Seq != 1 || first.Channel != "invalidate" || string(first.Payload) != `{"key":"balance:42"}` {
		t.Fatalf("first line: %+v", first)
	}

	r = runCLI(t, "", "--db", db, "watch", "*", "--from-seq", "2", "--count", "2")
	if !strings.Contains(r.stdout, `"payload":"plain text"`) || !strings.Contains(r.stdout, `"payload":null`) {
		t.Fatalf("from-seq 2: %s", r.stdout)
	}

	r = runCLI(t, "", "--db", db, "events", "--limit", "2")
	if r.code != exitSuccess || !strings.Contains(r.stdout, "other") || strings.Contains(r.stdout, "invalidate ") ||
		!strings.Contains(r.stdout, "retained: 4 events (seq 1..4)") {
		t.Fatalf("events: %s", r.stdout)
	}
	r = runCLI(t, "", "--db", db, "events", "--json")
	if strings.Count(r.stdout, "\n") != 4 {
		t.Fatalf("events --json: %s", r.stdout)
	}
}

func TestNotifyRejectsReservedAndWatchGap(t *testing.T) {
	db := tempDBPath(t)
	if r := runCLI(t, "", "--db", db, "notify", "__cache__", "x"); r.code != exitUsage {
		t.Fatalf("reserved channel: %+v", r)
	}
	runCLI(t, "", "--db", db, "notify", "c", "1")
	if r := runCLI(t, "", "--db", db, "watch", "c", "--from-seq", "99", "--count", "1"); r.code != exitDatabase ||
		!strings.Contains(r.stderr, "ahead") {
		t.Fatalf("gap: %+v", r)
	}
}

func TestLegacyWatchFormStillRecognized(t *testing.T) {
	// The interval form is recognized (and validated) before any event logic.
	if r := runCLI(t, "", "watch", "-5"); r.code != exitUsage {
		t.Fatalf("watch -5: %+v", r)
	}
	if r := runCLI(t, "", "stats", "--watch=0"); r.code != exitUsage {
		t.Fatalf("stats --watch=0: %+v", r)
	}
}

func TestHashCommands(t *testing.T) {
	db := tempDBPath(t)
	for _, step := range []struct {
		args []string
		out  string
		code int
	}{
		{[]string{"hset", "user:1", "name", "Ada"}, "1\n", exitSuccess},
		{[]string{"hset", "user:1", "name", "Ada2"}, "0\n", exitSuccess},
		{[]string{"hset", "user:1", "logins", "3"}, "1\n", exitSuccess},
		{[]string{"hget", "user:1", "name"}, "Ada2\n", exitSuccess},
		{[]string{"hget", "user:1", "logins"}, "3\n", exitSuccess},
		{[]string{"hget", "user:1", "nope"}, "(nil)\n", exitFalseOrMiss},
		{[]string{"hget", "missing", "x"}, "(nil)\n", exitFalseOrMiss},
		{[]string{"hgetall", "user:1"}, "{\n  \"name\": \"Ada2\",\n  \"logins\": 3\n}\n", exitSuccess},
		{[]string{"hgetall", "user:1", "--raw"}, `{"name":"Ada2","logins":3}` + "\n", exitSuccess},
		{[]string{"hgetall", "missing"}, "(nil)\n", exitFalseOrMiss},
		{[]string{"set", "plain", "1"}, "", exitSuccess},
		{[]string{"hset", "plain", "f", "1"}, "", exitDatabase},
	} {
		r := runCLI(t, "", append([]string{"--db", db}, step.args...)...)
		if r.code != step.code || (step.out != "" && r.stdout != step.out) {
			t.Fatalf("%v: code=%d stdout=%q stderr=%q", step.args, r.code, r.stdout, r.stderr)
		}
	}
	runCLI(t, "", "--db", db, "set", "empty", "{}")
	if r := runCLI(t, "", "--db", db, "hgetall", "empty"); r.code != exitSuccess || r.stdout != "{}\n" {
		t.Fatalf("empty hash: %+v", r)
	}
}
