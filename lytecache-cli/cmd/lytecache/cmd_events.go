package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	lytecache "github.com/lytecache/lytecache-go"
)

// Local event notification from the shell. SINGLE HOST ONLY, polled, and
// durable -- see the library's docs/notifications.md. These commands are
// the debugging tools that make the feature usable: publish an event, tail
// events live, list what is retained.

func newNotifyCmd(flags *globalFlags) *cobra.Command {
	var typeFlag string
	cmd := &cobra.Command{
		Use:   "notify <channel> [payload]",
		Short: "Publish a local event (single host, polled, durable)",
		Long: "Publish an event to <channel> for watchers on this host, printing its seq.\n\n" +
			"The payload is inferred like `set` values: an integer or float literal,\n" +
			"valid JSON, or otherwise a plain string. Omit it for a null payload.\n" +
			"Events are delivered by polling to subscribers on THIS machine only.",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			var payload any
			if len(args) == 2 {
				if typeFlag != "" {
					v, err := parseTypedValue(args[1], typeFlag)
					if err != nil {
						return usageErrorf("%v", err)
					}
					payload = v
				} else {
					payload = inferredValue(args[1])
				}
			}
			c, closeFn, err := openCache(flags, false)
			if err != nil {
				return err
			}
			defer closeFn()
			seq, err := c.Notify(args[0], payload)
			if err != nil {
				if errors.Is(err, lytecache.ErrInvalidArgument) {
					return usageErrorf("%v", err)
				}
				return databaseError(err)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), seq)
			return nil
		},
	}
	cmd.Flags().StringVar(&typeFlag, "type", "", "force the payload type: string|int|float|json")
	return cmd
}

// eventLine is one `watch`/`events --json` output line.
type eventLine struct {
	Seq       int64           `json:"seq"`
	Channel   string          `json:"channel"`
	Namespace string          `json:"namespace"`
	CreatedAt string          `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
	// PayloadType is set only when the payload is not shown as plain JSON:
	// "bytes" (payload is base64) or a non-portable code.
	PayloadType string `json:"payload_type,omitempty"`
	SizeBytes   int    `json:"size_bytes"`
}

func toEventLine(e lytecache.Event) eventLine {
	l := eventLine{
		Seq: e.Seq, Channel: e.Channel, Namespace: e.Namespace,
		CreatedAt: e.CreatedAt.UTC().Format(time.RFC3339Nano),
		SizeBytes: len(e.Payload.Data),
		Payload:   json.RawMessage("null"),
	}
	if e.Null {
		return l
	}
	switch code := e.Payload.ValueType(); code {
	case typeJSON:
		l.Payload = json.RawMessage(e.Payload.Data)
	case typeBytes:
		l.Payload, _ = json.Marshal(base64.StdEncoding.EncodeToString(e.Payload.Data))
		l.PayloadType = "bytes"
	case typeString:
		l.Payload, _ = json.Marshal(string(e.Payload.Data))
	case typeInt, typeFloat:
		var v any
		if err := e.Decode(&v); err == nil {
			l.Payload, _ = json.Marshal(v)
		}
	default:
		l.PayloadType = typeCodeName(code) // non-portable: shown as null
	}
	return l
}

func newEventsWatchCmd(flags *globalFlags) *cobra.Command {
	var fromSeq int64
	var poll time.Duration
	var count int
	cmd := &cobra.Command{
		Use:   "watch <channel-pattern>... [--from-seq N]",
		Short: "Tail local events live, one JSON line per event",
		Long: "Tail events published on THIS host to matching channels, one JSON object per\n" +
			"line (suitable for piping into jq). A pattern is an exact channel name, or\n" +
			"ends in * to match a prefix; '*' matches everything, including __cache__.\n\n" +
			"--from-seq N resumes after seq N (0 replays everything retained); without it,\n" +
			"only new events are shown. Exits 3 with a gap error if events after N were\n" +
			"already deleted by retention. Ctrl-C to stop.\n\n" +
			"(`watch` with no arguments, or a single number, still redraws `stats` as\n" +
			"before; that form is deprecated in favour of `stats --watch`.)",
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if legacy, interval, err := legacyWatchArgs(cmd, args); legacy {
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(),
					"note: `lytecache watch [interval]` is deprecated; use `lytecache stats --watch[=interval]`. "+
						"`lytecache watch <channel-pattern>` now tails events.")
				return runStatsWatch(cmd, flags, interval)
			}
			c, closeFn, err := openCache(flags, true)
			if err != nil {
				return err
			}
			defer closeFn()
			opts := []lytecache.WatchOption{lytecache.PollInterval(poll)}
			if cmd.Flags().Changed("from-seq") {
				if fromSeq < 0 {
					return usageErrorf("--from-seq must be >= 0")
				}
				opts = append(opts, lytecache.FromSeq(fromSeq))
			}
			sub, err := c.Watch(args, opts...)
			if err != nil {
				if errors.Is(err, lytecache.ErrInvalidArgument) {
					return usageErrorf("%v", err)
				}
				return databaseError(err)
			}
			defer func() { _ = sub.Close() }()

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()
			enc := json.NewEncoder(cmd.OutOrStdout())
			for seen := 0; count == 0 || seen < count; seen++ {
				e, err := sub.Next(ctx)
				switch {
				case errors.Is(err, context.Canceled):
					return nil
				case err != nil:
					return databaseError(err)
				}
				if err := enc.Encode(toEventLine(e)); err != nil {
					return err // e.g. a closed pipe
				}
			}
			return nil
		},
	}
	cmd.Flags().Int64Var(&fromSeq, "from-seq", 0, "resume after this seq (0 = everything retained)")
	cmd.Flags().DurationVar(&poll, "poll", 50*time.Millisecond, "poll interval while events are flowing")
	cmd.Flags().IntVar(&count, "count", 0, "exit after this many events (0 = run until Ctrl-C)")
	return cmd
}

// legacyWatchArgs recognizes the pre-events `watch [interval]` form: no
// arguments, or one positive number, and no event flags.
func legacyWatchArgs(cmd *cobra.Command, args []string) (bool, time.Duration, error) {
	if cmd.Flags().Changed("from-seq") || cmd.Flags().Changed("count") || len(args) > 1 {
		return false, 0, nil
	}
	if len(args) == 0 {
		return true, 2 * time.Second, nil
	}
	seconds, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		return false, 0, nil // a channel pattern
	}
	if seconds <= 0 {
		return true, 0, usageErrorf("interval must be positive, got %s", args[0])
	}
	return true, time.Duration(seconds * float64(time.Second)), nil
}

func newEventsCmd(flags *globalFlags) *cobra.Command {
	var limit int
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "events",
		Short: "List recent retained events (seq, channel, age, size)",
		Long: "List the most recent events still retained in the file, across every\n" +
			"namespace, oldest first, followed by retention stats. Read-only: it never\n" +
			"creates the events table.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if limit <= 0 {
				return usageErrorf("--limit must be positive")
			}
			c, closeFn, err := openCache(flags, true)
			if err != nil {
				return err
			}
			defer closeFn()
			return runEvents(cmd.OutOrStdout(), c, limit, asJSON)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 50, "how many recent events to show")
	cmd.Flags().BoolVar(&asJSON, "json", false, "one JSON object per line instead of a table")
	return cmd
}

func runEvents(w io.Writer, c *lytecache.Cache, limit int, asJSON bool) error {
	info, err := c.EventStats()
	if err != nil {
		return databaseError(err)
	}
	if !info.Enabled {
		if !asJSON {
			_, _ = fmt.Fprintln(w, "(no events table: notify/watch has never been used on this file)")
		}
		return nil
	}
	events, err := c.RecentEvents(limit, -1)
	if err != nil {
		return databaseError(err)
	}
	if asJSON {
		enc := json.NewEncoder(w)
		for _, e := range events {
			if err := enc.Encode(toEventLine(e)); err != nil {
				return err
			}
		}
		return nil
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "SEQ\tCHANNEL\tNAMESPACE\tAGE\tSIZE")
	now := time.Now()
	for _, e := range events {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%d B\n", e.Seq, e.Channel, e.Namespace,
			humanAge(now.Sub(e.CreatedAt)), len(e.Payload.Data))
	}
	_ = tw.Flush()
	_, _ = fmt.Fprintf(w, "\nretained: %d events (seq %d..%d), %d payload bytes", info.Count, info.OldestSeq, info.NewestSeq, info.PayloadBytes)
	if info.Count > 0 {
		_, _ = fmt.Fprintf(w, ", oldest %s old", humanAge(now.Sub(info.OldestAt)))
	}
	_, _ = fmt.Fprintln(w)
	return nil
}

func humanAge(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}
