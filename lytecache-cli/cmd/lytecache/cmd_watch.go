package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"
)

// runStatsWatch redraws `stats` every interval until Ctrl-C (`stats
// --watch`, and the deprecated `watch [interval]` form).
func runStatsWatch(cmd *cobra.Command, flags *globalFlags, interval time.Duration) error {
	// In one-shot mode, every tick opens and closes its own Cache, matching
	// the open/act/close discipline described in the README, so this never
	// holds a write connection longer than a single Stats call, safely
	// alongside a live application using the file. In REPL mode, openCache
	// instead reuses the session's single already-open Cache each tick.
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
	defer stop()

	w := cmd.OutOrStdout()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		c, closeFn, err := openCache(flags, true)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprint(w, "\033[H\033[2J") // clear screen
		_, _ = fmt.Fprintf(w, "lytecache stats --watch -- every %s -- Ctrl-C to quit\n\n", interval)
		statsErr := runStats(w, c, false)
		closeFn()
		if statsErr != nil {
			return statsErr
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
