package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	lytecache "github.com/lytecache/lytecache-go"
)

// Hash commands: field-level access to JSON object values. A hash is just a
// JSON value -- `get` on the same key prints the whole document. (The web
// UI stays delete-only and does not expose hash writes.)

func hashError(err error) error {
	if errors.Is(err, lytecache.ErrSerialization) || errors.Is(err, lytecache.ErrNotNumeric) {
		return databaseError(fmt.Errorf("%w (hash commands need a JSON object value)", err))
	}
	return databaseError(err)
}

func newHGetCmd(flags *globalFlags) *cobra.Command {
	var raw bool
	cmd := &cobra.Command{
		Use:   "hget <key> <field>",
		Short: "Get one field of a JSON object value",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, closeFn, err := openCache(flags, true)
			if err != nil {
				return err
			}
			defer closeFn()
			v, found, err := c.HGetRaw(args[0], args[1])
			if err != nil {
				return hashError(err)
			}
			if !found {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "(nil)")
				return silentExit(exitFalseOrMiss)
			}
			// A JSON string prints as its text, like `get` on a string value;
			// anything else prints as JSON.
			var s string
			if !raw && json.Unmarshal(v, &s) == nil {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), s)
				return nil
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), prettyJSON(v, raw))
			return nil
		},
	}
	cmd.Flags().BoolVar(&raw, "raw", false, "print the field's exact JSON text")
	return cmd
}

func newHGetAllCmd(flags *globalFlags) *cobra.Command {
	var raw bool
	cmd := &cobra.Command{
		Use:   "hgetall <key>",
		Short: "Print every field of a JSON object value, in document order",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, closeFn, err := openCache(flags, true)
			if err != nil {
				return err
			}
			defer closeFn()
			keys, err := c.HKeys(args[0])
			if err != nil {
				return hashError(err)
			}
			vals, err := c.HVals(args[0])
			if err != nil {
				return hashError(err)
			}
			if len(keys) == 0 {
				if ok, _ := c.Exists(args[0]); !ok {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "(nil)")
					return silentExit(exitFalseOrMiss)
				}
			}
			var buf bytes.Buffer
			buf.WriteByte('{')
			for i, k := range keys {
				if i > 0 {
					buf.WriteByte(',')
				}
				kb, _ := json.Marshal(k)
				buf.Write(kb)
				buf.WriteByte(':')
				buf.Write(vals[i])
			}
			buf.WriteByte('}')
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), prettyJSON(buf.Bytes(), raw))
			return nil
		},
	}
	cmd.Flags().BoolVar(&raw, "raw", false, "compact JSON instead of indented")
	return cmd
}

func newHSetCmd(flags *globalFlags) *cobra.Command {
	var ttlSeconds float64
	var typeFlag string
	cmd := &cobra.Command{
		Use:   "hset <key> <field> <value>",
		Short: "Atomically set one field of a JSON object value",
		Long: "Atomically set one field of the JSON object at <key>, printing 1 if the field\n" +
			"was new and 0 if it replaced a value. A missing key is created as a new object\n" +
			"with no expiry (--ttl applies only when the key is created; an existing key's\n" +
			"TTL is never changed). The value is inferred like `set` values.",
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			var value any
			if typeFlag != "" {
				v, err := parseTypedValue(args[2], typeFlag)
				if err != nil {
					return usageErrorf("%v", err)
				}
				value = v
			} else {
				value = inferredValue(args[2])
			}
			c, closeFn, err := openCache(flags, false)
			if err != nil {
				return err
			}
			defer closeFn()
			var opts []lytecache.SetOption
			if cmd.Flags().Changed("ttl") {
				opts = append(opts, lytecache.TTL(time.Duration(ttlSeconds*float64(time.Second))))
			}
			created, err := c.HSet(args[0], args[1], value, opts...)
			if err != nil {
				return hashError(err)
			}
			if created {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), 1)
			} else {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), 0)
			}
			return nil
		},
	}
	cmd.Flags().Float64Var(&ttlSeconds, "ttl", 0, "expiry in seconds, applied only if the key is created")
	cmd.Flags().StringVar(&typeFlag, "type", "", "force the value type: string|int|float|json")
	return cmd
}

func prettyJSON(b []byte, compact bool) string {
	var out bytes.Buffer
	var err error
	if compact {
		err = json.Compact(&out, b)
	} else {
		err = json.Indent(&out, b, "", "  ")
	}
	if err != nil {
		return string(b)
	}
	return out.String()
}
