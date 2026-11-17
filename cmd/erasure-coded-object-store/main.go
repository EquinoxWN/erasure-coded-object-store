// Command erasure-coded-object-store runs disk nodes, stores and reads erasure-coded objects, and
// measures durability and storage overhead in process.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/EquinoxWN/erasure-coded-object-store/internal/disk"
	"github.com/EquinoxWN/erasure-coded-object-store/internal/node"
	"github.com/EquinoxWN/erasure-coded-object-store/internal/placement"
	"github.com/EquinoxWN/erasure-coded-object-store/internal/store"
)

const usage = `usage:
  erasure-coded-object-store serve  -id ID -dir DIR [-addr 127.0.0.1:7201]
  erasure-coded-object-store put    -disks SPEC [-k 4 -m 2 -timeout 5s] KEY FILE
  erasure-coded-object-store get    -disks SPEC [-k 4 -m 2 -timeout 5s] KEY OUTFILE   (OUTFILE "-" = stdout)
  erasure-coded-object-store delete -disks SPEC [-k 4 -m 2 -timeout 5s] KEY
  erasure-coded-object-store repair -disks SPEC [-k 4 -m 2 -timeout 5s] KEY
  erasure-coded-object-store locate -disks SPEC [-k 4 -m 2] KEY
  erasure-coded-object-store bench  [-k 4 -m 2 -disks 6 -domains 3 -objects 20 -size 1048576 -seed 1]

SPEC is a comma-separated list of ID@DOMAIN=TARGET, where TARGET is a node URL
(http://host:port) or a local directory.
`

// main runs the CLI until it finishes or receives an interrupt.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

var errUsage = errors.New("bad usage")

// run dispatches a subcommand and returns the exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "serve":
		err = serve(ctx, args[1:], stdout)
	case "put", "get", "delete", "repair", "locate":
		err = client(ctx, args[0], args[1:], stdout)
	case "bench":
		err = bench(ctx, args[1:], stdout)
	default:
		err = fmt.Errorf("%w: unknown command %q", errUsage, args[0])
	}
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		if errors.Is(err, errUsage) {
			fmt.Fprint(stderr, usage)
			return 2
		}
		return 1
	}
	return 0
}

// newFlags returns a flag set that reports errors instead of exiting.
func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

// serve runs one disk node over HTTP until ctx is cancelled.
func serve(ctx context.Context, args []string, stdout io.Writer) error {
	fs := newFlags("serve")
	id := fs.String("id", "", "disk id")
	dir := fs.String("dir", "", "directory that holds the shard files")
	addr := fs.String("addr", "127.0.0.1:7201", "listen address")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%w: unexpected arguments %v", errUsage, fs.Args())
	}
	if err := placement.ValidateID(*id); err != nil {
		return fmt.Errorf("%w: -id: %w", errUsage, err)
	}
	if *dir == "" {
		return fmt.Errorf("%w: -dir is required", errUsage)
	}
	d, err := disk.NewDir(*id, *dir)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           node.Handler(d),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	fmt.Fprintf(stdout, "disk %s serving %s on http://%s\n", *id, *dir, ln.Addr())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	fmt.Fprintf(stdout, "disk %s stopped\n", *id)
	return nil
}

// parseDisks reads "id@domain=target,..." into store members.
func parseDisks(spec string, timeout time.Duration) ([]store.Member, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, fmt.Errorf("%w: -disks is required", errUsage)
	}
	var out []store.Member
	for _, part := range strings.Split(spec, ",") {
		name, target, ok := strings.Cut(strings.TrimSpace(part), "=")
		id, domain, ok2 := strings.Cut(name, "@")
		if !ok || !ok2 || target == "" {
			return nil, fmt.Errorf("%w: -disks entry %q is not ID@DOMAIN=TARGET", errUsage, part)
		}
		if err := placement.ValidateID(id); err != nil {
			return nil, fmt.Errorf("%w: %w", errUsage, err)
		}
		var d disk.Disk
		var err error
		if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
			d, err = node.NewClient(id, target, timeout)
		} else {
			d, err = disk.NewDir(id, filepath.Clean(target))
		}
		if err != nil {
			return nil, fmt.Errorf("%w: disk %s: %w", errUsage, id, err)
		}
		out = append(out, store.Member{Disk: d, Domain: domain})
	}
	return out, nil
}

// client runs one put, get, delete, repair or locate.
func client(ctx context.Context, cmd string, args []string, stdout io.Writer) error {
	fs := newFlags(cmd)
	spec := fs.String("disks", "", "comma-separated ID@DOMAIN=TARGET list")
	k := fs.Int("k", 4, "data shards")
	m := fs.Int("m", 2, "parity shards")
	timeout := fs.Duration("timeout", 5*time.Second, "time limit per disk call")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	want := map[string]int{"put": 2, "get": 2, "delete": 1, "repair": 1, "locate": 1}[cmd]
	if fs.NArg() != want {
		return fmt.Errorf("%w: %s takes %d argument(s), got %d", errUsage, cmd, want, fs.NArg())
	}
	members, err := parseDisks(*spec, *timeout)
	if err != nil {
		return err
	}
	cfg := store.DefaultConfig()
	cfg.K, cfg.M, cfg.Timeout = *k, *m, *timeout
	st, err := store.New(members, cfg)
	if err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	key := fs.Arg(0)
	placed, err := st.Locate(key)
	if err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	where := describe(placed, *m)
	switch cmd {
	case "locate":
		fmt.Fprintf(stdout, "%q k=%d m=%d: %s\n", key, *k, *m, where)
	case "put":
		data, err := readInput(fs.Arg(1))
		if err != nil {
			return err
		}
		res, err := st.Put(ctx, key, data)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "put %q ok: %d bytes, version %s, %d shards of %d bytes; %s\n",
			key, len(data), res.Version, len(res.Disks), res.ShardSize, where)
		if res.CleanupErr != nil {
			fmt.Fprintf(stdout, "warning: %d disk(s) still hold an older version: %v\n", res.StaleLeft, res.CleanupErr)
		}
	case "get":
		data, info, err := st.Get(ctx, key)
		if err != nil {
			return err
		}
		if err := writeOutput(fs.Arg(1), data, stdout); err != nil {
			return err
		}
		msg := fmt.Sprintf("get %q ok: %d bytes, version %s, SHA-256 verified", key, len(data), info.Version)
		if len(info.Missing) > 0 {
			msg += fmt.Sprintf(", rebuilt missing shards %v", info.Missing)
		}
		if fs.Arg(1) == "-" {
			return nil // keep stdout clean for the object bytes
		}
		fmt.Fprintln(stdout, msg)
	case "delete":
		n, err := st.Delete(ctx, key)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "delete %q ok: %d shard files removed\n", key, n)
	case "repair":
		res, err := st.Repair(ctx, key)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "repair %q ok: version %s, rewrote shards %v\n", key, res.Version, res.Rewritten)
	}
	return nil
}

// describe prints where shards went and how many domains can fail.
func describe(disks []placement.Disk, m int) string {
	parts := make([]string, len(disks))
	for i, d := range disks {
		parts[i] = fmt.Sprintf("%d:%s@%s", i, d.ID, d.Domain)
	}
	return fmt.Sprintf("shards on %s (survives any %d disks or %d whole domains lost)",
		strings.Join(parts, " "), m, placement.DomainsTolerated(disks, m))
}

// readInput reads a file of at most store.MaxObjectSize bytes.
func readInput(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }() // read-only file; a close error changes nothing
	data, err := io.ReadAll(io.LimitReader(f, store.MaxObjectSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > store.MaxObjectSize {
		return nil, fmt.Errorf("%w: %s is larger than %d bytes", errUsage, path, store.MaxObjectSize)
	}
	return data, nil
}

// writeOutput writes data to stdout ("-") or atomically to a file.
func writeOutput(path string, data []byte, stdout io.Writer) error {
	if path == "-" {
		_, err := stdout.Write(data)
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".get-*.tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return errors.Join(err, tmp.Close(), os.Remove(tmp.Name()))
	}
	if err := tmp.Close(); err != nil {
		return errors.Join(err, os.Remove(tmp.Name()))
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return errors.Join(err, os.Remove(tmp.Name()))
	}
	return nil
}
