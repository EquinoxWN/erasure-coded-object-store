package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"time"

	"github.com/EquinoxWN/erasure-coded-object-store/internal/disk"
	"github.com/EquinoxWN/erasure-coded-object-store/internal/store"
)

// benchConfig holds the bench flags.
type benchConfig struct {
	k, m, disks, domains, objects, size int
	seed                                uint64
}

// lossRow is the outcome of reading every object under every combination of f lost disks.
type lossRow struct {
	lost, combos, reads, ok, unavailable, wrong int
}

// bench measures durability and overhead on an in-process cluster of in-memory disks.
func bench(ctx context.Context, args []string, stdout io.Writer) error {
	fs := newFlags("bench")
	var c benchConfig
	fs.IntVar(&c.k, "k", 4, "data shards")
	fs.IntVar(&c.m, "m", 2, "parity shards")
	fs.IntVar(&c.disks, "disks", 6, "disks in the cluster")
	fs.IntVar(&c.domains, "domains", 3, "failure domains (disks are dealt round-robin)")
	fs.IntVar(&c.objects, "objects", 20, "objects to store")
	fs.IntVar(&c.size, "size", 1<<20, "bytes per object")
	fs.Uint64Var(&c.seed, "seed", 1, "random seed for object contents")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%w: unexpected arguments %v", errUsage, fs.Args())
	}
	switch {
	case c.disks < 1 || c.disks > 16:
		return fmt.Errorf("%w: -disks must be 1..16 (the sweep tries every combination)", errUsage)
	case c.domains < 1 || c.domains > c.disks:
		return fmt.Errorf("%w: -domains must be 1..disks", errUsage)
	case c.objects < 1 || c.objects > 1000:
		return fmt.Errorf("%w: -objects must be 1..1000", errUsage)
	case c.size < 0 || c.size > 16<<20:
		return fmt.Errorf("%w: -size must be 0..16777216", errUsage)
	}
	mems := make([]*disk.Mem, c.disks)
	members := make([]store.Member, c.disks)
	for i := range mems {
		mems[i] = disk.NewMem(fmt.Sprintf("d%02d", i+1))
		members[i] = store.Member{Disk: mems[i], Domain: fmt.Sprintf("z%d", i%c.domains+1)}
	}
	cfg := store.DefaultConfig()
	cfg.K, cfg.M, cfg.Retries = c.k, c.m, 0
	st, err := store.New(members, cfg)
	if err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	rng := rand.New(rand.NewPCG(c.seed, c.seed^0x9e3779b97f4a7c15))
	keys := make([]string, c.objects)
	want := make([][]byte, c.objects)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench/object-%04d", i)
		want[i] = make([]byte, c.size)
		for j := range want[i] {
			want[i][j] = byte(rng.Uint32())
		}
	}
	fmt.Fprintf(stdout, "cluster: %d in-memory disks in %d domains, k=%d m=%d, %d objects of %d bytes, seed %d\n",
		c.disks, c.domains, c.k, c.m, c.objects, c.size, c.seed)

	start := time.Now()
	for i, key := range keys {
		if _, err := st.Put(ctx, key, want[i]); err != nil {
			return err
		}
	}
	putTime := time.Since(start)
	var stored int64
	for _, d := range mems {
		stored += d.Bytes()
	}
	logical := int64(c.objects) * int64(c.size)
	fmt.Fprintf(stdout, "put: %d objects in %s (%s, encode + in-memory write)\n", c.objects, ms(putTime), rate(logical, putTime))
	if logical > 0 {
		fmt.Fprintf(stdout, "storage: %d bytes stored for %d object bytes = %.3fx (code (k+m)/k = %.3fx; 3x replication = 3.000x)\n",
			stored, logical, float64(stored)/float64(logical), float64(c.k+c.m)/float64(c.k))
	}

	readTime, err := timedReads(ctx, st, keys, want)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "get, all shards present: %s (%s)\n", ms(readTime), rate(logical, readTime))

	var rows []lossRow
	maxLost := min(c.m+1, c.disks)
	for f := 0; f <= maxLost; f++ {
		row := lossRow{lost: f}
		for _, combo := range combinations(c.disks, f) {
			row.combos++
			setOffline(mems, combo, true)
			for i, key := range keys {
				row.reads++
				got, _, err := st.Get(ctx, key)
				switch {
				case errors.Is(err, store.ErrUnavailable):
					row.unavailable++
				case err != nil:
					setOffline(mems, combo, false)
					return fmt.Errorf("lost %v: get %q: %w", combo, key, err)
				case !bytes.Equal(got, want[i]):
					row.wrong++
				default:
					row.ok++
				}
			}
			setOffline(mems, combo, false)
		}
		rows = append(rows, row)
	}
	fmt.Fprintln(stdout, "\ndisks lost | combinations | reads | ok | unavailable | wrong bytes")
	durable := true
	for _, r := range rows {
		fmt.Fprintf(stdout, "%10d | %12d | %5d | %d | %d | %d\n", r.lost, r.combos, r.reads, r.ok, r.unavailable, r.wrong)
		if r.wrong > 0 || (r.lost <= c.m && r.ok != r.reads) {
			durable = false
		}
	}

	fmt.Fprintln(stdout, "\ndomain lost | reads | ok | unavailable")
	for z := 1; z <= c.domains; z++ {
		var down []int
		for i := range mems {
			if i%c.domains+1 == z {
				down = append(down, i)
			}
		}
		setOffline(mems, down, true)
		ok, unavailable := 0, 0
		for i, key := range keys {
			got, _, err := st.Get(ctx, key)
			switch {
			case errors.Is(err, store.ErrUnavailable):
				unavailable++
			case err != nil:
				setOffline(mems, down, false)
				return err
			case bytes.Equal(got, want[i]):
				ok++
			default:
				durable = false
			}
		}
		setOffline(mems, down, false)
		fmt.Fprintf(stdout, "%11s | %5d | %d | %d\n", fmt.Sprintf("z%d", z), len(keys), ok, unavailable)
	}

	{ // store.New guarantees disks >= k+m, so m disks can always be wiped
		// Lose m disks for good, rebuild them, then read with m different disks down.
		lost := make([]int, c.m)
		for i := range lost {
			lost[i] = i
		}
		for _, i := range lost {
			mems[i].Wipe()
		}
		start = time.Now()
		rewritten := 0
		for _, key := range keys {
			res, err := st.Repair(ctx, key)
			if err != nil {
				return fmt.Errorf("repair %q: %w", key, err)
			}
			rewritten += len(res.Rewritten)
		}
		repairTime := time.Since(start)
		fmt.Fprintf(stdout, "\nrebuild: wiped %d disks, rewrote %d shards in %s (in-memory disks, no network)\n",
			c.m, rewritten, ms(repairTime))
		other := make([]int, 0, c.m)
		for i := c.disks - 1; i >= 0 && len(other) < c.m; i-- {
			other = append(other, i)
		}
		setOffline(mems, other, true)
		_, err = timedReads(ctx, st, keys, want)
		setOffline(mems, other, false)
		if err != nil {
			return fmt.Errorf("read after rebuild with disks %v down: %w", other, err)
		}
		fmt.Fprintf(stdout, "after rebuild, every object reads with disks %v down: yes\n", names(mems, other))
	}

	if !durable {
		return errors.New("durability check failed: an object was unreadable or wrong with at most m disks lost")
	}
	fmt.Fprintf(stdout, "\nverdict: every object read back byte-for-byte with any %d of %d disks lost\n", c.m, c.disks)
	return nil
}

// timedReads reads every key, checks the bytes and returns the elapsed time.
func timedReads(ctx context.Context, st *store.Store, keys []string, want [][]byte) (time.Duration, error) {
	start := time.Now()
	for i, key := range keys {
		got, _, err := st.Get(ctx, key)
		if err != nil {
			return 0, fmt.Errorf("get %q: %w", key, err)
		}
		if !bytes.Equal(got, want[i]) {
			return 0, fmt.Errorf("get %q returned wrong bytes", key)
		}
	}
	return time.Since(start), nil
}

// setOffline switches the listed disks off or on.
func setOffline(mems []*disk.Mem, idx []int, v bool) {
	for _, i := range idx {
		mems[i].SetOffline(v)
	}
}

// names returns the ids of the listed disks.
func names(mems []*disk.Mem, idx []int) []string {
	out := make([]string, len(idx))
	for j, i := range idx {
		out[j] = mems[i].ID()
	}
	return out
}

// combinations returns every f-element subset of 0..n-1 in lexical order.
func combinations(n, f int) [][]int {
	var out [][]int
	cur := make([]int, 0, f)
	var rec func(start int)
	rec = func(start int) {
		if len(cur) == f {
			out = append(out, append([]int(nil), cur...))
			return
		}
		for i := start; i <= n-(f-len(cur)); i++ {
			cur = append(cur, i)
			rec(i + 1)
			cur = cur[:len(cur)-1]
		}
	}
	rec(0)
	return out
}

// ms formats a duration in milliseconds.
func ms(d time.Duration) string { return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000) }

// rate formats bytes per duration as MiB/s.
func rate(n int64, d time.Duration) string {
	if d <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.0f MiB/s", float64(n)/(1<<20)/d.Seconds())
}
