package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EquinoxWN/erasure-coded-object-store/internal/disk"
	"github.com/EquinoxWN/erasure-coded-object-store/internal/node"
)

// runCLI runs the CLI and returns the exit code, stdout and stderr.
func runCLI(ctx context.Context, args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	code := run(ctx, args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// dirSpec returns a -disks value for six local directories over three domains.
func dirSpec(t *testing.T) (string, []string) {
	t.Helper()
	root := t.TempDir()
	var parts, dirs []string
	for i := 1; i <= 6; i++ {
		dir := filepath.Join(root, fmt.Sprintf("d%d", i))
		dirs = append(dirs, dir)
		parts = append(parts, fmt.Sprintf("d%d@rack%d=%s", i, (i-1)%3+1, dir))
	}
	return strings.Join(parts, ","), dirs
}

func TestPutGetDeleteWithLocalDirectories(t *testing.T) {
	ctx := context.Background()
	spec, dirs := dirSpec(t)
	in := filepath.Join(t.TempDir(), "in.bin")
	data := bytes.Repeat([]byte("0123456789"), 5000)
	if err := os.WriteFile(in, data, 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := runCLI(ctx, "put", "-disks", spec, "docs/a.bin", in)
	if code != 0 || !strings.Contains(out, "put \"docs/a.bin\" ok: 50000 bytes") || !strings.Contains(out, "1 whole domains lost") {
		t.Fatalf("put: %d %q %q", code, out, errOut)
	}
	// Remove two whole disk directories: the object must still read back.
	for _, d := range dirs[:2] {
		if err := os.RemoveAll(d); err != nil {
			t.Fatal(err)
		}
	}
	outFile := filepath.Join(t.TempDir(), "out.bin")
	code, out, errOut = runCLI(ctx, "get", "-disks", spec, "docs/a.bin", outFile)
	if code != 0 || !strings.Contains(out, "SHA-256 verified") || !strings.Contains(out, "rebuilt missing shards") {
		t.Fatalf("get: %d %q %q", code, out, errOut)
	}
	if got, err := os.ReadFile(outFile); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("output differs: %v", err)
	}
	code, out, _ = runCLI(ctx, "repair", "-disks", spec, "docs/a.bin")
	if code != 0 || !strings.Contains(out, "rewrote shards [") || strings.Contains(out, "rewrote shards []") {
		t.Fatalf("repair: %d %q", code, out)
	}
	code, out, _ = runCLI(ctx, "get", "-disks", spec, "docs/a.bin", "-")
	if code != 0 || out != string(data) {
		t.Fatalf("get to stdout: %d, %d bytes", code, len(out))
	}
	code, out, _ = runCLI(ctx, "locate", "-disks", spec, "docs/a.bin")
	if code != 0 || strings.Count(out, "@rack") != 6 {
		t.Fatalf("locate: %d %q", code, out)
	}
	code, out, _ = runCLI(ctx, "delete", "-disks", spec, "docs/a.bin")
	if code != 0 || !strings.Contains(out, "6 shard files removed") {
		t.Fatalf("delete: %d %q", code, out)
	}
	code, _, errOut = runCLI(ctx, "get", "-disks", spec, "docs/a.bin", outFile)
	if code != 1 || !strings.Contains(errOut, "not found") {
		t.Fatalf("get after delete: %d %q", code, errOut)
	}
}

func TestPutGetOverHTTPNodes(t *testing.T) {
	ctx := context.Background()
	var parts []string
	var servers []*httptest.Server
	for i := 1; i <= 6; i++ {
		d, err := disk.NewDir(fmt.Sprintf("n%d", i), t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(node.Handler(d))
		t.Cleanup(srv.Close)
		servers = append(servers, srv)
		parts = append(parts, fmt.Sprintf("n%d@z%d=%s", i, (i-1)%3+1, srv.URL))
	}
	spec := strings.Join(parts, ",")
	in := filepath.Join(t.TempDir(), "in.txt")
	if err := os.WriteFile(in, []byte("hello over http"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, out, errOut := runCLI(ctx, "put", "-disks", spec, "-timeout", "2s", "k", in); code != 0 {
		t.Fatalf("put: %q %q", out, errOut)
	}
	servers[0].Close()
	servers[3].Close()
	code, out, errOut := runCLI(ctx, "get", "-disks", spec, "-timeout", "2s", "k", "-")
	if code != 0 || out != "hello over http" {
		t.Fatalf("get with two nodes down: %d %q %q", code, out, errOut)
	}
	servers[1].Close()
	if code, _, _ := runCLI(ctx, "get", "-disks", spec, "-timeout", "2s", "k", "-"); code != 1 {
		t.Fatalf("get with three nodes down exited %d, want 1", code)
	}
}

func TestBadArgumentsExitTwo(t *testing.T) {
	ctx := context.Background()
	spec, _ := dirSpec(t)
	cases := [][]string{
		{},
		{"nope"},
		{"put", "-disks", spec, "k"},
		{"get", "-disks", spec, "k"},
		{"delete", "-disks", spec},
		{"put", "k", "f"},
		{"locate", "-disks", "a=b", "k"},
		{"locate", "-disks", "a@z=", "k"},
		{"locate", "-disks", "../x@z=dir", "k"},
		{"locate", "-disks", "a@z=ftp://x", "k"},
		{"locate", "-disks", spec, "-k", "5", "k"},
		{"locate", "-disks", spec, "-m", "0", "k"},
		{"locate", "-disks", spec, ""},
		{"locate", "-disks", spec, "-bogus", "k"},
		{"serve"},
		{"serve", "-id", "a"},
		{"serve", "-id", "a", "-dir", "x", "extra"},
		{"bench", "-disks", "17"},
		{"bench", "-k", "6", "-m", "2", "-disks", "6"},
		{"bench", "-domains", "0"},
		{"bench", "-objects", "0"},
		{"bench", "-size", "-1"},
		{"bench", "extra"},
	}
	for _, args := range cases {
		code, _, errOut := runCLI(ctx, args...)
		if code != 2 || !strings.Contains(errOut, "usage:") {
			t.Errorf("%v: exit %d, stderr %q", args, code, errOut)
		}
	}
	in := filepath.Join(t.TempDir(), "missing.bin")
	if code, _, _ := runCLI(ctx, "put", "-disks", spec, "k", in); code != 1 {
		t.Errorf("missing input file: exit %d, want 1", code)
	}
}

func TestBenchSmallRun(t *testing.T) {
	code, out, errOut := runCLI(context.Background(), "bench", "-objects", "3", "-size", "4096")
	if code != 0 {
		t.Fatalf("bench: %d %q", code, errOut)
	}
	for _, want := range []string{
		"         2 |           15 |    45 | 45 | 0 | 0",
		"         3 |           20 |    60 | 0 | 60 | 0",
		"rewrote 6 shards",
		"verdict: every object read back byte-for-byte with any 2 of 6 disks lost",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("bench output lacks %q:\n%s", want, out)
		}
	}
}

func TestServeStartsAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var out syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"serve", "-id", "s1", "-dir", t.TempDir(), "-addr", "127.0.0.1:0"}, &out, &out)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(out.String(), "serving") {
		if time.Now().After(deadline) {
			t.Fatalf("serve did not start: %q", out.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 || !strings.Contains(out.String(), "disk s1 stopped") {
			t.Fatalf("serve exit %d: %q", code, out.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not stop")
	}
}

func TestCombinations(t *testing.T) {
	for _, c := range []struct{ n, f, want int }{{6, 0, 1}, {6, 2, 15}, {6, 3, 20}, {5, 5, 1}, {9, 3, 84}} {
		got := combinations(c.n, c.f)
		if len(got) != c.want {
			t.Errorf("C(%d,%d) = %d, want %d", c.n, c.f, len(got), c.want)
		}
		seen := map[string]bool{}
		for _, s := range got {
			seen[fmt.Sprint(s)] = true
		}
		if len(seen) != len(got) {
			t.Errorf("C(%d,%d) has duplicates", c.n, c.f)
		}
	}
}

// syncBuffer is a bytes.Buffer safe for one writer and one reader.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

// Write appends p.
func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

// String returns the contents so far.
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }
