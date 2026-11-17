package disk

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	obj = strings.Repeat("ab", 32)
	v1  = FormatVersion(1, 0xabc)
	v2  = FormatVersion(2, 0xdef)
)

// both returns a fresh directory disk and memory disk.
func both(t *testing.T) map[string]Disk {
	t.Helper()
	d, err := NewDir("dir", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return map[string]Disk{"dir": d, "mem": NewMem("mem")}
}

func TestNames(t *testing.T) {
	if v1 != "v00000000000000000001-0000000000000abc" || !ValidVersion(v1) {
		t.Fatalf("FormatVersion = %q", v1)
	}
	if !(v1 < v2) || !(FormatVersion(9, 0) < FormatVersion(10, 0)) {
		t.Fatal("version names must sort by sequence number")
	}
	for _, bad := range []string{"", "v1-abc", "../" + v1, v1 + "x", strings.ToUpper(v1), "v0000000000000000000a-0000000000000abc"} {
		if ValidVersion(bad) {
			t.Errorf("version %q accepted", bad)
		}
	}
	for _, bad := range []string{"", obj[:63], obj + "a", strings.ToUpper(obj), "../" + obj[3:], obj[:62] + "/x"} {
		if ValidObject(bad) {
			t.Errorf("object %q accepted", bad)
		}
	}
}

func TestWriteReadListDelete(t *testing.T) {
	ctx := context.Background()
	for name, d := range both(t) {
		t.Run(name, func(t *testing.T) {
			if vs, err := d.Versions(ctx, obj); err != nil || len(vs) != 0 {
				t.Fatalf("empty disk: %v %v", vs, err)
			}
			if _, err := d.Read(ctx, obj, v1); !errors.Is(err, ErrNotFound) {
				t.Fatalf("read missing: %v", err)
			}
			for _, v := range []string{v2, v1} {
				if err := d.Write(ctx, obj, v, []byte("data-"+v)); err != nil {
					t.Fatal(err)
				}
			}
			// Writing the same file again is a no-op.
			if err := d.Write(ctx, obj, v1, []byte("data-"+v1)); err != nil {
				t.Fatal(err)
			}
			vs, err := d.Versions(ctx, obj)
			if err != nil || len(vs) != 2 || vs[0] != v1 || vs[1] != v2 {
				t.Fatalf("versions = %v %v", vs, err)
			}
			got, err := d.Read(ctx, obj, v1)
			if err != nil || string(got) != "data-"+v1 {
				t.Fatalf("read = %q %v", got, err)
			}
			got[0] = 'X' // the caller's copy must not alias stored data
			if again, _ := d.Read(ctx, obj, v1); again[0] != 'd' {
				t.Fatal("Read returned stored bytes, not a copy")
			}
			for i := 0; i < 2; i++ {
				if err := d.Delete(ctx, obj, v1); err != nil {
					t.Fatalf("delete #%d: %v", i, err)
				}
			}
			if vs, _ := d.Versions(ctx, obj); len(vs) != 1 || vs[0] != v2 {
				t.Fatalf("after delete: %v", vs)
			}
		})
	}
}

func TestInvalidNamesNeverTouchStorage(t *testing.T) {
	ctx := context.Background()
	for name, d := range both(t) {
		t.Run(name, func(t *testing.T) {
			for _, o := range []string{"..", "../../etc", obj[:10], ""} {
				if _, err := d.Versions(ctx, o); !errors.Is(err, ErrInvalid) {
					t.Errorf("Versions(%q): %v", o, err)
				}
				if err := d.Write(ctx, o, v1, []byte("x")); !errors.Is(err, ErrInvalid) {
					t.Errorf("Write(%q): %v", o, err)
				}
			}
			for _, v := range []string{"..", "v1", "../" + v1, ""} {
				if _, err := d.Read(ctx, obj, v); !errors.Is(err, ErrInvalid) {
					t.Errorf("Read(%q): %v", v, err)
				}
				if err := d.Delete(ctx, obj, v); !errors.Is(err, ErrInvalid) {
					t.Errorf("Delete(%q): %v", v, err)
				}
			}
			if err := d.Write(ctx, obj, v1, make([]byte, MaxShardFile+1)); !errors.Is(err, ErrInvalid) {
				t.Errorf("oversized write: %v", err)
			}
			if vs, _ := d.Versions(ctx, obj); len(vs) != 0 {
				t.Errorf("rejected writes left files: %v", vs)
			}
		})
	}
}

func TestDirLayoutIgnoresStrayFilesAndLeavesNoTemp(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	d, err := NewDir("d1", root)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Write(ctx, obj, v1, []byte("x")); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, obj[:2], obj)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != v1+".shard" {
		t.Fatalf("directory holds %v, want only %s.shard", entries, v1)
	}
	for _, stray := range []string{"." + v2 + "-123.tmp", "notes.txt", "v1.shard"} {
		if err := os.WriteFile(filepath.Join(dir, stray), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, v2+".shard"), 0o750); err != nil {
		t.Fatal(err)
	}
	vs, err := d.Versions(ctx, obj)
	if err != nil || len(vs) != 1 || vs[0] != v1 {
		t.Fatalf("versions with stray files = %v %v", vs, err)
	}
	if _, err := NewDir("", root); err == nil {
		t.Fatal("empty id accepted")
	}
}

func TestDirRefusesOversizedFileOnRead(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	d, _ := NewDir("d1", root)
	dir := filepath.Join(root, obj[:2], obj)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, v1+".shard"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(MaxShardFile + 1); err != nil { // sparse file, no real allocation
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Read(ctx, obj, v1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized file read: %v", err)
	}
}

func TestCancelledContextStopsCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for name, d := range both(t) {
		if err := d.Write(ctx, obj, v1, []byte("x")); !errors.Is(err, context.Canceled) {
			t.Errorf("%s write: %v", name, err)
		}
		if _, err := d.Versions(ctx, obj); !errors.Is(err, context.Canceled) {
			t.Errorf("%s versions: %v", name, err)
		}
		if _, err := d.Read(ctx, obj, v1); !errors.Is(err, context.Canceled) {
			t.Errorf("%s read: %v", name, err)
		}
		if err := d.Delete(ctx, obj, v1); !errors.Is(err, context.Canceled) {
			t.Errorf("%s delete: %v", name, err)
		}
	}
}

func TestMemFaults(t *testing.T) {
	ctx := context.Background()
	m := NewMem("m1")
	if err := m.Write(ctx, obj, v1, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	m.SetOffline(true)
	if _, err := m.Read(ctx, obj, v1); !errors.Is(err, ErrOffline) {
		t.Fatalf("offline read: %v", err)
	}
	if _, err := m.Versions(ctx, obj); !errors.Is(err, ErrOffline) {
		t.Fatalf("offline list: %v", err)
	}
	m.SetOffline(false)
	m.SetFailWrites(true)
	if err := m.Write(ctx, obj, v2, []byte("x")); !errors.Is(err, ErrOffline) {
		t.Fatalf("failing write: %v", err)
	}
	if got, err := m.Read(ctx, obj, v1); err != nil || string(got) != "hello" {
		t.Fatalf("read while writes fail: %q %v", got, err)
	}
	m.SetFailWrites(false)
	if err := m.Flip(obj, v1, 0); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Read(ctx, obj, v1); bytes.Equal(got, []byte("hello")) {
		t.Fatal("Flip did not change the stored bytes")
	}
	if err := m.Flip(obj, v1, 5); !errors.Is(err, ErrInvalid) {
		t.Fatalf("flip out of range: %v", err)
	}
	if err := m.Flip(obj, v2, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("flip missing: %v", err)
	}
	if m.Bytes() != 5 {
		t.Fatalf("Bytes = %d, want 5", m.Bytes())
	}
	m.Wipe()
	if vs, _ := m.Versions(ctx, obj); len(vs) != 0 || m.Bytes() != 0 {
		t.Fatal("Wipe left data")
	}
	m.SetDelay(time.Second)
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := m.Versions(short, obj); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delayed call: %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("delay ignored the context deadline")
	}
}

func TestConcurrentWritersOnOneDisk(t *testing.T) {
	ctx := context.Background()
	for name, d := range both(t) {
		var wg sync.WaitGroup
		errs := make(chan error, 32)
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				v := FormatVersion(uint64(i+1), 1)
				if err := d.Write(ctx, obj, v, []byte{byte(i)}); err != nil {
					errs <- err
				}
				if _, err := d.Versions(ctx, obj); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("%s: %v", name, err)
		}
		if vs, _ := d.Versions(ctx, obj); len(vs) != 16 {
			t.Fatalf("%s: %d versions, want 16", name, len(vs))
		}
	}
}
