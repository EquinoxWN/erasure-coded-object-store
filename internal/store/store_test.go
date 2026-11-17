package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EquinoxWN/erasure-coded-object-store/internal/disk"
	"github.com/EquinoxWN/erasure-coded-object-store/internal/placement"
)

// cluster builds a store over in-memory disks dealt round-robin over domains.
func cluster(t *testing.T, disks, domains, k, m int) (*Store, map[string]*disk.Mem) {
	t.Helper()
	mems := map[string]*disk.Mem{}
	members := make([]Member, disks)
	for i := range members {
		d := disk.NewMem(fmt.Sprintf("d%02d", i+1))
		mems[d.ID()] = d
		members[i] = Member{Disk: d, Domain: fmt.Sprintf("z%d", i%domains+1)}
	}
	cfg := DefaultConfig()
	cfg.K, cfg.M, cfg.Timeout, cfg.Retries, cfg.Backoff = k, m, time.Second, 1, time.Millisecond
	s, err := New(members, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s, mems
}

// payload returns n deterministic pseudo-random bytes.
func payload(n int, seed uint64) []byte {
	rng := rand.New(rand.NewPCG(seed, 7))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

// shardDisks returns the in-memory disks holding key's shards, shard i first.
func shardDisks(t *testing.T, s *Store, mems map[string]*disk.Mem, key string) []*disk.Mem {
	t.Helper()
	placed, err := s.Locate(key)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]*disk.Mem, len(placed))
	for i, p := range placed {
		out[i] = mems[p.ID]
	}
	return out
}

// subsets calls fn with every f-element subset of 0..n-1.
func subsets(n, f int, fn func([]int)) {
	cur := make([]int, 0, f)
	var rec func(int)
	rec = func(start int) {
		if len(cur) == f {
			fn(cur)
			return
		}
		for i := start; i < n; i++ {
			cur = append(cur, i)
			rec(i + 1)
			cur = cur[:len(cur)-1]
		}
	}
	rec(0)
}

func TestNewValidatesConfigAndCluster(t *testing.T) {
	mems := []Member{{disk.NewMem("a"), "z"}, {disk.NewMem("b"), "z"}, {disk.NewMem("c"), "z"}}
	good := DefaultConfig()
	good.K, good.M = 2, 1
	if _, err := New(mems, good); err != nil {
		t.Fatalf("valid store rejected: %v", err)
	}
	bad := map[string]Config{
		"k=0":       {K: 0, M: 1, Timeout: time.Second},
		"m=0":       {K: 2, M: 0, Timeout: time.Second},
		"timeout 0": {K: 2, M: 1},
		"retries 6": {K: 2, M: 1, Timeout: time.Second, Retries: 6},
		"neg back":  {K: 2, M: 1, Timeout: time.Second, Backoff: -1},
		"k+m > n":   {K: 3, M: 1, Timeout: time.Second},
	}
	for name, cfg := range bad {
		if _, err := New(mems, cfg); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err=%v, want ErrInvalid", name, err)
		}
	}
	if _, err := New([]Member{{nil, "z"}}, good); !errors.Is(err, ErrInvalid) {
		t.Errorf("nil disk: %v", err)
	}
	dup := []Member{{disk.NewMem("a"), "z"}, {disk.NewMem("a"), "z"}, {disk.NewMem("c"), "z"}}
	if _, err := New(dup, good); !errors.Is(err, ErrInvalid) {
		t.Errorf("duplicate disk id: %v", err)
	}
}

func TestPutGetRoundTripSizes(t *testing.T) {
	ctx := context.Background()
	s, _ := cluster(t, 6, 3, 4, 2)
	for _, size := range []int{0, 1, 3, 4, 5, 4096, 100_003} {
		key := fmt.Sprintf("size/%d", size)
		data := payload(size, uint64(size))
		res, err := s.Put(ctx, key, data)
		if err != nil {
			t.Fatalf("put %d: %v", size, err)
		}
		if len(res.Disks) != 6 || res.ShardSize != (size+3)/4 || res.StaleLeft != 0 {
			t.Fatalf("put %d: result %+v", size, res)
		}
		got, info, err := s.Get(ctx, key)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("get %d: %v (len %d)", size, err, len(got))
		}
		if info.Version != res.Version || len(info.Missing) != 0 {
			t.Fatalf("get %d: info %+v", size, info)
		}
	}
}

// Storage overhead is (k+m)/k plus a small per-shard header.
func TestStorageOverheadIsKPlusMOverK(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	const size = 1 << 20
	if _, err := s.Put(ctx, "big", payload(size, 1)); err != nil {
		t.Fatal(err)
	}
	var stored int64
	for _, m := range mems {
		stored += m.Bytes()
	}
	ratio := float64(stored) / size
	if ratio < 1.5 || ratio > 1.502 {
		t.Fatalf("stored %d bytes for %d: ratio %.4f, want 1.5 plus headers", stored, size, ratio)
	}
}

// The M1 claim: any k of n shards rebuild the object, so every set of up to m lost disks is survived.
func TestGetSurvivesEveryCombinationOfMDiskLosses(t *testing.T) {
	ctx := context.Background()
	for _, p := range [][4]int{{6, 3, 4, 2}, {9, 3, 6, 3}, {8, 4, 5, 3}, {5, 5, 3, 2}} {
		disks, domains, k, m := p[0], p[1], p[2], p[3]
		s, mems := cluster(t, disks, domains, k, m)
		ids := make([]string, 0, disks)
		for i := 1; i <= disks; i++ {
			ids = append(ids, fmt.Sprintf("d%02d", i))
		}
		keys := []string{"a", "b/c", "unicode/é", "empty"}
		want := map[string][]byte{}
		for i, key := range keys {
			want[key] = payload(1000*i+17, uint64(i))
			if key == "empty" {
				want[key] = nil
			}
			if _, err := s.Put(ctx, key, want[key]); err != nil {
				t.Fatal(err)
			}
		}
		for f := 1; f <= m; f++ {
			subsets(disks, f, func(lost []int) {
				for _, i := range lost {
					mems[ids[i]].SetOffline(true)
				}
				for _, key := range keys {
					got, _, err := s.Get(ctx, key)
					if err != nil || !bytes.Equal(got, want[key]) {
						t.Fatalf("k=%d m=%d lost %v: get %q: %v", k, m, lost, key, err)
					}
				}
				for _, i := range lost {
					mems[ids[i]].SetOffline(false)
				}
			})
		}
	}
}

func TestGetFailsWithMPlusOneShardsLost(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	if _, err := s.Put(ctx, "k", payload(500, 1)); err != nil {
		t.Fatal(err)
	}
	sd := shardDisks(t, s, mems, "k")
	subsets(6, 3, func(lost []int) {
		for _, i := range lost {
			sd[i].SetOffline(true)
		}
		if _, _, err := s.Get(ctx, "k"); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("lost shards %v: err=%v, want ErrUnavailable", lost, err)
		}
		for _, i := range lost {
			sd[i].SetOffline(false)
		}
	})
}

func TestGetReportsRebuiltShards(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	if _, err := s.Put(ctx, "k", payload(4000, 2)); err != nil {
		t.Fatal(err)
	}
	sd := shardDisks(t, s, mems, "k")
	sd[0].SetOffline(true)
	sd[3].Wipe()
	got, info, err := s.Get(ctx, "k")
	if err != nil || !bytes.Equal(got, payload(4000, 2)) {
		t.Fatal(err)
	}
	if fmt.Sprint(info.Missing) != "[0 3]" {
		t.Fatalf("missing = %v, want [0 3]", info.Missing)
	}
}

// One bad disk never holds two shards: a whole failure domain can go when it holds at most m shards.
func TestGetSurvivesLosingAWholeDomain(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("obj-%d", i)
		if _, err := s.Put(ctx, keys[i], payload(300, uint64(i))); err != nil {
			t.Fatal(err)
		}
		placed, _ := s.Locate(keys[i])
		if got := placement.DomainsTolerated(placed, 2); got != 1 {
			t.Fatalf("%s: tolerates %d domains, want 1 (%v)", keys[i], got, placement.PerDomain(placed))
		}
	}
	for z := 1; z <= 3; z++ {
		for i := 0; i < 6; i++ {
			if i%3+1 == z {
				mems[fmt.Sprintf("d%02d", i+1)].SetOffline(true)
			}
		}
		for i, key := range keys {
			got, _, err := s.Get(ctx, key)
			if err != nil || !bytes.Equal(got, payload(300, uint64(i))) {
				t.Fatalf("domain z%d down: get %s: %v", z, key, err)
			}
		}
		for _, m := range mems {
			m.SetOffline(false)
		}
	}
}

func TestGetMissingKey(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	if _, _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing key: %v", err)
	}
	for _, m := range mems {
		m.SetOffline(true)
	}
	if _, _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("all disks down: %v, want ErrUnavailable (cannot tell missing from down)", err)
	}
}

func TestOverwriteReturnsNewestAndRemovesOld(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	r1, err := s.Put(ctx, "k", []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.Put(ctx, "k", []byte("second value"))
	if err != nil {
		t.Fatal(err)
	}
	if !(r2.Version > r1.Version) {
		t.Fatalf("versions not increasing: %s then %s", r1.Version, r2.Version)
	}
	got, info, err := s.Get(ctx, "k")
	if err != nil || string(got) != "second value" || info.Version != r2.Version {
		t.Fatalf("get after overwrite = %q %+v %v", got, info, err)
	}
	for _, m := range shardDisks(t, s, mems, "k") {
		vs, _ := m.Versions(ctx, objectName("k"))
		if len(vs) != 1 || vs[0] != r2.Version {
			t.Fatalf("disk %s holds %v, want only %s", m.ID(), vs, r2.Version)
		}
	}
}

func TestOverwriteKeepsStaleCountWhenCleanupFails(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	if _, err := s.Put(ctx, "k", []byte("one")); err != nil {
		t.Fatal(err)
	}
	sd := shardDisks(t, s, mems, "k")
	// Writes succeed but this disk rejects deletes: simulate with a wrapper that fails Delete.
	s.disks[sd[2].ID()] = failDelete{sd[2]}
	res, err := s.Put(ctx, "k", []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if res.StaleLeft != 1 || res.CleanupErr == nil {
		t.Fatalf("stale=%d cleanupErr=%v, want 1 and an error", res.StaleLeft, res.CleanupErr)
	}
	if got, _, err := s.Get(ctx, "k"); err != nil || string(got) != "two" {
		t.Fatalf("get = %q %v; the stale shard must be ignored", got, err)
	}
}

// failDelete is a disk whose deletes always fail.
type failDelete struct{ disk.Disk }

// Delete always fails.
func (failDelete) Delete(context.Context, string, string) error { return disk.ErrOffline }

// A put that cannot reach all disks fails and leaves the previous version readable.
func TestFailedPutRollsBackAndKeepsOldVersion(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	old := payload(999, 3)
	if _, err := s.Put(ctx, "k", old); err != nil {
		t.Fatal(err)
	}
	sd := shardDisks(t, s, mems, "k")
	sd[4].SetFailWrites(true) // lists fine, so the put gets to the write phase
	if _, err := s.Put(ctx, "k", []byte("new")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("put with a failing disk: %v", err)
	}
	sd[4].SetFailWrites(false)
	for _, m := range sd {
		vs, _ := m.Versions(ctx, objectName("k"))
		if len(vs) != 1 {
			t.Fatalf("disk %s holds %v after rollback, want only the old version", m.ID(), vs)
		}
	}
	got, _, err := s.Get(ctx, "k")
	if err != nil || !bytes.Equal(got, old) {
		t.Fatalf("old version lost: %v", err)
	}
	// A put that cannot even list every disk fails before writing anything.
	sd[1].SetOffline(true)
	if _, err := s.Put(ctx, "k", []byte("x")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("put with an offline disk: %v", err)
	}
	sd[1].SetOffline(false)
	if got, _, _ := s.Get(ctx, "k"); !bytes.Equal(got, old) {
		t.Fatal("old version changed")
	}
}

// A leftover from a failed put (fewer than n shards) is skipped, never served or blocking.
func TestLeftoverOfFailedPutIsSkipped(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	if _, err := s.Put(ctx, "k", []byte("committed")); err != nil {
		t.Fatal(err)
	}
	sd := shardDisks(t, s, mems, "k")
	junk := disk.FormatVersion(99, 1)
	for _, m := range sd[:2] {
		if err := m.Write(ctx, objectName("k"), junk, []byte("garbage")); err != nil {
			t.Fatal(err)
		}
	}
	got, info, err := s.Get(ctx, "k")
	if err != nil || string(got) != "committed" || info.Version == junk {
		t.Fatalf("get = %q %+v %v", got, info, err)
	}
	// Next put gets a higher sequence than the leftover and removes it.
	res, err := s.Put(ctx, "k", []byte("next"))
	if err != nil || !(res.Version > junk) {
		t.Fatalf("put after leftover: %+v %v", res, err)
	}
}

// When a newer version may be committed but too few of its shards are reachable, an older
// version must not be served instead.
func TestNoFallbackToOlderVersionWhenNewerMayExist(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	if _, err := s.Put(ctx, "k", []byte("old")); err != nil {
		t.Fatal(err)
	}
	sd := shardDisks(t, s, mems, "k")
	oldVersions, _ := sd[0].Versions(ctx, objectName("k"))
	// Keep a copy of the old version's shards, then overwrite.
	saved := make([][]byte, 6)
	for i, m := range sd {
		saved[i], _ = m.Read(ctx, objectName("k"), oldVersions[0])
	}
	if _, err := s.Put(ctx, "k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	for i, m := range sd { // put the old shards back, as if cleanup had failed everywhere
		if err := m.Write(ctx, objectName("k"), oldVersions[0], saved[i]); err != nil {
			t.Fatal(err)
		}
	}
	// Three disks down: the new version shows 3 shards (< k) but 3 + 3 unreachable = n.
	for _, m := range sd[:3] {
		m.SetOffline(true)
	}
	if got, _, err := s.Get(ctx, "k"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("get = %q, %v; want ErrUnavailable, not the old version", got, err)
	}
}

func TestCorruptShardIsDetectedNotServed(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	data := payload(2000, 5)
	res, err := s.Put(ctx, "k", data)
	if err != nil {
		t.Fatal(err)
	}
	sd := shardDisks(t, s, mems, "k")
	// Flip the last byte of data shard 1 (payload, not header).
	raw, _ := sd[1].Read(ctx, objectName("k"), res.Version)
	if err := sd[1].Flip(objectName("k"), res.Version, len(raw)-1); err != nil {
		t.Fatal(err)
	}
	if got, _, err := s.Get(ctx, "k"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt data shard served: err=%v equal=%v", err, bytes.Equal(got, data))
	}
	// With the corrupt disk offline the object reads correctly from the other shards.
	sd[1].SetOffline(true)
	if got, _, err := s.Get(ctx, "k"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("get without the corrupt shard: %v", err)
	}
}

func TestBadShardFilesAreTreatedAsMissing(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	data := payload(800, 6)
	res, err := s.Put(ctx, "k", data)
	if err != nil {
		t.Fatal(err)
	}
	sd := shardDisks(t, s, mems, "k")
	obj := objectName("k")
	// Shard 0: garbage. Shard 1: a valid shard file for the wrong index (copied from shard 2).
	if err := sd[0].Write(ctx, obj, res.Version, []byte("not a shard")); err != nil {
		t.Fatal(err)
	}
	other, _ := sd[2].Read(ctx, obj, res.Version)
	if err := sd[1].Write(ctx, obj, res.Version, other); err != nil {
		t.Fatal(err)
	}
	got, info, err := s.Get(ctx, "k")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("get with two bad shard files: %v", err)
	}
	if fmt.Sprint(info.Missing) != "[0 1]" {
		t.Fatalf("missing = %v, want [0 1]", info.Missing)
	}
	// A third bad file leaves only 3 good shards.
	if err := sd[5].Write(ctx, obj, res.Version, []byte("ECS1")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get(ctx, "k"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("three bad shards: %v", err)
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	if _, err := s.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	n, err := s.Delete(ctx, "k")
	if err != nil || n != 6 {
		t.Fatalf("delete = %d %v, want 6 shard files", n, err)
	}
	if n, err := s.Delete(ctx, "k"); err != nil || n != 0 {
		t.Fatalf("second delete = %d %v", n, err)
	}
	if _, _, err := s.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	for _, m := range mems {
		if m.Bytes() != 0 {
			t.Fatalf("disk %s still holds %d bytes", m.ID(), m.Bytes())
		}
	}
	if _, err := s.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	shardDisks(t, s, mems, "k")[0].SetOffline(true)
	if n, err := s.Delete(ctx, "k"); !errors.Is(err, ErrUnavailable) || n != 5 {
		t.Fatalf("delete with a disk down = %d %v, want 5 and ErrUnavailable", n, err)
	}
}

// Repair rebuilds shards of replaced (wiped) disks; the object then survives m other losses.
func TestRepairRebuildsWipedDisks(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	data := payload(5000, 8)
	if _, err := s.Put(ctx, "k", data); err != nil {
		t.Fatal(err)
	}
	sd := shardDisks(t, s, mems, "k")
	sd[0].Wipe()
	sd[4].Wipe()
	res, err := s.Repair(ctx, "k")
	if err != nil || fmt.Sprint(res.Rewritten) != "[0 4]" {
		t.Fatalf("repair = %+v %v, want shards [0 4]", res, err)
	}
	if res, err := s.Repair(ctx, "k"); err != nil || len(res.Rewritten) != 0 {
		t.Fatalf("second repair = %+v %v, want nothing to do", res, err)
	}
	for _, i := range []int{1, 2, 3, 5} {
		sd[i].SetOffline(true)
	}
	sd[1].SetOffline(false)
	sd[2].SetOffline(false) // now 3 and 5 are down: the rebuilt 0 and 4 must carry the read
	got, info, err := s.Get(ctx, "k")
	if err != nil || !bytes.Equal(got, data) || fmt.Sprint(info.Missing) != "[3 5]" {
		t.Fatalf("read after repair: %+v %v", info, err)
	}
}

func TestRepairRefusesCorruptObjectAndSkipsDownDisks(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	res, err := s.Put(ctx, "k", payload(400, 9))
	if err != nil {
		t.Fatal(err)
	}
	sd := shardDisks(t, s, mems, "k")
	sd[5].Wipe()
	sd[4].SetOffline(true)
	rep, err := s.Repair(ctx, "k")
	if err != nil || fmt.Sprint(rep.Rewritten) != "[5]" {
		t.Fatalf("repair with a disk down = %+v %v", rep, err)
	}
	sd[4].SetOffline(false)
	raw, _ := sd[0].Read(ctx, objectName("k"), res.Version)
	if err := sd[0].Flip(objectName("k"), res.Version, len(raw)-1); err != nil {
		t.Fatal(err)
	}
	sd[3].Wipe()
	if _, err := s.Repair(ctx, "k"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("repair of corrupt object: %v", err)
	}
	if vs, _ := sd[3].Versions(ctx, objectName("k")); len(vs) != 0 {
		t.Fatal("repair wrote a shard from corrupt data")
	}
}

func TestInputValidation(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	for _, key := range []string{"", strings.Repeat("k", MaxKeyLen+1), "bad\x00key", string([]byte{0xff, 0xfe})} {
		if _, err := s.Put(ctx, key, []byte("x")); !errors.Is(err, ErrInvalid) {
			t.Errorf("put key %q: %v", key, err)
		}
		if _, _, err := s.Get(ctx, key); !errors.Is(err, ErrInvalid) {
			t.Errorf("get key %q: %v", key, err)
		}
		if _, err := s.Delete(ctx, key); !errors.Is(err, ErrInvalid) {
			t.Errorf("delete key %q: %v", key, err)
		}
		if _, err := s.Repair(ctx, key); !errors.Is(err, ErrInvalid) {
			t.Errorf("repair key %q: %v", key, err)
		}
	}
	if _, err := s.Put(ctx, strings.Repeat("k", MaxKeyLen), nil); err != nil {
		t.Errorf("max-length key rejected: %v", err)
	}
	if _, err := s.Put(ctx, "big", make([]byte, MaxObjectSize+1)); !errors.Is(err, ErrInvalid) {
		t.Errorf("oversized object: %v", err)
	}
	for _, m := range mems {
		if m.Bytes() > 20<<10 {
			t.Fatalf("rejected input reached disk %s", m.ID())
		}
	}
}

func TestRetriesAreBoundedAndTimeoutsApply(t *testing.T) {
	ctx := context.Background()
	s, mems := cluster(t, 6, 3, 4, 2)
	if _, err := s.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	calls := &countingDisk{Disk: s.disks["d01"]}
	s.disks["d01"] = calls
	mems["d01"].SetOffline(true)
	if _, _, err := s.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if got := calls.n.Load(); got != int64(1+s.cfg.Retries) {
		t.Fatalf("offline disk called %d times, want %d (1 + retries)", got, 1+s.cfg.Retries)
	}
	mems["d01"].SetOffline(false)
	// A hung disk costs at most Timeout per attempt, not forever.
	mems["d02"].SetDelay(time.Hour)
	start := time.Now()
	if _, _, err := s.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("get with a hung disk took %s", el)
	}
	// Caller cancellation stops everything.
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	for _, m := range mems {
		m.SetDelay(time.Hour)
	}
	if _, _, err := s.Get(cctx, "k"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("cancelled get: %v", err)
	}
}

// countingDisk counts Versions calls.
type countingDisk struct {
	disk.Disk
	n atomic.Int64
}

// Versions counts and forwards.
func (c *countingDisk) Versions(ctx context.Context, o string) ([]string, error) {
	c.n.Add(1)
	return c.Disk.Versions(ctx, o)
}

func TestPermanentErrorsAreNotRetried(t *testing.T) {
	s, _ := cluster(t, 6, 3, 4, 2)
	for _, err := range []error{disk.ErrInvalid, disk.ErrNotFound, ErrBadShard} {
		calls := 0
		got := s.call(context.Background(), func(context.Context) error { calls++; return err })
		if !errors.Is(got, err) || calls != 1 {
			t.Errorf("%v: %d calls, want 1", err, calls)
		}
	}
	calls := 0
	_ = s.call(context.Background(), func(context.Context) error { calls++; return disk.ErrOffline })
	if calls != 2 {
		t.Errorf("transient error: %d calls, want 2 (Retries=1)", calls)
	}
}

// Concurrent puts and gets of the same keys never return a mix of versions.
func TestConcurrentPutsAndGets(t *testing.T) {
	ctx := context.Background()
	s, _ := cluster(t, 6, 3, 4, 2)
	values := map[string]bool{}
	for w := 0; w < 4; w++ {
		for i := 0; i < 10; i++ {
			values[fmt.Sprintf("writer-%d-round-%d-%s", w, i, strings.Repeat("x", 100*w))] = true
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for w := 0; w < 4; w++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				v := fmt.Sprintf("writer-%d-round-%d-%s", w, i, strings.Repeat("x", 100*w))
				if _, err := s.Put(ctx, fmt.Sprintf("shared-%d", i%3), []byte(v)); err != nil {
					errs <- err
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				got, _, err := s.Get(ctx, fmt.Sprintf("shared-%d", i%3))
				if errors.Is(err, ErrNotFound) {
					continue
				}
				if err != nil {
					errs <- err
				} else if !values[string(got)] {
					errs <- fmt.Errorf("read a value nobody wrote: %q", got)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if len(s.locks.m) != 0 {
		t.Fatalf("%d key locks leaked", len(s.locks.m))
	}
}

func TestShardFileFormat(t *testing.T) {
	h := Header{Key: "k", Version: disk.FormatVersion(1, 2), Size: 5, K: 2, M: 1, Index: 2, ShardSize: 3,
		Disks: []string{"a", "b", "c"}, SHA256: strings.Repeat("0", 64)}
	file, err := encodeShard(h, []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	got, shard, err := decodeShard(file)
	if err != nil || !got.sameObject(h) || got.Index != 2 || !bytes.Equal(shard, []byte{1, 2, 3}) {
		t.Fatalf("round trip: %+v %v %v", got, shard, err)
	}
	mutate := func(f func(*Header)) []byte {
		c := h
		c.Disks = append([]string(nil), h.Disks...)
		f(&c)
		b, _ := encodeShard(c, []byte{1, 2, 3})
		return b
	}
	bad := map[string][]byte{
		"empty":         nil,
		"magic":         append([]byte("XXXX"), file[4:]...),
		"header length": append([]byte("ECS1\xff\xff\xff\xff"), file[8:]...),
		"truncated":     file[:len(file)-1],
		"extra byte":    append(append([]byte(nil), file...), 0),
		"json":          append([]byte("ECS1\x00\x00\x00\x01{"), 1, 2, 3),
		"version":       mutate(func(c *Header) { c.Version = "v1" }),
		"k":             mutate(func(c *Header) { c.K = 0 }),
		"index":         mutate(func(c *Header) { c.Index = 3 }),
		"disks":         mutate(func(c *Header) { c.Disks = c.Disks[:2] }),
		"size":          mutate(func(c *Header) { c.Size = 7 }),
		"negative size": mutate(func(c *Header) { c.Size = -1 }),
		"checksum":      mutate(func(c *Header) { c.SHA256 = "abc" }),
	}
	for name, b := range bad {
		if _, _, err := decodeShard(b); !errors.Is(err, ErrBadShard) {
			t.Errorf("%s: err=%v, want ErrBadShard", name, err)
		}
	}
	// Unknown fields from a later format version are ignored.
	hdr, _ := json.Marshal(h)
	hdr = bytes.Replace(hdr, []byte(`{"key"`), []byte(`{"future":1,"key"`), 1)
	withExtra := binary.BigEndian.AppendUint32([]byte(magic), uint32(len(hdr)))
	withExtra = append(append(withExtra, hdr...), 1, 2, 3)
	if _, _, err := decodeShard(withExtra); err != nil {
		t.Fatalf("unknown field rejected: %v", err)
	}
}
