// Package store is the erasure-coded object store: it splits each object into k data and m
// parity shards, places them on k + m distinct disks spread over failure domains, and reads an
// object back from any k of its shards.
package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/EquinoxWN/erasure-coded-object-store/internal/disk"
	"github.com/EquinoxWN/erasure-coded-object-store/internal/placement"
	"github.com/EquinoxWN/erasure-coded-object-store/internal/rs"
)

// Input limits.
const (
	MaxKeyLen     = 1024
	MaxObjectSize = 64 << 20
)

var (
	// ErrNotFound means no disk holds the object.
	ErrNotFound = errors.New("store: object not found")
	// ErrUnavailable means too few shards could be read or written to complete the operation.
	ErrUnavailable = errors.New("store: not enough shards available")
	// ErrCorrupt means the shards decoded to bytes whose SHA-256 does not match the object's.
	ErrCorrupt = errors.New("store: object checksum mismatch")
	// ErrInvalid means the key, object or configuration was rejected.
	ErrInvalid = errors.New("store: invalid input")
)

// Member is one disk of the cluster and the failure domain it sits in.
type Member struct {
	Disk   disk.Disk
	Domain string
}

// Config holds the code parameters and per-call limits.
type Config struct {
	K, M    int           // data and parity shards per object
	Timeout time.Duration // limit for each disk call attempt
	Retries int           // extra attempts after a transient disk error (0..5)
	Backoff time.Duration // first retry delay, doubled each time, capped at 1 s
}

// DefaultConfig is 4 data + 2 parity shards (1.5x storage, any 2 disks may fail).
func DefaultConfig() Config {
	return Config{K: 4, M: 2, Timeout: 5 * time.Second, Retries: 2, Backoff: 50 * time.Millisecond}
}

// Validate checks the configuration.
func (c Config) Validate() error {
	if c.K < 1 || c.K > rs.MaxData || c.M < 1 || c.M > rs.MaxParity {
		return fmt.Errorf("%w: k=%d must be 1..%d and m=%d must be 1..%d", ErrInvalid, c.K, rs.MaxData, c.M, rs.MaxParity)
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("%w: timeout must be positive", ErrInvalid)
	}
	if c.Retries < 0 || c.Retries > 5 {
		return fmt.Errorf("%w: retries=%d must be 0..5", ErrInvalid, c.Retries)
	}
	if c.Backoff < 0 {
		return fmt.Errorf("%w: backoff must not be negative", ErrInvalid)
	}
	return nil
}

// Store reads and writes erasure-coded objects; it is safe for concurrent use.
type Store struct {
	cfg    Config
	codec  *rs.Codec
	placer *placement.Placer
	disks  map[string]disk.Disk
	locks  keyLocks
}

// New returns a store over the given disks.
func New(members []Member, cfg Config) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	codec, err := rs.New(cfg.K, cfg.M)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	infos := make([]placement.Disk, 0, len(members))
	disks := make(map[string]disk.Disk, len(members))
	for _, mb := range members {
		if mb.Disk == nil {
			return nil, fmt.Errorf("%w: nil disk", ErrInvalid)
		}
		infos = append(infos, placement.Disk{ID: mb.Disk.ID(), Domain: mb.Domain})
		disks[mb.Disk.ID()] = mb.Disk
	}
	placer, err := placement.New(infos)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if n := codec.TotalShards(); n > len(infos) {
		return nil, fmt.Errorf("%w: %w: k+m=%d shards need %d distinct disks, got %d",
			ErrInvalid, placement.ErrNotEnoughDisks, n, n, len(infos))
	}
	return &Store{cfg: cfg, codec: codec, placer: placer, disks: disks}, nil
}

// Config returns the store's configuration.
func (s *Store) Config() Config { return s.cfg }

// ValidateKey checks an object key: 1..1024 bytes of UTF-8 without NUL.
func ValidateKey(key string) error {
	if key == "" || len(key) > MaxKeyLen {
		return fmt.Errorf("%w: key must be 1..%d bytes, got %d", ErrInvalid, MaxKeyLen, len(key))
	}
	if !utf8.ValidString(key) {
		return fmt.Errorf("%w: key is not valid UTF-8", ErrInvalid)
	}
	for _, r := range key {
		if r == 0 {
			return fmt.Errorf("%w: key contains NUL", ErrInvalid)
		}
	}
	return nil
}

// objectName is the on-disk name of a key: hex SHA-256, so keys never become paths.
func objectName(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Locate returns the k + m disks that hold key's shards, shard i on disk i.
func (s *Store) Locate(key string) ([]placement.Disk, error) {
	if err := ValidateKey(key); err != nil {
		return nil, err
	}
	return s.placer.Place(key, s.codec.TotalShards())
}

// PutResult describes a stored object version.
type PutResult struct {
	Version   string
	Disks     []placement.Disk
	ShardSize int
	// StaleLeft counts older shard files that could not be deleted; reads ignore them and the
	// next Put or Delete of the key retries the removal. CleanupErr says why.
	StaleLeft  int
	CleanupErr error
}

// Put stores data under key as a new version on all k + m disks; it fails unless every shard is
// written, and then removes the shards it did write.
func (s *Store) Put(ctx context.Context, key string, data []byte) (PutResult, error) {
	if err := ValidateKey(key); err != nil {
		return PutResult{}, err
	}
	if len(data) > MaxObjectSize {
		return PutResult{}, fmt.Errorf("%w: object is %d bytes, limit %d", ErrInvalid, len(data), MaxObjectSize)
	}
	unlock := s.locks.lock(key)
	defer unlock()
	disks, err := s.Locate(key)
	if err != nil {
		return PutResult{}, err
	}
	obj := objectName(key)
	listed, listErrs := s.listAll(ctx, obj, disks)
	if err := errors.Join(listErrs...); err != nil {
		return PutResult{}, fmt.Errorf("%w: put %q needs all %d disks: %w", ErrUnavailable, key, len(disks), err)
	}
	var maxSeq uint64
	for _, vs := range listed {
		for _, v := range vs {
			seq, _ := seqOf(v) // listAll already rejected unparsable names
			maxSeq = max(maxSeq, seq)
		}
	}
	if maxSeq == math.MaxUint64 {
		return PutResult{}, fmt.Errorf("%w: version sequence of %q exhausted", ErrInvalid, key)
	}
	id, err := randomID()
	if err != nil {
		return PutResult{}, err
	}
	version := disk.FormatVersion(maxSeq+1, id)
	sum := sha256.Sum256(data)
	shards := s.codec.Split(data)
	if err := s.codec.Encode(shards); err != nil {
		return PutResult{}, err
	}
	ids := make([]string, len(disks))
	for i, d := range disks {
		ids[i] = d.ID
	}
	hdr := Header{Key: key, Version: version, Size: int64(len(data)), K: s.cfg.K, M: s.cfg.M,
		ShardSize: len(shards[0]), Disks: ids, SHA256: hex.EncodeToString(sum[:])}
	files := make([][]byte, len(disks))
	for i := range disks {
		hdr.Index = i
		if files[i], err = encodeShard(hdr, shards[i]); err != nil {
			return PutResult{}, err
		}
	}
	writeErrs := s.fanout(ctx, len(disks), func(ctx context.Context, i int) error {
		d := s.disks[disks[i].ID]
		return s.call(ctx, func(ctx context.Context) error { return d.Write(ctx, obj, version, files[i]) })
	})
	if err := errors.Join(writeErrs...); err != nil {
		// Roll back on every disk: a write that timed out may still have landed.
		rollback := s.fanout(context.WithoutCancel(ctx), len(disks), func(ctx context.Context, i int) error {
			d := s.disks[disks[i].ID]
			return s.call(ctx, func(ctx context.Context) error { return d.Delete(ctx, obj, version) })
		})
		return PutResult{}, fmt.Errorf("%w: put %q: %w", ErrUnavailable, key, errors.Join(err, joinNonNil("rollback", rollback)))
	}
	res := PutResult{Version: version, Disks: disks, ShardSize: hdr.ShardSize}
	cleanup := s.fanout(context.WithoutCancel(ctx), len(disks), func(ctx context.Context, i int) error {
		d := s.disks[disks[i].ID]
		var errs []error
		for _, v := range listed[i] {
			if err := s.call(ctx, func(ctx context.Context) error { return d.Delete(ctx, obj, v) }); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	})
	for _, e := range cleanup {
		if e != nil {
			res.StaleLeft++
		}
	}
	res.CleanupErr = errors.Join(cleanup...)
	return res, nil
}

// GetInfo describes how an object was read.
type GetInfo struct {
	Version string
	Missing []int // shard indexes that were unavailable and rebuilt from the others
}

// Get reads key from any k of its k + m shards and checks the object's SHA-256.
func (s *Store) Get(ctx context.Context, key string) ([]byte, GetInfo, error) {
	if err := ValidateKey(key); err != nil {
		return nil, GetInfo{}, err
	}
	st, err := s.load(ctx, key)
	if err != nil {
		return nil, GetInfo{}, err
	}
	data, err := s.decode(st)
	if err != nil {
		return nil, GetInfo{}, err
	}
	return data, GetInfo{Version: st.hdr.Version, Missing: st.missing()}, nil
}

// Delete removes every version of key from its disks and returns how many shard files it removed.
// It is safe to repeat; deleting a missing key returns 0 and no error.
func (s *Store) Delete(ctx context.Context, key string) (int, error) {
	disks, err := s.Locate(key)
	if err != nil {
		return 0, err
	}
	unlock := s.locks.lock(key)
	defer unlock()
	obj := objectName(key)
	listed, listErrs := s.listAll(ctx, obj, disks)
	var mu sync.Mutex
	removed := 0
	delErrs := s.fanout(ctx, len(disks), func(ctx context.Context, i int) error {
		d := s.disks[disks[i].ID]
		var errs []error
		for _, v := range listed[i] {
			if err := s.call(ctx, func(ctx context.Context) error { return d.Delete(ctx, obj, v) }); err != nil {
				errs = append(errs, err)
				continue
			}
			mu.Lock()
			removed++
			mu.Unlock()
		}
		return errors.Join(errs...)
	})
	if err := errors.Join(errors.Join(listErrs...), errors.Join(delErrs...)); err != nil {
		return removed, fmt.Errorf("%w: delete %q incomplete: %w", ErrUnavailable, key, err)
	}
	return removed, nil
}

// RepairResult lists which shards Repair rewrote.
type RepairResult struct {
	Version   string
	Rewritten []int // shard indexes rebuilt and written back
}

// Repair rebuilds the shards of the current version that are missing from reachable disks
// (for example after a disk was replaced) and writes them back.
func (s *Store) Repair(ctx context.Context, key string) (RepairResult, error) {
	if err := ValidateKey(key); err != nil {
		return RepairResult{}, err
	}
	unlock := s.locks.lock(key)
	defer unlock()
	st, err := s.load(ctx, key)
	if err != nil {
		return RepairResult{}, err
	}
	if _, err := s.decode(st); err != nil { // verifies the checksum before anything is written
		return RepairResult{}, err
	}
	res := RepairResult{Version: st.hdr.Version}
	hdr := st.hdr
	var targets []int
	for i := range st.disks {
		if st.listErrs[i] == nil && !slices.Contains(st.listed[i], hdr.Version) {
			targets = append(targets, i)
		}
	}
	files := make([][]byte, len(targets))
	for t, i := range targets {
		hdr.Index = i
		if files[t], err = encodeShard(hdr, st.shards[i]); err != nil {
			return res, err
		}
	}
	errs := s.fanout(ctx, len(targets), func(ctx context.Context, t int) error {
		d := s.disks[st.disks[targets[t]].ID]
		return s.call(ctx, func(ctx context.Context) error {
			return d.Write(ctx, st.obj, hdr.Version, files[t])
		})
	})
	for t, e := range errs {
		if e == nil {
			res.Rewritten = append(res.Rewritten, targets[t])
		}
	}
	if err := errors.Join(errs...); err != nil {
		return res, fmt.Errorf("%w: repair %q: %w", ErrUnavailable, key, err)
	}
	return res, nil
}

// loadState is what a read found on the object's disks.
type loadState struct {
	obj      string
	disks    []placement.Disk
	listed   [][]string
	listErrs []error
	hdr      Header
	shards   [][]byte // nil where unavailable; filled in by decode
	have     []bool   // shard read from disk (not rebuilt)
}

// missing returns the shard indexes that were not read from disk.
func (st *loadState) missing() []int {
	var out []int
	for i, ok := range st.have {
		if !ok {
			out = append(out, i)
		}
	}
	return out
}

// maxLoadAttempts bounds how often a read starts over because a concurrent overwrite removed
// the version it had chosen between listing and reading.
const maxLoadAttempts = 5

// load reads the newest readable version, starting over (after the retry backoff) if that
// version vanished mid-read.
func (s *Store) load(ctx context.Context, key string) (*loadState, error) {
	var err error
	for attempt := 0; attempt < maxLoadAttempts; attempt++ {
		if attempt > 0 {
			t := time.NewTimer(s.cfg.Backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, errors.Join(err, ctx.Err())
			case <-t.C:
			}
		}
		var st *loadState
		st, err = s.loadOnce(ctx, key)
		if err == nil || !errors.Is(err, errVanished) || ctx.Err() != nil {
			return st, err
		}
	}
	return nil, err
}

// errVanished marks a read whose chosen version was deleted after it was listed.
var errVanished = errors.New("version removed while reading")

// loadOnce lists versions, picks the newest readable one and reads its shards.
func (s *Store) loadOnce(ctx context.Context, key string) (*loadState, error) {
	disks, err := s.Locate(key)
	if err != nil {
		return nil, err
	}
	n, k := len(disks), s.cfg.K
	st := &loadState{obj: objectName(key), disks: disks}
	st.listed, st.listErrs = s.listAll(ctx, st.obj, disks)
	unreachable := 0
	count := map[string]int{}
	for i := range disks {
		if st.listErrs[i] != nil {
			unreachable++
			continue
		}
		for _, v := range st.listed[i] {
			count[v]++
		}
	}
	if len(count) == 0 {
		if unreachable >= n {
			return nil, fmt.Errorf("%w: no disk of %q answered: %w", ErrUnavailable, key, errors.Join(st.listErrs...))
		}
		return nil, ErrNotFound
	}
	versions := make([]string, 0, len(count))
	for v := range count {
		versions = append(versions, v)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(versions)))
	// A committed version has a shard on all n disks. A newer version that may be committed
	// (seen + unreachable >= n) but has fewer than k shards visible blocks the read rather than
	// letting an older version be served; one that cannot be committed (a failed put) is skipped.
	for _, v := range versions {
		if count[v] >= k {
			if err := s.readVersion(ctx, st, key, v); err != nil {
				return nil, err
			}
			return st, nil
		}
		if count[v]+unreachable >= n {
			return nil, fmt.Errorf("%w: %q version %s has %d of %d shards reachable, need %d: %w",
				ErrUnavailable, key, v, count[v], n, k, errors.Join(st.listErrs...))
		}
	}
	return nil, fmt.Errorf("%w: %q has no version with %d shards (only failed writes found)", ErrNotFound, key, k)
}

// readVersion reads every listed shard of version v and keeps those that agree with each other.
func (s *Store) readVersion(ctx context.Context, st *loadState, key, v string) error {
	n := len(st.disks)
	type got struct {
		hdr   Header
		shard []byte
	}
	results := make([]*got, n)
	readErrs := s.fanout(ctx, n, func(ctx context.Context, i int) error {
		if st.listErrs[i] != nil || !slices.Contains(st.listed[i], v) {
			return nil
		}
		d := s.disks[st.disks[i].ID]
		var data []byte
		err := s.call(ctx, func(ctx context.Context) error {
			var e error
			data, e = d.Read(ctx, st.obj, v)
			return e
		})
		if err != nil {
			return err
		}
		h, shard, err := decodeShard(data)
		if err != nil {
			return fmt.Errorf("disk %s: %w", d.ID(), err)
		}
		if h.Key != key || h.Version != v || h.Index != i || h.Disks[i] != st.disks[i].ID || h.K != s.cfg.K || h.M != s.cfg.M {
			return fmt.Errorf("%w: disk %s holds shard %d of %s, expected shard %d of %s with k=%d m=%d",
				ErrBadShard, d.ID(), h.Index, h.Version, i, v, s.cfg.K, s.cfg.M)
		}
		results[i] = &got{h, shard}
		return nil
	})
	// Group the shards by header and keep the largest agreeing group.
	best, bestCount := -1, 0
	for i, r := range results {
		if r == nil {
			continue
		}
		c := 0
		for _, o := range results {
			if o != nil && o.hdr.sameObject(r.hdr) {
				c++
			}
		}
		if c > bestCount {
			best, bestCount = i, c
		}
	}
	if bestCount < s.cfg.K {
		err := fmt.Errorf("%w: version %s: %d consistent shards, need %d: %w",
			ErrUnavailable, v, bestCount, s.cfg.K, errors.Join(readErrs...))
		for _, e := range readErrs {
			if errors.Is(e, disk.ErrNotFound) {
				return fmt.Errorf("%w: %w", errVanished, err)
			}
		}
		return err
	}
	st.hdr = results[best].hdr
	st.shards = make([][]byte, n)
	st.have = make([]bool, n)
	for i, r := range results {
		if r != nil && r.hdr.sameObject(st.hdr) {
			st.shards[i] = r.shard
			st.have[i] = true
		}
	}
	return nil
}

// decode rebuilds missing shards, joins the data shards and checks the SHA-256.
func (s *Store) decode(st *loadState) ([]byte, error) {
	if err := s.codec.Reconstruct(st.shards); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	data, err := s.codec.Join(st.shards, int(st.hdr.Size))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != st.hdr.SHA256 {
		return nil, fmt.Errorf("%w: version %s", ErrCorrupt, st.hdr.Version)
	}
	return data, nil
}

// listAll lists versions of obj on every disk concurrently.
func (s *Store) listAll(ctx context.Context, obj string, disks []placement.Disk) ([][]string, []error) {
	listed := make([][]string, len(disks))
	errs := s.fanout(ctx, len(disks), func(ctx context.Context, i int) error {
		d := s.disks[disks[i].ID]
		return s.call(ctx, func(ctx context.Context) error {
			vs, err := d.Versions(ctx, obj)
			if err != nil {
				return err
			}
			for _, v := range vs {
				if _, err := seqOf(v); err != nil {
					return fmt.Errorf("%w: disk %s listed version %q", ErrBadShard, d.ID(), v)
				}
			}
			listed[i] = vs
			return nil
		})
	})
	return listed, errs
}

// fanout runs fn for 0..n-1 concurrently and returns each call's error.
func (s *Store) fanout(ctx context.Context, n int, fn func(context.Context, int) error) []error {
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(ctx, i)
		}()
	}
	wg.Wait()
	return errs
}

// permanent reports errors that a retry cannot fix.
func permanent(err error) bool {
	return errors.Is(err, disk.ErrInvalid) || errors.Is(err, disk.ErrNotFound) || errors.Is(err, ErrBadShard)
}

// call runs one disk operation with a per-attempt timeout and bounded, backed-off retries.
func (s *Store) call(ctx context.Context, op func(context.Context) error) error {
	backoff := s.cfg.Backoff
	var err error
	for attempt := 0; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
		err = op(actx)
		cancel()
		if err == nil || permanent(err) || ctx.Err() != nil || attempt >= s.cfg.Retries {
			return err
		}
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return errors.Join(err, ctx.Err())
		case <-t.C:
		}
		backoff = min(2*backoff, time.Second)
	}
}

// joinNonNil wraps the non-nil errors of a fan-out under a label, or returns nil.
func joinNonNil(label string, errs []error) error {
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	return nil
}

// seqOf parses the sequence number of a version name.
func seqOf(v string) (uint64, error) {
	if !disk.ValidVersion(v) {
		return 0, fmt.Errorf("%w: version %q", ErrBadShard, v)
	}
	return strconv.ParseUint(v[1:21], 10, 64)
}

// randomID returns 64 random bits for a version name.
func randomID() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

// keyLocks serialises Put, Delete and Repair of the same key inside one process.
type keyLocks struct {
	mu sync.Mutex
	m  map[string]*keyLock
}

type keyLock struct {
	mu   sync.Mutex
	refs int
}

// lock takes the key's lock and returns the function that releases it.
func (l *keyLocks) lock(key string) func() {
	l.mu.Lock()
	if l.m == nil {
		l.m = map[string]*keyLock{}
	}
	kl := l.m[key]
	if kl == nil {
		kl = &keyLock{}
		l.m[key] = kl
	}
	kl.refs++
	l.mu.Unlock()
	kl.mu.Lock()
	return func() {
		kl.mu.Unlock()
		l.mu.Lock()
		kl.refs--
		if kl.refs == 0 {
			delete(l.m, key)
		}
		l.mu.Unlock()
	}
}
