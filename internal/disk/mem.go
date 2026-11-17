package disk

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Mem is an in-memory disk with injectable faults, for tests and in-process clusters.
type Mem struct {
	id string

	mu         sync.Mutex
	files      map[string]map[string][]byte // object -> version -> bytes
	offline    bool
	failWrites bool
	delay      time.Duration
}

// NewMem returns an empty in-memory disk.
func NewMem(id string) *Mem { return &Mem{id: id, files: map[string]map[string][]byte{}} }

// ID returns the disk id.
func (m *Mem) ID() string { return m.id }

// SetOffline makes every call fail with ErrOffline (true) or work again (false).
func (m *Mem) SetOffline(v bool) { m.mu.Lock(); m.offline = v; m.mu.Unlock() }

// SetFailWrites makes Write fail with ErrOffline while reads still work.
func (m *Mem) SetFailWrites(v bool) { m.mu.Lock(); m.failWrites = v; m.mu.Unlock() }

// SetDelay makes every call wait d (or until its context ends) before running.
func (m *Mem) SetDelay(d time.Duration) { m.mu.Lock(); m.delay = d; m.mu.Unlock() }

// Wipe deletes every shard, like replacing the disk with an empty one.
func (m *Mem) Wipe() { m.mu.Lock(); m.files = map[string]map[string][]byte{}; m.mu.Unlock() }

// Flip XORs one byte of a stored shard file to simulate silent corruption.
func (m *Mem) Flip(object, version string, offset int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[object][version]
	if !ok {
		return ErrNotFound
	}
	if offset < 0 || offset >= len(data) {
		return fmt.Errorf("%w: offset %d outside %d bytes", ErrInvalid, offset, len(data))
	}
	data[offset] ^= 0xff
	return nil
}

// Bytes returns the total size of stored shard files.
func (m *Mem) Bytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, vs := range m.files {
		for _, b := range vs {
			n += int64(len(b))
		}
	}
	return n
}

// enter applies the injected delay and offline fault.
func (m *Mem) enter(ctx context.Context, write bool) error {
	m.mu.Lock()
	delay, offline, failWrites := m.delay, m.offline, m.failWrites
	m.mu.Unlock()
	if delay > 0 {
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if offline || (write && failWrites) {
		return fmt.Errorf("disk %s: %w", m.id, ErrOffline)
	}
	return nil
}

// Versions lists the shard versions stored for an object, oldest first.
func (m *Mem) Versions(ctx context.Context, object string) ([]string, error) {
	if err := checkObject(object); err != nil {
		return nil, err
	}
	if err := m.enter(ctx, false); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for v := range m.files[object] {
		out = append(out, v)
	}
	sort.Strings(out)
	return out, nil
}

// Read returns a copy of one shard file.
func (m *Mem) Read(ctx context.Context, object, version string) ([]byte, error) {
	if err := check(object, version); err != nil {
		return nil, err
	}
	if err := m.enter(ctx, false); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[object][version]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

// Write stores a copy of one shard file.
func (m *Mem) Write(ctx context.Context, object, version string, data []byte) error {
	if err := check(object, version); err != nil {
		return err
	}
	if len(data) > MaxShardFile {
		return fmt.Errorf("%w: shard file is %d bytes, limit %d", ErrInvalid, len(data), MaxShardFile)
	}
	if err := m.enter(ctx, true); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.files[object] == nil {
		m.files[object] = map[string][]byte{}
	}
	m.files[object][version] = append([]byte(nil), data...)
	return nil
}

// Delete removes one shard file; a missing file is not an error.
func (m *Mem) Delete(ctx context.Context, object, version string) error {
	if err := check(object, version); err != nil {
		return err
	}
	if err := m.enter(ctx, true); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files[object], version)
	if len(m.files[object]) == 0 {
		delete(m.files, object)
	}
	return nil
}
