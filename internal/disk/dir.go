package disk

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const shardExt = ".shard"

// Dir is a disk backed by a local directory: <root>/<object[:2]>/<object>/<version>.shard.
type Dir struct {
	id   string
	root string
}

// NewDir returns a directory disk, creating root if needed.
func NewDir(id, root string) (*Dir, error) {
	if id == "" || root == "" {
		return nil, fmt.Errorf("%w: disk id and root are required", ErrInvalid)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o750); err != nil {
		return nil, err
	}
	return &Dir{id: id, root: abs}, nil
}

// ID returns the disk id.
func (d *Dir) ID() string { return d.id }

// objectDir is the directory holding an object's shard files.
func (d *Dir) objectDir(object string) string {
	return filepath.Join(d.root, object[:2], object)
}

// Versions lists the shard versions stored for an object, oldest first.
func (d *Dir) Versions(ctx context.Context, object string) ([]string, error) {
	if err := checkObject(object); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(d.objectDir(object))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), shardExt)
		if ok && e.Type().IsRegular() && ValidVersion(name) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// Read returns one shard file.
func (d *Dir) Read(ctx context.Context, object, version string) ([]byte, error) {
	if err := check(object, version); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path := filepath.Join(d.objectDir(object), version+shardExt)
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if info.Size() > MaxShardFile {
		return nil, fmt.Errorf("%w: shard file %s is %d bytes", ErrInvalid, path, info.Size())
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return data, err
}

// Write stores one shard file through a synced temporary file and a rename.
func (d *Dir) Write(ctx context.Context, object, version string, data []byte) (err error) {
	if err := check(object, version); err != nil {
		return err
	}
	if len(data) > MaxShardFile {
		return fmt.Errorf("%w: shard file is %d bytes, limit %d", ErrInvalid, len(data), MaxShardFile)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := d.objectDir(object)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+version+"-*.tmp")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, removeIfExists(tmp.Name()))
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		return errors.Join(err, tmp.Close())
	}
	if err := tmp.Sync(); err != nil {
		return errors.Join(err, tmp.Close())
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, version+shardExt))
}

// Delete removes one shard file; a missing file is not an error.
func (d *Dir) Delete(ctx context.Context, object, version string) error {
	if err := check(object, version); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return removeIfExists(filepath.Join(d.objectDir(object), version+shardExt))
}

// removeIfExists deletes a file and ignores only "does not exist".
func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
