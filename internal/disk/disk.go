// Package disk stores shard files: one directory-backed implementation, one in-memory
// implementation with fault injection, and the names both accept.
package disk

import (
	"context"
	"errors"
	"fmt"
	"regexp"
)

// MaxShardFile bounds one shard file (header plus payload) so a bad peer cannot exhaust memory.
const MaxShardFile = 64<<20 + 64<<10

var (
	// ErrNotFound means the disk has no shard file with that name.
	ErrNotFound = errors.New("disk: shard not found")
	// ErrOffline means the disk cannot be reached (used for injected and real outages).
	ErrOffline = errors.New("disk: offline")
	// ErrInvalid means a name or payload was rejected; retrying will not help.
	ErrInvalid = errors.New("disk: invalid request")
)

var (
	objectPattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	versionPattern = regexp.MustCompile(`^v[0-9]{20}-[0-9a-f]{16}$`)
)

// Disk holds at most one shard per object version.
type Disk interface {
	// ID returns the disk's identifier.
	ID() string
	// Versions lists the shard versions stored for an object.
	Versions(ctx context.Context, object string) ([]string, error)
	// Read returns one shard file.
	Read(ctx context.Context, object, version string) ([]byte, error)
	// Write stores one shard file atomically; writing the same bytes again is a no-op.
	Write(ctx context.Context, object, version string, data []byte) error
	// Delete removes one shard file; deleting a missing file succeeds.
	Delete(ctx context.Context, object, version string) error
}

// ValidObject reports whether s is an object name (hex SHA-256 of the key).
func ValidObject(s string) bool { return objectPattern.MatchString(s) }

// ValidVersion reports whether s is a version name ("v" + 20-digit sequence + "-" + 16 hex).
func ValidVersion(s string) bool { return versionPattern.MatchString(s) }

// FormatVersion builds a version name from a sequence number and a random 64-bit id.
func FormatVersion(seq uint64, id uint64) string { return fmt.Sprintf("v%020d-%016x", seq, id) }

// checkObject validates an object name.
func checkObject(object string) error {
	if !ValidObject(object) {
		return fmt.Errorf("%w: object name %q", ErrInvalid, object)
	}
	return nil
}

// check validates an object name and a version name.
func check(object, version string) error {
	if err := checkObject(object); err != nil {
		return err
	}
	if !ValidVersion(version) {
		return fmt.Errorf("%w: version name %q", ErrInvalid, version)
	}
	return nil
}
