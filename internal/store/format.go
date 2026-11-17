package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/EquinoxWN/erasure-coded-object-store/internal/disk"
	"github.com/EquinoxWN/erasure-coded-object-store/internal/rs"
)

// Shard file layout: magic "ECS1", 4-byte big-endian header length, JSON header, shard bytes.
const (
	magic        = "ECS1"
	maxHeaderLen = 32 << 10
)

// ErrBadShard means a shard file could not be parsed or does not belong where it was found.
var ErrBadShard = errors.New("store: bad shard file")

// Header describes one shard of one object version; every shard of a version carries the same
// header apart from Index. Unknown JSON fields are ignored so later versions can add fields.
type Header struct {
	Key       string   `json:"key"`
	Version   string   `json:"version"`
	Size      int64    `json:"size"`
	K         int      `json:"k"`
	M         int      `json:"m"`
	Index     int      `json:"index"`
	ShardSize int      `json:"shard_size"`
	Disks     []string `json:"disks"`
	SHA256    string   `json:"sha256"`
}

// sameObject reports whether two headers describe the same object version.
func (h Header) sameObject(o Header) bool {
	return h.Key == o.Key && h.Version == o.Version && h.Size == o.Size && h.K == o.K && h.M == o.M &&
		h.ShardSize == o.ShardSize && h.SHA256 == o.SHA256 && slices.Equal(h.Disks, o.Disks)
}

// validate checks a header's internal consistency.
func (h Header) validate() error {
	switch {
	case !disk.ValidVersion(h.Version):
		return fmt.Errorf("%w: version %q", ErrBadShard, h.Version)
	case h.K < 1 || h.K > rs.MaxData || h.M < 1 || h.M > rs.MaxParity:
		return fmt.Errorf("%w: k=%d m=%d", ErrBadShard, h.K, h.M)
	case h.Index < 0 || h.Index >= h.K+h.M:
		return fmt.Errorf("%w: index %d", ErrBadShard, h.Index)
	case len(h.Disks) != h.K+h.M:
		return fmt.Errorf("%w: %d disks for %d shards", ErrBadShard, len(h.Disks), h.K+h.M)
	case h.Size < 0 || h.Size > MaxObjectSize || int64(h.ShardSize) != (h.Size+int64(h.K)-1)/int64(h.K):
		return fmt.Errorf("%w: size %d with shard size %d", ErrBadShard, h.Size, h.ShardSize)
	case len(h.SHA256) != 64:
		return fmt.Errorf("%w: checksum %q", ErrBadShard, h.SHA256)
	}
	return nil
}

// encodeShard builds a shard file from a header and the shard bytes.
func encodeShard(h Header, shard []byte) ([]byte, error) {
	hdr, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(magic)+4+len(hdr)+len(shard))
	out = append(out, magic...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(hdr)))
	out = append(out, hdr...)
	return append(out, shard...), nil
}

// decodeShard parses and validates a shard file.
func decodeShard(data []byte) (Header, []byte, error) {
	var h Header
	if len(data) < len(magic)+4 || string(data[:len(magic)]) != magic {
		return h, nil, fmt.Errorf("%w: missing magic", ErrBadShard)
	}
	n := binary.BigEndian.Uint32(data[len(magic):])
	rest := data[len(magic)+4:]
	if n > maxHeaderLen || int(n) > len(rest) {
		return h, nil, fmt.Errorf("%w: header length %d", ErrBadShard, n)
	}
	dec := json.NewDecoder(bytes.NewReader(rest[:n]))
	if err := dec.Decode(&h); err != nil {
		return h, nil, fmt.Errorf("%w: header: %w", ErrBadShard, err)
	}
	if err := h.validate(); err != nil {
		return h, nil, err
	}
	payload := rest[n:]
	if len(payload) != h.ShardSize {
		return h, nil, fmt.Errorf("%w: payload %d bytes, header says %d", ErrBadShard, len(payload), h.ShardSize)
	}
	return h, payload, nil
}
