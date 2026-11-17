// Package rs is a systematic Reed-Solomon erasure code over GF(2^8).
//
// An object is split into k data shards and m parity shards are computed from them. The
// encoding matrix is the k x k identity stacked on an m x k Cauchy matrix, so every k x k
// submatrix is invertible and any k of the n = k + m shards rebuild the object.
package rs

import (
	"errors"
	"fmt"

	"github.com/EquinoxWN/erasure-coded-object-store/internal/gf"
)

// Limits on the code parameters (k + m must stay within the 256 field elements).
const (
	MaxData   = 64
	MaxParity = 32
)

var (
	// ErrTooFewShards means fewer than k shards are present, so the object cannot be rebuilt.
	ErrTooFewShards = errors.New("rs: too few shards to reconstruct")
	// ErrShardSize means the shards present do not all have the same length.
	ErrShardSize = errors.New("rs: shards differ in size")
)

// Codec encodes and reconstructs shards for fixed k and m; it is safe for concurrent use.
type Codec struct {
	k, m   int
	matrix gf.Matrix // n x k encoding matrix: identity on top, Cauchy rows below
}

// New returns a codec with k data shards and m parity shards.
func New(k, m int) (*Codec, error) {
	if k < 1 || k > MaxData {
		return nil, fmt.Errorf("rs: data shards k=%d out of range 1..%d", k, MaxData)
	}
	if m < 1 || m > MaxParity {
		return nil, fmt.Errorf("rs: parity shards m=%d out of range 1..%d", m, MaxParity)
	}
	matrix := gf.NewMatrix(k+m, k)
	for i := 0; i < k; i++ {
		matrix[i][i] = 1
	}
	// Cauchy entry 1/(x_i + y_j) with x_i = k+i and y_j = j; all x and y are distinct.
	for i := 0; i < m; i++ {
		for j := 0; j < k; j++ {
			matrix[k+i][j] = gf.Inv(byte(k+i) ^ byte(j))
		}
	}
	return &Codec{k: k, m: m, matrix: matrix}, nil
}

// DataShards returns k.
func (c *Codec) DataShards() int { return c.k }

// ParityShards returns m.
func (c *Codec) ParityShards() int { return c.m }

// TotalShards returns k + m.
func (c *Codec) TotalShards() int { return c.k + c.m }

// Row returns a copy of encoding-matrix row i (for tests and inspection).
func (c *Codec) Row(i int) []byte { return append([]byte(nil), c.matrix[i]...) }

// ShardSize returns the shard length used for an object of size bytes.
func (c *Codec) ShardSize(size int) int { return (size + c.k - 1) / c.k }

// Split copies data into k equal data shards (zero padded) plus m empty parity shards.
func (c *Codec) Split(data []byte) [][]byte {
	size := c.ShardSize(len(data))
	buf := make([]byte, size*(c.k+c.m))
	copy(buf, data)
	shards := make([][]byte, c.k+c.m)
	for i := range shards {
		shards[i] = buf[i*size : (i+1)*size : (i+1)*size]
	}
	return shards
}

// Join concatenates the data shards and trims the result to size bytes.
func (c *Codec) Join(shards [][]byte, size int) ([]byte, error) {
	if len(shards) < c.k {
		return nil, ErrTooFewShards
	}
	out := make([]byte, 0, size)
	for i := 0; i < c.k && len(out) < size; i++ {
		if shards[i] == nil {
			return nil, fmt.Errorf("rs: data shard %d missing: %w", i, ErrTooFewShards)
		}
		out = append(out, shards[i]...)
	}
	if len(out) < size {
		return nil, fmt.Errorf("rs: data shards hold %d bytes, want %d", len(out), size)
	}
	return out[:size], nil
}

// Encode computes the m parity shards from the k data shards, in place.
func (c *Codec) Encode(shards [][]byte) error {
	if len(shards) != c.k+c.m {
		return fmt.Errorf("rs: got %d shards, want %d", len(shards), c.k+c.m)
	}
	size := len(shards[0])
	for i, s := range shards {
		if s == nil || len(s) != size {
			return fmt.Errorf("rs: shard %d: %w", i, ErrShardSize)
		}
	}
	for i := 0; i < c.m; i++ {
		c.encodeRow(c.matrix[c.k+i], shards[:c.k], shards[c.k+i])
	}
	return nil
}

// encodeRow sets out to the dot product of coefficient row and the given input shards.
func (c *Codec) encodeRow(row []byte, in [][]byte, out []byte) {
	clear(out)
	for j, coef := range row {
		gf.MulAdd(coef, in[j], out)
	}
}

// Verify reports whether the parity shards match the data shards; all shards must be present.
func (c *Codec) Verify(shards [][]byte) (bool, error) {
	if len(shards) != c.k+c.m {
		return false, fmt.Errorf("rs: got %d shards, want %d", len(shards), c.k+c.m)
	}
	size := len(shards[0])
	for i, s := range shards {
		if s == nil || len(s) != size {
			return false, fmt.Errorf("rs: shard %d: %w", i, ErrShardSize)
		}
	}
	tmp := make([]byte, size)
	for i := 0; i < c.m; i++ {
		c.encodeRow(c.matrix[c.k+i], shards[:c.k], tmp)
		for b := range tmp {
			if tmp[b] != shards[c.k+i][b] {
				return false, nil
			}
		}
	}
	return true, nil
}

// Reconstruct fills every nil shard from any k present shards, in place.
func (c *Codec) Reconstruct(shards [][]byte) error {
	if len(shards) != c.k+c.m {
		return fmt.Errorf("rs: got %d shards, want %d", len(shards), c.k+c.m)
	}
	size := -1
	present := make([]int, 0, c.k)
	for i, s := range shards {
		if s == nil {
			continue
		}
		if size < 0 {
			size = len(s)
		} else if len(s) != size {
			return fmt.Errorf("rs: shard %d: %w", i, ErrShardSize)
		}
		if len(present) < c.k {
			present = append(present, i)
		}
	}
	if len(present) < c.k {
		return fmt.Errorf("%w: have %d, need %d", ErrTooFewShards, len(present), c.k)
	}
	missingData := false
	for i := 0; i < c.k; i++ {
		if shards[i] == nil {
			missingData = true
			break
		}
	}
	if missingData {
		sub := gf.NewMatrix(c.k, c.k)
		in := make([][]byte, c.k)
		for r, idx := range present {
			copy(sub[r], c.matrix[idx])
			in[r] = shards[idx]
		}
		decode, err := sub.Invert()
		if err != nil {
			return fmt.Errorf("rs: decode matrix: %w", err) // cannot happen for a Cauchy code
		}
		for i := 0; i < c.k; i++ {
			if shards[i] == nil {
				shards[i] = make([]byte, size)
				c.encodeRow(decode[i], in, shards[i])
			}
		}
	}
	for i := 0; i < c.m; i++ {
		if shards[c.k+i] == nil {
			shards[c.k+i] = make([]byte, size)
			c.encodeRow(c.matrix[c.k+i], shards[:c.k], shards[c.k+i])
		}
	}
	return nil
}
