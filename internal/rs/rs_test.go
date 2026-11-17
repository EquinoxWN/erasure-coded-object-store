package rs

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/EquinoxWN/erasure-coded-object-store/internal/gf"
)

// subsets calls fn with every subset of 0..n-1 of size f.
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

// randomShards returns encoded shards for size random bytes and the original data.
func randomShards(t *testing.T, c *Codec, size int, seed uint64) ([][]byte, []byte) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, seed+1))
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(rng.Uint32())
	}
	shards := c.Split(data)
	if err := c.Encode(shards); err != nil {
		t.Fatal(err)
	}
	return shards, data
}

func TestNewRejectsBadParameters(t *testing.T) {
	for _, p := range [][2]int{{0, 1}, {1, 0}, {-1, 2}, {MaxData + 1, 1}, {1, MaxParity + 1}} {
		if _, err := New(p[0], p[1]); err == nil {
			t.Errorf("New(%d,%d) accepted", p[0], p[1])
		}
	}
	c, err := New(MaxData, MaxParity)
	if err != nil {
		t.Fatal(err)
	}
	if c.DataShards() != MaxData || c.ParityShards() != MaxParity || c.TotalShards() != MaxData+MaxParity {
		t.Fatal("shard counts wrong")
	}
}

// Any k of n shards rebuild the object: every pattern of up to m erasures, for several codes.
func TestReconstructEveryErasurePattern(t *testing.T) {
	for _, p := range [][2]int{{1, 1}, {2, 1}, {3, 2}, {4, 2}, {6, 3}, {8, 4}, {10, 4}} {
		k, m := p[0], p[1]
		c, err := New(k, m)
		if err != nil {
			t.Fatal(err)
		}
		orig, data := randomShards(t, c, 1000+k, uint64(k*100+m))
		patterns := 0
		for f := 0; f <= m; f++ {
			subsets(k+m, f, func(lost []int) {
				patterns++
				shards := make([][]byte, k+m)
				for i := range orig {
					shards[i] = append([]byte(nil), orig[i]...)
				}
				for _, i := range lost {
					shards[i] = nil
				}
				if err := c.Reconstruct(shards); err != nil {
					t.Fatalf("k=%d m=%d lost %v: %v", k, m, lost, err)
				}
				for i := range shards {
					if !bytes.Equal(shards[i], orig[i]) {
						t.Fatalf("k=%d m=%d lost %v: shard %d differs", k, m, lost, i)
					}
				}
				got, err := c.Join(shards, len(data))
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("k=%d m=%d lost %v: joined data differs (%v)", k, m, lost, err)
				}
			})
		}
		if patterns == 0 {
			t.Fatal("no patterns tried")
		}
	}
}

// The code is MDS: every k x k submatrix of the encoding matrix is invertible.
func TestEveryKRowSubmatrixIsInvertible(t *testing.T) {
	for _, p := range [][2]int{{4, 2}, {6, 3}, {8, 4}, {5, 5}} {
		k, m := p[0], p[1]
		c, err := New(k, m)
		if err != nil {
			t.Fatal(err)
		}
		subsets(k+m, k, func(rows []int) {
			sub := gf.NewMatrix(k, k)
			for r, idx := range rows {
				copy(sub[r], c.Row(idx))
			}
			if _, err := sub.Invert(); err != nil {
				t.Fatalf("k=%d m=%d rows %v: %v", k, m, rows, err)
			}
		})
	}
}

func TestTooManyErasuresFail(t *testing.T) {
	c, _ := New(4, 2)
	orig, _ := randomShards(t, c, 64, 9)
	subsets(6, 3, func(lost []int) {
		shards := append([][]byte(nil), orig...)
		for _, i := range lost {
			shards[i] = nil
		}
		if err := c.Reconstruct(shards); !errors.Is(err, ErrTooFewShards) {
			t.Fatalf("lost %v: err=%v, want ErrTooFewShards", lost, err)
		}
	})
}

func TestParityChangesWhenAnyDataByteChanges(t *testing.T) {
	c, _ := New(4, 2)
	orig, _ := randomShards(t, c, 40, 11)
	for d := 0; d < 4; d++ {
		shards := make([][]byte, 6)
		for i := range orig {
			shards[i] = append([]byte(nil), orig[i]...)
		}
		shards[d][3] ^= 0x5a
		ok, err := c.Verify(shards)
		if err != nil || ok {
			t.Fatalf("data shard %d changed but Verify=%v err=%v", d, ok, err)
		}
		if err := c.Encode(shards); err != nil {
			t.Fatal(err)
		}
		for p := 4; p < 6; p++ {
			if bytes.Equal(shards[p], orig[p]) {
				t.Fatalf("parity shard %d did not change after data shard %d changed", p, d)
			}
		}
	}
	ok, err := c.Verify(orig)
	if err != nil || !ok {
		t.Fatalf("Verify on fresh encoding = %v, %v", ok, err)
	}
}

func TestSplitPadsAndJoinTrims(t *testing.T) {
	c, _ := New(4, 2)
	for _, size := range []int{0, 1, 3, 4, 5, 1023} {
		data := bytes.Repeat([]byte{7}, size)
		shards := c.Split(data)
		if len(shards) != 6 {
			t.Fatalf("size %d: %d shards", size, len(shards))
		}
		want := (size + 3) / 4
		for i, s := range shards {
			if len(s) != want {
				t.Fatalf("size %d: shard %d has %d bytes, want %d", size, i, len(s), want)
			}
		}
		if err := c.Encode(shards); err != nil {
			t.Fatal(err)
		}
		got, err := c.Join(shards, size)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("size %d: Join=%v err=%v", size, len(got), err)
		}
	}
	// Writing to one shard must not spill into the next (capacity is capped).
	shards := c.Split([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	_ = append(shards[0], 99)
	if shards[1][0] != 3 {
		t.Fatal("append to shard 0 overwrote shard 1")
	}
}

func TestBadShardSetsRejected(t *testing.T) {
	c, _ := New(2, 1)
	if err := c.Encode(make([][]byte, 2)); err == nil {
		t.Error("Encode accepted 2 of 3 shards")
	}
	if err := c.Encode([][]byte{{1}, {1, 2}, {0}}); !errors.Is(err, ErrShardSize) {
		t.Errorf("Encode with uneven shards: %v", err)
	}
	if err := c.Encode([][]byte{{1}, nil, {0}}); !errors.Is(err, ErrShardSize) {
		t.Errorf("Encode with nil data shard: %v", err)
	}
	if err := c.Reconstruct([][]byte{{1}, {1, 2}, nil}); !errors.Is(err, ErrShardSize) {
		t.Errorf("Reconstruct with uneven shards: %v", err)
	}
	if err := c.Reconstruct(make([][]byte, 4)); err == nil {
		t.Error("Reconstruct accepted 4 shards for n=3")
	}
	if _, err := c.Verify([][]byte{{1}, {2}}); err == nil {
		t.Error("Verify accepted 2 shards")
	}
	if _, err := c.Verify([][]byte{{1}, {2}, nil}); !errors.Is(err, ErrShardSize) {
		t.Errorf("Verify with a missing shard: %v", err)
	}
	if _, err := c.Join([][]byte{{1}}, 1); !errors.Is(err, ErrTooFewShards) {
		t.Errorf("Join with 1 of 2 data shards: %v", err)
	}
	if _, err := c.Join([][]byte{nil, {1}, {1}}, 1); !errors.Is(err, ErrTooFewShards) {
		t.Errorf("Join with nil data shard: %v", err)
	}
	if _, err := c.Join([][]byte{{1}, {2}, {3}}, 5); err == nil {
		t.Error("Join asked for more bytes than the shards hold")
	}
}

// Randomized: random code, size and erasures always round-trip.
func TestRandomizedRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(21, 22))
	for trial := 0; trial < 300; trial++ {
		k, m := 1+rng.IntN(16), 1+rng.IntN(8)
		c, err := New(k, m)
		if err != nil {
			t.Fatal(err)
		}
		orig, data := randomShards(t, c, rng.IntN(5000), uint64(trial))
		shards := append([][]byte(nil), orig...)
		for _, i := range rng.Perm(k + m)[:rng.IntN(m+1)] {
			shards[i] = nil
		}
		if err := c.Reconstruct(shards); err != nil {
			t.Fatalf("trial %d k=%d m=%d: %v", trial, k, m, err)
		}
		got, err := c.Join(shards, len(data))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("trial %d k=%d m=%d: round trip failed (%v)", trial, k, m, err)
		}
	}
}

func BenchmarkEncode4x2(b *testing.B) { benchEncode(b, 4, 2) }

func BenchmarkEncode10x4(b *testing.B) { benchEncode(b, 10, 4) }

// benchEncode measures parity computation for a 1 MiB object.
func benchEncode(b *testing.B, k, m int) {
	c, _ := New(k, m)
	shards := c.Split(make([]byte, 1<<20))
	b.SetBytes(1 << 20)
	for b.Loop() {
		if err := c.Encode(shards); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReconstruct4x2TwoDataShardsLost(b *testing.B) {
	c, _ := New(4, 2)
	orig := c.Split(make([]byte, 1<<20))
	if err := c.Encode(orig); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(1 << 20)
	for b.Loop() {
		shards := append([][]byte(nil), orig...)
		shards[0], shards[1] = nil, nil
		if err := c.Reconstruct(shards); err != nil {
			b.Fatal(err)
		}
	}
}
