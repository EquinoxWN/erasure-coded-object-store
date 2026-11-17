package gf

import (
	"errors"
	"math/rand/v2"
	"testing"
)

// slowMul multiplies by shift-and-add with reduction, independent of the tables.
func slowMul(a, b byte) byte {
	var p int
	x, y := int(a), int(b)
	for y > 0 {
		if y&1 != 0 {
			p ^= x
		}
		x <<= 1
		if x&0x100 != 0 {
			x ^= Poly
		}
		y >>= 1
	}
	return byte(p)
}

func TestMulMatchesShiftAndAddForAllPairs(t *testing.T) {
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			if got, want := Mul(byte(a), byte(b)), slowMul(byte(a), byte(b)); got != want {
				t.Fatalf("Mul(%d,%d)=%d, want %d", a, b, got, want)
			}
		}
	}
}

func TestFieldAxioms(t *testing.T) {
	for a := 0; a < 256; a++ {
		if Mul(byte(a), 1) != byte(a) || Mul(byte(a), 0) != 0 || Add(byte(a), byte(a)) != 0 {
			t.Fatalf("identity or zero law fails for %d", a)
		}
		if a == 0 {
			continue
		}
		if Mul(byte(a), Inv(byte(a))) != 1 {
			t.Fatalf("a * inv(a) != 1 for %d", a)
		}
		for b := 1; b < 256; b++ {
			if Mul(Div(byte(a), byte(b)), byte(b)) != byte(a) {
				t.Fatalf("(a/b)*b != a for %d,%d", a, b)
			}
		}
	}
	if Div(0, 7) != 0 {
		t.Fatal("0/b must be 0")
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 20000; i++ {
		a, b, c := byte(rng.Uint32()), byte(rng.Uint32()), byte(rng.Uint32())
		if Mul(a, b^c) != Mul(a, b)^Mul(a, c) {
			t.Fatalf("distributive law fails for %d,%d,%d", a, b, c)
		}
		if Mul(Mul(a, b), c) != Mul(a, Mul(b, c)) || Mul(a, b) != Mul(b, a) {
			t.Fatalf("associative or commutative law fails for %d,%d,%d", a, b, c)
		}
	}
}

func TestGeneratorHasOrder255(t *testing.T) {
	seen := map[byte]bool{}
	for i := 0; i < 255; i++ {
		seen[Exp(i)] = true
	}
	if len(seen) != 255 || seen[0] {
		t.Fatalf("generator reaches %d distinct non-zero values, want 255", len(seen))
	}
	if Exp(255) != 1 {
		t.Fatal("generator^255 must be 1")
	}
}

func TestZeroInverseAndDivisionPanic(t *testing.T) {
	for name, f := range map[string]func(){"Inv(0)": func() { Inv(0) }, "Div(1,0)": func() { Div(1, 0) }} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic", name)
				}
			}()
			f()
		}()
	}
}

func TestMulAddMatchesMul(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	in := make([]byte, 300)
	for i := range in {
		in[i] = byte(rng.Uint32())
	}
	for c := 0; c < 256; c++ {
		out := make([]byte, len(in))
		for i := range out {
			out[i] = byte(i)
		}
		MulAdd(byte(c), in, out)
		for i := range out {
			if want := byte(i) ^ Mul(byte(c), in[i]); out[i] != want {
				t.Fatalf("MulAdd c=%d i=%d: %d, want %d", c, i, out[i], want)
			}
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("MulAdd with different lengths did not panic")
		}
	}()
	MulAdd(2, []byte{1}, []byte{1, 2})
}

func TestInvertRandomMatrices(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	inverted := 0
	for trial := 0; trial < 300; trial++ {
		n := 1 + rng.IntN(12)
		m := NewMatrix(n, n)
		for i := range m {
			for j := range m[i] {
				m[i][j] = byte(rng.Uint32())
			}
		}
		inv, err := m.Invert()
		if errors.Is(err, ErrSingular) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		inverted++
		for _, p := range []Matrix{m.Mul(inv), inv.Mul(m)} {
			for i := range p {
				for j := range p[i] {
					want := byte(0)
					if i == j {
						want = 1
					}
					if p[i][j] != want {
						t.Fatalf("trial %d: product is not the identity at %d,%d", trial, i, j)
					}
				}
			}
		}
	}
	if inverted < 250 {
		t.Fatalf("only %d of 300 random matrices invertible; expected nearly all", inverted)
	}
}

func TestInvertRejectsSingularAndNonSquare(t *testing.T) {
	singular := Matrix{{1, 2, 3}, {2, 4, 6}, {0, 0, 1}} // row 2 = 2 * row 1 in GF(2^8)
	singular[1] = []byte{Mul(2, 1), Mul(2, 2), Mul(2, 3)}
	if _, err := singular.Invert(); !errors.Is(err, ErrSingular) {
		t.Fatalf("singular matrix: err=%v, want ErrSingular", err)
	}
	if _, err := (Matrix{{1, 2}}).Invert(); err == nil {
		t.Fatal("non-square matrix accepted")
	}
	id, err := Identity(4).Invert()
	if err != nil {
		t.Fatal(err)
	}
	for i := range id {
		for j := range id[i] {
			if (i == j) != (id[i][j] == 1) || (i != j && id[i][j] != 0) {
				t.Fatal("inverse of identity is not identity")
			}
		}
	}
}
