// Package gf implements arithmetic in GF(2^8) and matrix inversion over it.
//
// The field uses the polynomial x^8 + x^4 + x^3 + x^2 + 1 (0x11d) with generator 2, the same
// field as most Reed-Solomon storage codes. Addition and subtraction are XOR.
package gf

import "errors"

// Poly is the reducing polynomial of the field.
const Poly = 0x11d

var (
	expTable [512]byte
	logTable [256]byte
	mulTable [256][256]byte
)

// init builds the log, exp and full multiplication tables.
func init() {
	x := 1
	for i := 0; i < 255; i++ {
		expTable[i] = byte(x)
		logTable[x] = byte(i)
		x <<= 1
		if x&0x100 != 0 {
			x ^= Poly
		}
	}
	for i := 255; i < len(expTable); i++ {
		expTable[i] = expTable[i-255]
	}
	for a := 1; a < 256; a++ {
		for b := 1; b < 256; b++ {
			mulTable[a][b] = expTable[int(logTable[a])+int(logTable[b])]
		}
	}
}

// Add returns a + b (XOR).
func Add(a, b byte) byte { return a ^ b }

// Mul returns a * b.
func Mul(a, b byte) byte { return mulTable[a][b] }

// Exp returns the generator raised to n (n >= 0).
func Exp(n int) byte { return expTable[n%255] }

// Inv returns the multiplicative inverse of a; a must not be zero.
func Inv(a byte) byte {
	if a == 0 {
		panic("gf: inverse of zero")
	}
	return expTable[255-int(logTable[a])]
}

// Div returns a / b; b must not be zero.
func Div(a, b byte) byte {
	if b == 0 {
		panic("gf: division by zero")
	}
	if a == 0 {
		return 0
	}
	return expTable[int(logTable[a])+255-int(logTable[b])]
}

// MulAdd sets out[i] ^= c * in[i] for every i; in and out must have the same length.
func MulAdd(c byte, in, out []byte) {
	if len(in) != len(out) {
		panic("gf: MulAdd length mismatch")
	}
	switch c {
	case 0:
		return
	case 1:
		for i, v := range in {
			out[i] ^= v
		}
		return
	}
	row := &mulTable[c]
	for i, v := range in {
		out[i] ^= row[v]
	}
}

// ErrSingular is returned when a matrix has no inverse.
var ErrSingular = errors.New("gf: matrix is singular")

// Matrix is a dense row-major matrix over GF(2^8).
type Matrix [][]byte

// NewMatrix returns a rows x cols zero matrix.
func NewMatrix(rows, cols int) Matrix {
	m := make(Matrix, rows)
	for i := range m {
		m[i] = make([]byte, cols)
	}
	return m
}

// Identity returns the n x n identity matrix.
func Identity(n int) Matrix {
	m := NewMatrix(n, n)
	for i := range m {
		m[i][i] = 1
	}
	return m
}

// Mul returns m * o; the column count of m must equal the row count of o.
func (m Matrix) Mul(o Matrix) Matrix {
	if len(m) == 0 || len(m[0]) != len(o) {
		panic("gf: matrix size mismatch")
	}
	out := NewMatrix(len(m), len(o[0]))
	for i := range m {
		for k, a := range m[i] {
			if a != 0 {
				MulAdd(a, o[k], out[i])
			}
		}
	}
	return out
}

// Invert returns the inverse of a square matrix by Gauss-Jordan elimination.
func (m Matrix) Invert() (Matrix, error) {
	n := len(m)
	for _, row := range m {
		if len(row) != n {
			return nil, errors.New("gf: matrix is not square")
		}
	}
	work := NewMatrix(n, 2*n)
	for i := range m {
		copy(work[i], m[i])
		work[i][n+i] = 1
	}
	for col := 0; col < n; col++ {
		pivot := -1
		for r := col; r < n; r++ {
			if work[r][col] != 0 {
				pivot = r
				break
			}
		}
		if pivot < 0 {
			return nil, ErrSingular
		}
		work[col], work[pivot] = work[pivot], work[col]
		scale := Inv(work[col][col])
		for j := range work[col] {
			work[col][j] = Mul(work[col][j], scale)
		}
		for r := 0; r < n; r++ {
			if r != col && work[r][col] != 0 {
				MulAdd(work[r][col], work[col], work[r])
			}
		}
	}
	out := NewMatrix(n, n)
	for i := range out {
		copy(out[i], work[i][n:])
	}
	return out, nil
}
