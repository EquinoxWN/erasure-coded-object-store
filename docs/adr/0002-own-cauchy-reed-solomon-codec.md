# ADR 0002: Own Cauchy Reed-Solomon codec instead of klauspost/reedsolomon

- **Status:** Accepted (M1)

## Context

The store needs a systematic MDS erasure code: k data shards stored as-is, m parity shards, and
any k of the k + m shards enough to rebuild. `klauspost/reedsolomon` is the standard Go library
(used by MinIO and others) and is many times faster thanks to SIMD (AVX2/GFNI). This project is
a learning and portfolio system whose point is to show how the code works and prove it.

## Decision

Write the codec in the repository: GF(2^8) arithmetic with polynomial 0x11d and a full 64 KiB
multiplication table (`internal/gf`), and an encoding matrix made of the k x k identity on top of
an m x k Cauchy matrix with entries 1/(x_i + y_j), x_i = k + i, y_j = j (`internal/rs`). Every
square submatrix of a Cauchy matrix is invertible, so every k rows of the stacked matrix are
invertible and the code is MDS without a separate systematic transform. Decoding inverts the k x k
submatrix of the present rows with Gauss-Jordan elimination.

## Consequences

- No third-party module: `go.mod` has no requirements and govulncheck scans only the standard
  library.
- Correctness is proven by tests rather than borrowed: every field product is checked against a
  shift-and-add reference for all 65,536 pairs, every k-row submatrix is inverted for four codes,
  and every erasure pattern of up to m shards is rebuilt for seven codes.
- Throughput is lower. Measured on the author's machine with other builds running (1 MiB object,
  `go test -bench . ./internal/rs`): encode 4+2 at about 650 MB/s, 10+4 at about 280 MB/s,
  reconstruct 4+2 with two data shards lost at about 590 MB/s. klauspost/reedsolomon publishes
  multi-GB/s figures for similar settings; that comparison has not been run here.
- Parity bytes are not compatible with other implementations' matrices, so shards cannot be read by
  another Reed-Solomon tool. The shard header records k and m; a change of matrix later would need
  a format version field.
- If speed becomes a goal (M3 rebuild-time comparison), the `rs.Codec` API (Split, Encode,
  Reconstruct, Join) is small enough to back with the library behind the same interface.
