# RFC 0001: erasure-coded-object-store design

- **Status:** Accepted for M1 (M2 and M3 sections are plans)
- **Author:** EquinoxWN
- **Created:** 2026-11-17

## Problem

An object store that keeps three full copies of every object survives two lost disks but pays
3x the raw storage. Reed-Solomon erasure coding gives the same two-disk tolerance for 1.5x
(k=4 data + m=2 parity shards), but only if every shard of an object sits on a different disk
(and, ideally, different failure domains), and the read path can rebuild an object from any k
shards. This repository builds that core and measures it.

**Users:** engineers learning or designing durable storage (the "design S3" question), and
anyone choosing k and m for a cluster who wants to see what a setting tolerates.

### Critical-to-quality measures (Define)

| CTQ | Target (M1) | How it is checked |
|---|---|---|
| Durability | Every object reads back byte-for-byte with any m of n disks lost | Exhaustive sweep over every combination of lost disks (tests and `bench`) |
| Correctness of the code | Any k of n shards rebuild the object, for several (k, m) | Exhaustive erasure patterns and every k x k submatrix inverted (tests) |
| Placement | No disk holds two shards of one object; no domain holds more than ceil(n / domains) | Tests over 500 keys and six cluster shapes |
| Storage overhead | (k+m)/k plus under 0.2% header at 1 MiB objects | `bench` measures bytes stored |
| Safety | No wrong bytes ever returned; corrupt data detected (SHA-256) | Corruption tests, mutation check |

## Goals

- M1: objects are split into k data shards plus m parity shards with Reed-Solomon, so any k of
  n = k + m shards rebuild the object.
- M1: shards are placed on n distinct disks, spread round-robin over failure domains, so one bad
  disk never holds two shards of the same object.
- M2: every shard carries a checksum, and reads detect and repair silent corruption; a
  background scrubber re-reads shards to find bit rot.
- M3: a subset of the S3 API (put, get, multipart); disk-removal and bit-flip tests with rebuild
  time compared to 3x replication.

## Non-goals

- A hosted production service, authentication, TLS, or multi-tenant access control.
- Streaming of very large objects in M1: an object is held in memory (limit 64 MiB).
- Rebalancing when disks are added or removed (the disk set is fixed in M1).
- SIMD-speed coding; the codec favours readable code (see ADR 0002).

## Design (M1)

```
client --Put/Get--> store.Store --placement.Place(key, n)--> n distinct disks (round-robin over domains)
                       |  rs.Codec: split + encode k+m shards / reconstruct from any k
                       |  shard file = "ECS1" + header (JSON) + shard bytes
                       +--> disk.Disk: Dir (local directory) | Mem (in-memory, faults) | node.Client (HTTP)
```

- **Codec (`internal/gf`, `internal/rs`).** GF(2^8) with polynomial 0x11d and full
  multiplication tables. The encoding matrix is the identity on top of an m x k Cauchy matrix
  (entry 1/(x_i + y_j), x_i = k+i, y_j = j), so every k x k submatrix is invertible. Decoding
  picks k present shards, inverts their rows (Gauss-Jordan) and recomputes missing data, then
  missing parity. k is 1..64, m is 1..32.
- **Placement (`internal/placement`).** Each disk gets a rendezvous score SHA-256(key, disk id).
  Disks are grouped by domain and sorted by score; domains are ordered by their best disk and
  visited round-robin until n disks are chosen. The result is deterministic, does not depend on
  the order disks are listed, never repeats a disk, and puts at most ceil(n / domains) shards in
  a domain when the domains are large enough. `DomainsTolerated` reports how many whole domains
  can fail with at most m shards lost.
- **Versions and shard files (`internal/store`, `internal/disk`).** A key's object name on disk
  is the hex SHA-256 of the key, so keys never become paths. Each put writes a new version named
  `v<20-digit sequence>-<16 hex random>`; the sequence is one above the highest version any of
  the n disks holds. Each disk stores one shard file per version, written to a temporary file,
  synced and renamed. The header carries key, version, size, k, m, shard index, shard size, the
  disk ids in shard order, and the SHA-256 of the whole object.
- **Put.** Lists versions on all n disks (fails if any is unreachable), encodes, writes all n
  shards in parallel, and succeeds only if all n writes succeed. On failure it deletes the new
  version from all n disks (a timed-out write may still have landed). On success it deletes older
  versions; any it cannot delete are reported (`StaleLeft`) and ignored by reads.
- **Get.** Lists versions on the n disks, takes the newest version with at least k listed
  shards, reads them, keeps the largest group of shards whose headers agree, rebuilds the rest
  and checks the SHA-256. A newer version that could be committed (seen + unreachable >= n) but
  has fewer than k reachable shards makes the read fail instead of serving an older version. If
  a concurrent overwrite deletes the chosen version mid-read, the read starts over (at most 5
  attempts).
- **Repair.** Reads and verifies the object, then writes the missing shards of the current
  version back to reachable disks that lack them (for example a replaced disk).
- **Calls.** Every disk call has a per-attempt timeout and up to `Retries` (0..5) retries with
  doubling backoff capped at 1 s; invalid input, missing files and bad shard files are not
  retried. Put, Delete and Repair of one key are serialised inside a process.
- **Nodes (`internal/node`).** A disk can be served over HTTP (`serve`), and `node.Client`
  implements the same `Disk` interface, so shards can live in separate processes. Locally the
  multi-node tests run in one process (`httptest`); the six-process cluster runs in CI.

## Alternatives considered

| Option | Why not (for M1) |
|---|---|
| 3x replication | Simple and fast to repair, but 3.0x storage for the same two-disk tolerance; kept as the comparison baseline. |
| `klauspost/reedsolomon` as the codec | Much faster (SIMD), but hides the maths this project exists to show and adds a dependency; see ADR 0002. |
| Vandermonde-derived systematic matrix | Works, but needs an extra inversion to make it systematic; a Cauchy matrix is systematic-ready and every square submatrix is invertible by construction. |
| Consistent-hash ring with virtual nodes for placement | Needs a ring per domain to spread shards over domains; rendezvous hashing scores every disk directly and is simpler at up to 256 disks; see ADR 0003. |
| Central metadata service holding placement | A single point of failure to build and replicate; in M1 placement is computed from the key and the disk list, and every shard header repeats the object metadata. |
| Write quorum below n (e.g. k+1) | Faster puts with a disk down, but a committed object would then survive fewer losses until repaired; M1 requires all n so "any m disks" holds from the moment a put returns. |

## Measurement plan

- Measure (M1): `make bench` stores 20 objects of 1 MiB on 6 in-memory disks in 3 domains with
  k=4 m=2, reads every object under every combination of 0..3 lost disks and each lost domain,
  wipes 2 disks and rebuilds them, and prints stored bytes against object bytes. A second run
  uses k=6 m=3 on 9 disks. Go benchmarks measure encode and reconstruct throughput.
- Analyze/Improve: results and defects found in `docs/results/m1.md` and
  `docs/quality/2026-11-01.md`.
- Control: CI runs gofmt, vet, the tests with the race detector, the bench (fails on any lost
  object with at most m disks down), the six-process cluster script and govulncheck.
- M3: rebuild time and network bytes against 3x replication on real processes.

## Milestones

- **M1:** Reed-Solomon split into k + m shards, any k rebuild the object; placement on distinct
  disks spread over failure domains. (Done.)
- **M2:** Per-shard checksums with repair on read; background scrubber.
- **M3:** S3 API subset; disk-removal and bit-flip tests with rebuild time against replication.

## Risks and open questions

- The disk set is fixed: adding a disk changes placements, and objects written before would not
  be found at their new disks until a rebalance tool exists (M2/M3).
- Without per-shard checksums (M2), a corrupt shard is detected by the object SHA-256 but not
  located, so the read fails with `ErrCorrupt` instead of using the other shards.
- Puts need every disk of the placement; with one disk down, new writes to keys placed on it fail
  until it returns.
- Two processes writing the same key at once can pick the same sequence number; the random
  suffix then decides which wins, and the other write is lost silently.
