# erasure-coded-object-store

[![ci](https://github.com/EquinoxWN/erasure-coded-object-store/actions/workflows/ci.yml/badge.svg)](https://github.com/EquinoxWN/erasure-coded-object-store/actions/workflows/ci.yml)
![status](https://img.shields.io/badge/status-M1%20done%2C%20M2%20in%20progress-yellow)

> Lose two disks and lose no data, while paying 1.5x the storage instead of 3x: an S3-style object store using erasure coding.

Part of my **Distributed Systems & Storage** list · Go · core project

## Proof it works

Six in-memory disks in three failure domains, k=4 data + m=2 parity shards, 20 objects of 1 MiB.
Every object reads back byte-for-byte under every one of the 21 ways to lose up to two disks and
with any whole domain lost; with three disks lost the store refuses instead of returning wrong
bytes. Storage is 1.502x of the object bytes, against 3x for three copies:

![durability sweep](docs/proof/durability.jpg)

65 tests pass, and 16 of 16 hand mutations of the core code are caught:

![tests](docs/proof/tests.jpg)

## Architecture

**What M1 runs today:**

```mermaid
flowchart LR
  C["client: put / get / delete / repair"] --> S["store.Store"]
  S -- "placement.Place(key, k+m)" --> P["k+m distinct disks<br/>round-robin over failure domains"]
  S -- "rs.Codec: split + encode<br/>rebuild from any k" --> S
  S -- "timeouts + bounded retries" --> D{{"disk.Disk"}}
  D --> L["Dir: local directory<br/>(temp file + rename)"]
  D --> M["Mem: in-memory,<br/>injected faults"]
  D --> H["node.Client: HTTP"] --> N1[("serve n1")] & N2[("serve n2")] & N3[("serve ...")]
```

**Full roadmap (M1 to M3):**

![architecture mindmap](docs/architecture.png)

## How it works

_Steps 1 and 2 are built and tested (M1); the rest is on the [roadmap](#roadmap)._

1. Objects are split into data shards plus parity shards with Reed-Solomon, so any k of n shards rebuild the object.
2. Shards are placed on different disks or failure domains, so one bad disk never holds two shards of the same object.
3. Every shard carries a checksum, and reads detect and repair silent corruption.
4. A background scrubber re-reads shards to find bit rot before the data is needed.
5. A subset of the S3 API (put, get, multipart) lets standard S3 clients use it.
6. Tests remove disks and flip bits, verifying every object still reads correctly and measuring rebuild time.

In M1 the codec is written in the repository: GF(2^8) arithmetic and a systematic matrix (identity
on top of a Cauchy matrix), so every k of the k + m rows can be inverted
([ADR 0002](docs/adr/0002-own-cauchy-reed-solomon-codec.md)). Each disk gets a rendezvous score
for the key; disks are taken round-robin across failure domains in score order, so an object never
has two shards on one disk and at most ceil(n / domains) in one domain
([ADR 0003](docs/adr/0003-rendezvous-placement-round-robin-domains.md)). A put writes a new
version to all k + m disks (temp file and rename) and fails, rolling back, unless all succeed. A
get takes the newest version with at least k shards, rebuilds the missing ones and checks the
object's SHA-256. `repair` writes back shards missing from replaced disks. Disks can be local
directories or `serve` processes over HTTP.

## Who it helps

- **Who:** Engineers sizing storage clusters (MinIO, Ceph, SeaweedFS-style) and anyone preparing for "design S3" questions on durability and cost.
- **The problem:** "Erasure coding gives replication-level durability for half the space" is easy to say; it is harder to see which k and m survive which disk and rack losses, and why shard placement matters as much as the code.
- **How to use it:** Run `bench` with your k, m, disk and domain counts to get an exhaustive table of which losses every object survives and the real storage overhead; run six `serve` nodes and use `put`, `get`, `repair` while stopping them; read `internal/rs` and `internal/placement` for the maths and the placement rule.

## Tech stack

| Area | In M1 | Planned |
|---|---|---|
| Core | Go standard library, own GF(2^8) Cauchy Reed-Solomon codec, rendezvous placement over failure domains, versioned shard files | S3-compatible API subset with multipart (M3) |
| Integrity | Whole-object SHA-256 checked on every read (detects corruption, fails the read) | Per-shard checksums with repair on read, background scrubber (M2) |
| Network | HTTP disk nodes with timeouts, bounded retries and size limits | Rebalancing when disks are added |
| Test | Exhaustive erasure and disk-loss sweeps, in-process fault injection, HTTP nodes in one process, mutation check | Real disk removal and bit-flip tests with rebuild time against 3x replication (M3) |

| Path | What it is |
|---|---|
| `internal/gf` | GF(2^8) arithmetic and matrix inversion |
| `internal/rs` | Systematic Reed-Solomon codec: split, encode, reconstruct, verify, join |
| `internal/placement` | Shard placement on distinct disks over failure domains; domains tolerated |
| `internal/disk` | `Disk` interface, directory disk (atomic writes), in-memory disk with faults |
| `internal/store` | Put, get, delete, repair; versions, shard file format, retries |
| `internal/node` | HTTP server for one disk and the matching client |
| `cmd/erasure-coded-object-store` | CLI: `serve`, `put`, `get`, `delete`, `repair`, `locate`, `bench` |
| `scripts/` | `cluster.sh` (six processes, CI), `vulncheck.sh` |

## Run it

Needs Go 1.24 or newer (CI uses 1.26.11). No third-party modules.

```bash
go test -count=1 ./...        # 65 tests (CI adds -race)
go run ./cmd/erasure-coded-object-store bench -objects 20 -size 1048576 -seed 1
go run ./cmd/erasure-coded-object-store bench -k 6 -m 3 -disks 9 -domains 3 -objects 10 -size 1048576
```

`make lint`, `make test`, `make bench`, `make cluster` and `make audit` run the same steps as CI.

With local directories as disks (`ID@DOMAIN=DIR`):

```bash
DISKS=d1@rack1=data/d1,d2@rack2=data/d2,d3@rack3=data/d3,d4@rack1=data/d4,d5@rack2=data/d5,d6@rack3=data/d6
go run ./cmd/erasure-coded-object-store put    -disks $DISKS photos/cat.jpg cat.jpg
go run ./cmd/erasure-coded-object-store locate -disks $DISKS photos/cat.jpg
go run ./cmd/erasure-coded-object-store get    -disks $DISKS photos/cat.jpg out.jpg
go run ./cmd/erasure-coded-object-store repair -disks $DISKS photos/cat.jpg
```

With disk nodes over HTTP, one `serve` per disk (`ID@DOMAIN=URL`):

```bash
go run ./cmd/erasure-coded-object-store serve -id n1 -dir data/n1 -addr 127.0.0.1:7201
# ... n2..n6 on 7202..7206
DISKS=n1@rack1=http://127.0.0.1:7201,n2@rack2=http://127.0.0.1:7202,...
go run ./cmd/erasure-coded-object-store get -disks $DISKS photos/cat.jpg -
```

Flags: `-k`, `-m` (default 4 and 2), `-timeout` per disk call (default 5s). Objects are held in
memory, up to 64 MiB. `serve` has no authentication or TLS, so keep it on loopback or a private
network.

## Tests and results

Latest local run (full detail in [docs/results/m1.md](docs/results/m1.md)):

| Test file | Tests | What it proves |
|---|---|---|
| `internal/gf/gf_test.go` | 7 | Field multiplication matches a reference for all 65,536 pairs; field laws; matrix inversion |
| `internal/rs/rs_test.go` | 8 | Every pattern of up to m lost shards rebuilt for 7 codes; every k-row submatrix invertible; m+1 losses refused; 300 randomized round trips |
| `internal/placement/placement_test.go` | 8 | Never two shards on one disk; at most ceil(n/domains) per domain; deterministic and order-independent; even load |
| `internal/disk/disk_test.go` | 8 | Atomic, idempotent shard files; bad names never reach storage; fault injection; concurrent writers |
| `internal/store/store_test.go` | 23 | Every combination of up to m lost disks survived; m+1 refused; whole domain lost survived; rollback, overwrite, corruption detection, repair, retries, timeouts, concurrency |
| `internal/node/node_test.go` | 5 | HTTP round trip; bad requests rejected; failures classified; store over 6 HTTP nodes with nodes stopped |
| `cmd/erasure-coded-object-store/main_test.go` | 6 | End to end with directories and HTTP nodes; 23 bad argument sets rejected; bench; `serve` lifecycle |

| Check | Result |
|---|---|
| Tests | 65 passed, 0 failed (`go test -count=1 ./...`, Go 1.26.11) |
| k=4 m=2, 6 disks / 3 domains, 20 x 1 MiB | all 440 reads OK with 0-2 disks lost (22 combinations); 0 of 400 with 3 lost; 0 wrong bytes |
| k=6 m=3, 9 disks / 3 domains, 10 x 1 MiB | all 1,300 reads OK with 0-3 disks lost; 0 of 1,260 with 4 lost |
| Any one whole domain lost | every object read, in both settings |
| Storage overhead | 1.502x measured (1.500x code + headers) vs 3.000x for 3 copies |
| Codec, 1 MiB, one core | encode 4+2 654 MB/s, 10+4 283 MB/s; reconstruct 4+2 593 MB/s |
| Mutation check | 16 of 16 hand mutations killed |
| govulncheck v1.8.0 | No vulnerabilities found (Go 1.26.11) |

Throughput numbers were measured while other builds shared the machine and vary between runs. The
race detector, the six-process cluster script and all CI steps have not run yet; locally the
multi-node tests run six HTTP nodes inside one process. The 10 quality rules and the mutation
check are recorded in [docs/quality/2026-11-01.md](docs/quality/2026-11-01.md).

## Roadmap

**M1** (≈15 h)
- [x] Write `docs/rfc/0001-design.md`: problem, goals, non-goals, chosen design
- [x] Objects are split into data shards plus parity shards with Reed-Solomon, so any k of n shards rebuild the object.
- [x] Shards are placed on different disks or failure domains, so one bad disk never holds two shards of the same object.

**M2** (≈20 h)
- [ ] Every shard carries a checksum, and reads detect and repair silent corruption.
- [ ] A background scrubber re-reads shards to find bit rot before the data is needed.

**M3** (≈25 h)
- [ ] A subset of the S3 API (put, get, multipart) lets standard S3 clients use it.
- [ ] Tests remove disks and flip bits, verifying every object still reads correctly and measuring rebuild time.
- [ ] Publish the proof below with real numbers

## Proof

What this repo must show before it counts as done:

- Every object survives two-disk loss, with storage overhead and rebuild time compared to 3x replication.

| Result | Value |
|---|---|
| Two-disk loss survived, k=4 m=2 (in-process, M1) | Yes: 20 of 20 objects in all 15 two-disk combinations |
| Storage overhead (M1) | 1.502x vs 3.000x for 3 copies |
| Rebuild time vs 3x replication on real disks and network | Not measured yet (M3) |

## Why it matters

- **Interview angle:** 'Design S3' and durability (how many nines) follow-ups.
- **Upstream I'd like to contribute to:** klauspost/reedsolomon or SeaweedFS: tests and docs.

## Design docs

- [RFC 0001: design](docs/rfc/0001-design.md)
- [ADR 0001: record architecture decisions](docs/adr/0001-record-architecture-decisions.md)
- [ADR 0002: own Cauchy Reed-Solomon codec instead of klauspost/reedsolomon](docs/adr/0002-own-cauchy-reed-solomon-codec.md)
- [ADR 0003: rendezvous scores with round-robin over failure domains](docs/adr/0003-rendezvous-placement-round-robin-domains.md)
- [M1 results](docs/results/m1.md)

## Scope

This is a learning and portfolio system, not a hosted production service. Everything runs
locally. Known limits in M1: objects are held in memory (64 MiB limit); a corrupt shard is
detected by the object checksum but not located or repaired (M2), so the read fails; the disk set
is fixed (no rebalancing); a put needs all k + m disks of its placement; two processes writing the
same key at the same moment can lose one write silently; nodes do not authenticate.

## Security and contributing

- Every GitHub Action is pinned to a commit SHA; workflows run read-only, without persisted credentials.
- Dependabot update pull requests are paused (`open-pull-requests-limit: 0`) so the repository keeps a single `main` branch; dependencies are updated by hand and checked by the CI audit.
- `gofmt`, `go vet` and the race detector on every push; `govulncheck` (`make audit`) in CI.
- Report vulnerabilities privately: see [SECURITY.md](SECURITY.md). To contribute, see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT, see [LICENSE](LICENSE).
