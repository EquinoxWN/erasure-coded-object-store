# ADR 0003: Rendezvous scores with round-robin over failure domains for shard placement

- **Status:** Accepted (M1)

## Context

Each object's k + m shards must land on k + m different disks, and as far as possible on
different failure domains (node, rack, zone), so that one bad disk costs at most one shard and one
bad domain costs as few shards as possible. Placement must be computable by any client from the
key and the disk list alone (no metadata service in M1), and must not depend on the order disks
are listed.

Options considered:

1. A consistent-hash ring with virtual nodes, walked clockwise until n distinct disks are found
   (as in the leaderless-quorum-store repo). Spreading over domains then needs either one ring
   per domain or skipping disks whose domain is already used, which makes the walk long and the
   spread hard to reason about.
2. CRUSH-style hierarchical selection. Handles weights and deep hierarchies, but is far more
   than a two-level (domain, disk) cluster of at most 256 disks needs.
3. Rendezvous (highest random weight) hashing: score every disk with a hash of (key, disk id),
   take the top scores.

## Decision

Score every disk with the first 8 bytes of SHA-256(key, 0, disk id). Group disks by domain and
sort each group by score (ties by id). Order the domains by their best disk's score, then take one
disk from each domain in turn, round after round, until n disks are chosen. Shard i goes to the
i-th disk chosen.

## Consequences

- Never two shards on one disk (tested over 500 keys and five cluster shapes) and at most
  ceil(n / domains) shards per domain when domains are large enough; with uneven domains the
  round-robin still gives the smallest possible maximum (tested with a 1-disk and a 5-disk domain).
- Deterministic and independent of list order, so every client computes the same placement.
- Cost is O(disks) hashes per request, fine at the 256-disk limit; a ring would be O(log n).
- Load spreads evenly: 6,000 keys over 12 disks stay within 15% of the mean per disk (tested).
- With 6 disks in 3 domains and k=4 m=2, each domain holds exactly 2 shards, so the loss of any
  one whole domain is survived; `DomainsTolerated` reports this per object and the CLI prints it.
- Adding or removing a disk changes some placements. M1 has a fixed disk set; a rebalance step
  that moves shards to their new disks is future work and listed as a risk in the RFC.
