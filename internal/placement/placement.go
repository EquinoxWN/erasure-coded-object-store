// Package placement chooses which disks hold the n shards of an object.
//
// Every disk gets a rendezvous (highest-random-weight) score for the object key. Disks are
// grouped by failure domain and taken round-robin across domains in score order, so no disk
// ever holds two shards of one object and shards spread as evenly as possible over domains.
package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"sort"
)

// MaxDisks bounds the cluster size so placement stays cheap per request.
const MaxDisks = 256

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ErrNotEnoughDisks means the cluster has fewer disks than shards per object.
var ErrNotEnoughDisks = errors.New("placement: not enough disks")

// Disk names one disk and the failure domain (node, rack or zone) it belongs to.
type Disk struct {
	ID     string
	Domain string
}

// ValidateID checks a disk or domain identifier.
func ValidateID(id string) error {
	if !idPattern.MatchString(id) {
		return fmt.Errorf("placement: id %q must be 1-64 characters of A-Z a-z 0-9 . _ - (not starting with . _ -)", id)
	}
	return nil
}

// Placer maps object keys to disks; it is immutable and safe for concurrent use.
type Placer struct {
	disks   []Disk
	domains int
}

// New validates the disk list and returns a placer.
func New(disks []Disk) (*Placer, error) {
	if len(disks) == 0 {
		return nil, errors.New("placement: no disks")
	}
	if len(disks) > MaxDisks {
		return nil, fmt.Errorf("placement: %d disks, at most %d", len(disks), MaxDisks)
	}
	seen := map[string]bool{}
	domains := map[string]bool{}
	for _, d := range disks {
		if err := ValidateID(d.ID); err != nil {
			return nil, err
		}
		if err := ValidateID(d.Domain); err != nil {
			return nil, fmt.Errorf("disk %s domain: %w", d.ID, err)
		}
		if seen[d.ID] {
			return nil, fmt.Errorf("placement: disk %q listed twice", d.ID)
		}
		seen[d.ID] = true
		domains[d.Domain] = true
	}
	return &Placer{disks: append([]Disk(nil), disks...), domains: len(domains)}, nil
}

// Disks returns a copy of the disk list.
func (p *Placer) Disks() []Disk { return append([]Disk(nil), p.disks...) }

// Domains returns the number of distinct failure domains.
func (p *Placer) Domains() int { return p.domains }

// score is the rendezvous weight of a disk for a key.
func score(key, diskID string) uint64 {
	h := sha256.New()
	h.Write([]byte(key))
	h.Write([]byte{0})
	h.Write([]byte(diskID))
	return binary.BigEndian.Uint64(h.Sum(nil)[:8])
}

type scored struct {
	disk  Disk
	score uint64
}

// less orders by score, then id, so ties are broken the same way on every node.
func less(a, b scored) bool {
	if a.score != b.score {
		return a.score > b.score
	}
	return a.disk.ID < b.disk.ID
}

// Place returns n distinct disks for key; shard i goes to the i-th disk.
func (p *Placer) Place(key string, n int) ([]Disk, error) {
	if n < 1 {
		return nil, fmt.Errorf("placement: n=%d must be at least 1", n)
	}
	if n > len(p.disks) {
		return nil, fmt.Errorf("%w: %d shards need %d distinct disks, cluster has %d", ErrNotEnoughDisks, n, n, len(p.disks))
	}
	groups := map[string][]scored{}
	for _, d := range p.disks {
		groups[d.Domain] = append(groups[d.Domain], scored{d, score(key, d.ID)})
	}
	order := make([][]scored, 0, len(groups))
	for _, g := range groups {
		sort.Slice(g, func(i, j int) bool { return less(g[i], g[j]) })
		order = append(order, g)
	}
	// Domains are visited in the order of their best disk, so the result is deterministic.
	sort.Slice(order, func(i, j int) bool { return less(order[i][0], order[j][0]) })
	out := make([]Disk, 0, n)
	for round := 0; len(out) < n; round++ {
		for _, g := range order {
			if round < len(g) && len(out) < n {
				out = append(out, g[round].disk)
			}
		}
	}
	return out, nil
}

// PerDomain counts how many of the placed disks fall in each domain.
func PerDomain(disks []Disk) map[string]int {
	out := map[string]int{}
	for _, d := range disks {
		out[d.Domain]++
	}
	return out
}

// DomainsTolerated returns how many whole domains can fail, in the worst case, while at most
// m shards are lost.
func DomainsTolerated(disks []Disk, m int) int {
	loads := make([]int, 0)
	for _, c := range PerDomain(disks) {
		loads = append(loads, c)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(loads)))
	lost, tolerated := 0, 0
	for _, l := range loads {
		if lost+l > m {
			break
		}
		lost += l
		tolerated++
	}
	return tolerated
}
