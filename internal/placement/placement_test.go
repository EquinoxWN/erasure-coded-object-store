package placement

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// cluster returns disks d01.. dealt round-robin over the given number of domains.
func cluster(disks, domains int) []Disk {
	out := make([]Disk, disks)
	for i := range out {
		out[i] = Disk{ID: fmt.Sprintf("d%02d", i+1), Domain: fmt.Sprintf("z%d", i%domains+1)}
	}
	return out
}

func TestNewRejectsBadClusters(t *testing.T) {
	cases := map[string][]Disk{
		"empty":         nil,
		"duplicate id":  {{"a", "z"}, {"a", "y"}},
		"empty id":      {{"", "z"}},
		"empty domain":  {{"a", ""}},
		"slash in id":   {{"a/b", "z"}},
		"dot start":     {{"..", "z"}},
		"space":         {{"a b", "z"}},
		"too long":      {{string(make([]byte, 65)), "z"}},
		"too many disk": cluster(MaxDisks+1, 4),
	}
	for name, disks := range cases {
		if _, err := New(disks); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := New(cluster(MaxDisks, 4)); err != nil {
		t.Fatalf("MaxDisks rejected: %v", err)
	}
}

// One bad disk never holds two shards of the same object: every placement is n distinct disks.
func TestPlaceReturnsDistinctDisks(t *testing.T) {
	for _, c := range []struct{ disks, domains, n int }{{6, 3, 6}, {9, 3, 6}, {12, 4, 9}, {8, 1, 8}, {16, 16, 14}} {
		p, err := New(cluster(c.disks, c.domains))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 500; i++ {
			got, err := p.Place(fmt.Sprintf("key-%d", i), c.n)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != c.n {
				t.Fatalf("%+v: %d disks, want %d", c, len(got), c.n)
			}
			seen := map[string]bool{}
			for _, d := range got {
				if seen[d.ID] {
					t.Fatalf("%+v key-%d: disk %s used twice in %v", c, i, d.ID, got)
				}
				seen[d.ID] = true
			}
		}
	}
}

// Shards spread over failure domains: no domain holds more than ceil(n / domains) shards.
func TestPlaceSpreadsOverDomains(t *testing.T) {
	for _, c := range []struct{ disks, domains, n, wantMax int }{
		{6, 3, 6, 2}, {9, 3, 6, 2}, {12, 4, 6, 2}, {12, 6, 6, 1}, {12, 3, 9, 3}, {8, 8, 5, 1},
	} {
		p, _ := New(cluster(c.disks, c.domains))
		for i := 0; i < 500; i++ {
			got, _ := p.Place(fmt.Sprintf("obj/%d", i), c.n)
			for dom, cnt := range PerDomain(got) {
				if cnt > c.wantMax {
					t.Fatalf("%+v obj/%d: domain %s holds %d shards, max %d", c, i, dom, cnt, c.wantMax)
				}
			}
		}
	}
}

func TestPlaceUsesSmallDomainsFully(t *testing.T) {
	// Domain a has one disk, b has five: with n=4 the best spread is 1 + 3.
	disks := []Disk{{"a1", "a"}, {"b1", "b"}, {"b2", "b"}, {"b3", "b"}, {"b4", "b"}, {"b5", "b"}}
	p, _ := New(disks)
	for i := 0; i < 200; i++ {
		got, _ := p.Place(fmt.Sprint(i), 4)
		if c := PerDomain(got); c["a"] != 1 || c["b"] != 3 {
			t.Fatalf("key %d: per-domain %v, want a=1 b=3", i, c)
		}
	}
}

func TestPlaceIsDeterministicAndOrderIndependent(t *testing.T) {
	disks := cluster(12, 4)
	p1, _ := New(disks)
	shuffled := append([]Disk(nil), disks...)
	rand.New(rand.NewPCG(1, 1)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	p2, _ := New(shuffled)
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("k%d", i)
		a, _ := p1.Place(key, 6)
		b, _ := p2.Place(key, 6)
		c, _ := p1.Place(key, 6)
		if fmt.Sprint(a) != fmt.Sprint(b) || fmt.Sprint(a) != fmt.Sprint(c) {
			t.Fatalf("%s: placements differ: %v / %v / %v", key, a, b, c)
		}
	}
}

func TestPlaceRejectsBadN(t *testing.T) {
	p, _ := New(cluster(6, 3))
	for _, n := range []int{0, -1, 7} {
		if _, err := p.Place("k", n); err == nil {
			t.Errorf("n=%d accepted", n)
		}
	}
}

// Over many keys every disk gets a similar share of shards.
func TestPlaceSpreadsLoadOverDisks(t *testing.T) {
	p, _ := New(cluster(12, 4))
	counts := map[string]int{}
	const keys, n = 6000, 6
	for i := 0; i < keys; i++ {
		got, _ := p.Place(fmt.Sprintf("load-%d", i), n)
		for _, d := range got {
			counts[d.ID]++
		}
	}
	mean := float64(keys*n) / 12
	for id, c := range counts {
		if float64(c) < 0.85*mean || float64(c) > 1.15*mean {
			t.Errorf("disk %s holds %d shards, mean %.0f", id, c, mean)
		}
	}
	if len(counts) != 12 {
		t.Fatalf("only %d of 12 disks used", len(counts))
	}
}

func TestDomainsTolerated(t *testing.T) {
	d := func(domains ...string) []Disk {
		out := make([]Disk, len(domains))
		for i, z := range domains {
			out[i] = Disk{ID: fmt.Sprint(i), Domain: z}
		}
		return out
	}
	cases := []struct {
		disks []Disk
		m     int
		want  int
	}{
		{d("a", "a", "b", "b", "c", "c"), 2, 1},
		{d("a", "a", "b", "b", "c", "c"), 1, 0},
		{d("a", "a", "b", "b", "c", "c"), 4, 2},
		{d("a", "b", "c", "d", "e", "f"), 2, 2},
		{d("a", "a", "a", "b", "c", "d"), 2, 0},
		{d("a", "a", "a", "b", "c", "d"), 3, 1},
	}
	for _, c := range cases {
		if got := DomainsTolerated(c.disks, c.m); got != c.want {
			t.Errorf("%v m=%d: %d, want %d", PerDomain(c.disks), c.m, got, c.want)
		}
	}
}
