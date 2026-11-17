package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/EquinoxWN/erasure-coded-object-store/internal/disk"
	"github.com/EquinoxWN/erasure-coded-object-store/internal/store"
)

var (
	obj = strings.Repeat("cd", 32)
	ver = disk.FormatVersion(3, 0x10)
)

// server starts a node over a directory disk and returns a client for it.
func server(t *testing.T, id string) (*Client, *httptest.Server) {
	t.Helper()
	d, err := disk.NewDir(id, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(Handler(d))
	t.Cleanup(srv.Close)
	c, err := NewClient(id, srv.URL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv
}

func TestClientRoundTrip(t *testing.T) {
	ctx := context.Background()
	c, _ := server(t, "n1")
	if id, err := c.Health(ctx); err != nil || id != "n1" {
		t.Fatalf("health = %q %v", id, err)
	}
	if vs, err := c.Versions(ctx, obj); err != nil || len(vs) != 0 {
		t.Fatalf("empty versions = %v %v", vs, err)
	}
	if _, err := c.Read(ctx, obj, ver); !errors.Is(err, disk.ErrNotFound) {
		t.Fatalf("read missing: %v", err)
	}
	data := bytes.Repeat([]byte{0, 1, 2, 255}, 1000)
	if err := c.Write(ctx, obj, ver, data); err != nil {
		t.Fatal(err)
	}
	if vs, err := c.Versions(ctx, obj); err != nil || len(vs) != 1 || vs[0] != ver {
		t.Fatalf("versions = %v %v", vs, err)
	}
	if got, err := c.Read(ctx, obj, ver); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := c.Delete(ctx, obj, ver); err != nil {
			t.Fatalf("delete #%d: %v", i, err)
		}
	}
	if _, err := c.Read(ctx, obj, ver); !errors.Is(err, disk.ErrNotFound) {
		t.Fatalf("read after delete: %v", err)
	}
}

func TestServerRejectsBadRequests(t *testing.T) {
	_, srv := server(t, "n1")
	cases := []struct {
		method, path string
		body         []byte
		want         int
	}{
		{"GET", "/v1/objects/" + "zz", nil, http.StatusBadRequest},
		{"GET", "/v1/objects/" + obj + "/v1", nil, http.StatusBadRequest},
		{"PUT", "/v1/objects/" + obj + "/v1-ab", []byte("x"), http.StatusBadRequest},
		{"PUT", "/v1/objects/" + obj[:10] + "/" + ver, []byte("x"), http.StatusBadRequest},
		{"PUT", "/v1/objects/" + obj + "/" + ver, make([]byte, disk.MaxShardFile+1), http.StatusRequestEntityTooLarge},
		{"DELETE", "/v1/objects/" + obj + "/x", nil, http.StatusBadRequest},
		{"POST", "/v1/objects/" + obj + "/" + ver, nil, http.StatusMethodNotAllowed},
		{"GET", "/v1/nothing", nil, http.StatusNotFound},
	}
	for _, c := range cases {
		req, err := http.NewRequest(c.method, srv.URL+c.path, bytes.NewReader(c.body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s %s: status %d, want %d", c.method, c.path, resp.StatusCode, c.want)
		}
	}
}

func TestClientClassifiesFailures(t *testing.T) {
	ctx := context.Background()
	status := func(code int) *httptest.Server {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", code)
		}))
		t.Cleanup(s.Close)
		return s
	}
	cases := map[string]struct {
		url  string
		want error
	}{
		"500":         {status(500).URL, disk.ErrOffline},
		"503":         {status(503).URL, disk.ErrOffline},
		"400":         {status(400).URL, disk.ErrInvalid},
		"404":         {status(404).URL, disk.ErrNotFound},
		"closed port": {"http://127.0.0.1:1", disk.ErrOffline},
	}
	for name, c := range cases {
		cl, err := NewClient("x", c.url, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cl.Read(ctx, obj, ver); !errors.Is(err, c.want) {
			t.Errorf("%s: err=%v, want %v", name, err, c.want)
		}
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	t.Cleanup(slow.Close)
	cl, _ := NewClient("slow", slow.URL, 100*time.Millisecond)
	start := time.Now()
	if _, err := cl.Versions(ctx, obj); !errors.Is(err, disk.ErrOffline) {
		t.Errorf("slow node: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Error("client timeout not applied")
	}
	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{not json"))
	}))
	t.Cleanup(garbage.Close)
	cl, _ = NewClient("g", garbage.URL, time.Second)
	if _, err := cl.Versions(ctx, obj); err == nil {
		t.Error("garbage version list accepted")
	}
	huge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), 8<<10))
	}))
	t.Cleanup(huge.Close)
	cl, _ = NewClient("h", huge.URL, time.Second)
	if _, err := cl.Health(ctx); !errors.Is(err, disk.ErrInvalid) {
		t.Errorf("oversized response: %v", err)
	}
}

func TestClientRejectsBadConfigAndNames(t *testing.T) {
	for _, u := range []string{"", "ftp://h:1", "http://", "http://h:1/path", "http://u:p@h:1", "http://h:1?x=1", "::"} {
		if _, err := NewClient("x", u, time.Second); err == nil {
			t.Errorf("URL %q accepted", u)
		}
	}
	if _, err := NewClient("x", "http://h:1", 0); err == nil {
		t.Error("zero timeout accepted")
	}
	c, err := NewClient("x", "http://127.0.0.1:1/", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.Versions(ctx, "../x"); !errors.Is(err, disk.ErrInvalid) {
		t.Errorf("bad object: %v", err)
	}
	if err := c.Write(ctx, obj, "../../x", nil); !errors.Is(err, disk.ErrInvalid) {
		t.Errorf("bad version: %v", err)
	}
	if err := c.Delete(ctx, obj, ""); !errors.Is(err, disk.ErrInvalid) {
		t.Errorf("empty version: %v", err)
	}
}

// Six disk nodes over real HTTP in one process: objects survive two nodes stopping.
func TestStoreOverHTTPNodesSurvivesTwoNodesDown(t *testing.T) {
	ctx := context.Background()
	var members []store.Member
	servers := map[string]*httptest.Server{}
	for i := 1; i <= 6; i++ {
		id := fmt.Sprintf("n%d", i)
		c, srv := server(t, id)
		servers[id] = srv
		members = append(members, store.Member{Disk: c, Domain: fmt.Sprintf("rack%d", (i-1)%3+1)})
	}
	cfg := store.DefaultConfig()
	cfg.Timeout, cfg.Retries, cfg.Backoff = time.Second, 1, 10*time.Millisecond
	s, err := store.New(members, cfg)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("erasure-coded "), 20000)
	if _, err := s.Put(ctx, "photos/cat.jpg", data); err != nil {
		t.Fatal(err)
	}
	servers["n2"].Close()
	servers["n5"].Close()
	got, info, err := s.Get(ctx, "photos/cat.jpg")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("get with two nodes down: %v", err)
	}
	if len(info.Missing) != 2 {
		t.Fatalf("missing = %v, want two shards", info.Missing)
	}
	servers["n1"].Close()
	if _, _, err := s.Get(ctx, "photos/cat.jpg"); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("three nodes down: %v", err)
	}
	if _, err := s.Put(ctx, "new", []byte("x")); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("put with nodes down: %v", err)
	}
}
