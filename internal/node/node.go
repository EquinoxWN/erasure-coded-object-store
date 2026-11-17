// Package node serves one disk over HTTP and provides the matching client, so the shards of an
// object can live on separate processes or machines.
//
//	GET    /v1/health                       -> {"id": "..."}
//	GET    /v1/objects/{object}             -> {"versions": [...]}
//	GET    /v1/objects/{object}/{version}   -> shard file bytes
//	PUT    /v1/objects/{object}/{version}   <- shard file bytes
//	DELETE /v1/objects/{object}/{version}
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/EquinoxWN/erasure-coded-object-store/internal/disk"
)

// Handler returns the HTTP handler for one disk.
func Handler(d disk.Disk) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"id": d.ID()})
	})
	mux.HandleFunc("GET /v1/objects/{object}", func(w http.ResponseWriter, r *http.Request) {
		vs, err := d.Versions(r.Context(), r.PathValue("object"))
		if err != nil {
			writeError(w, err)
			return
		}
		if vs == nil {
			vs = []string{}
		}
		writeJSON(w, http.StatusOK, map[string][]string{"versions": vs})
	})
	mux.HandleFunc("GET /v1/objects/{object}/{version}", func(w http.ResponseWriter, r *http.Request) {
		data, err := d.Read(r.Context(), r.PathValue("object"), r.PathValue("version"))
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data) // the client has gone if this fails; nothing left to report to
	})
	mux.HandleFunc("PUT /v1/objects/{object}/{version}", func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, disk.MaxShardFile))
		if err != nil {
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				http.Error(w, "shard file too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := d.Write(r.Context(), r.PathValue("object"), r.PathValue("version"), data); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /v1/objects/{object}/{version}", func(w http.ResponseWriter, r *http.Request) {
		if err := d.Delete(r.Context(), r.PathValue("object"), r.PathValue("version")); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// writeJSON sends v as a JSON response.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the client has gone if this fails; nothing left to report to
}

// writeError maps a disk error to an HTTP status.
func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, disk.ErrInvalid):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, disk.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, disk.ErrOffline):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, "disk error", http.StatusInternalServerError)
	}
}

// Client is a disk.Disk that talks to a remote node.
type Client struct {
	id   string
	base string
	http *http.Client
}

// NewClient returns a client for the node at base (http://host:port or https://host:port).
func NewClient(id, base string, timeout time.Duration) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || (u.Path != "" && u.Path != "/") ||
		u.RawQuery != "" || u.User != nil {
		return nil, fmt.Errorf("%w: node URL %q must be http(s)://host:port", disk.ErrInvalid, base)
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("%w: timeout must be positive", disk.ErrInvalid)
	}
	return &Client{id: id, base: strings.TrimSuffix(base, "/"), http: &http.Client{Timeout: timeout}}, nil
}

// ID returns the disk id.
func (c *Client) ID() string { return c.id }

// Health returns the id the node reports for itself.
func (c *Client) Health(ctx context.Context) (string, error) {
	body, err := c.do(ctx, http.MethodGet, "/v1/health", nil, 4<<10)
	if err != nil {
		return "", err
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("node %s: health: %w", c.id, err)
	}
	return out.ID, nil
}

// Versions lists the shard versions the node stores for an object.
func (c *Client) Versions(ctx context.Context, object string) ([]string, error) {
	if !disk.ValidObject(object) {
		return nil, fmt.Errorf("%w: object name %q", disk.ErrInvalid, object)
	}
	body, err := c.do(ctx, http.MethodGet, "/v1/objects/"+object, nil, 1<<20)
	if err != nil {
		return nil, err
	}
	var out struct {
		Versions []string `json:"versions"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("node %s: versions: %w", c.id, err)
	}
	return out.Versions, nil
}

// Read fetches one shard file.
func (c *Client) Read(ctx context.Context, object, version string) ([]byte, error) {
	if err := checkNames(object, version); err != nil {
		return nil, err
	}
	return c.do(ctx, http.MethodGet, "/v1/objects/"+object+"/"+version, nil, disk.MaxShardFile)
}

// Write stores one shard file on the node.
func (c *Client) Write(ctx context.Context, object, version string, data []byte) error {
	if err := checkNames(object, version); err != nil {
		return err
	}
	_, err := c.do(ctx, http.MethodPut, "/v1/objects/"+object+"/"+version, data, 4<<10)
	return err
}

// Delete removes one shard file on the node.
func (c *Client) Delete(ctx context.Context, object, version string) error {
	if err := checkNames(object, version); err != nil {
		return err
	}
	_, err := c.do(ctx, http.MethodDelete, "/v1/objects/"+object+"/"+version, nil, 4<<10)
	return err
}

// checkNames rejects names before they reach a URL.
func checkNames(object, version string) error {
	if !disk.ValidObject(object) || !disk.ValidVersion(version) {
		return fmt.Errorf("%w: object %q version %q", disk.ErrInvalid, object, version)
	}
	return nil
}

// do sends one request and maps the response status back to disk errors.
func (c *Client) do(ctx context.Context, method, path string, body []byte, limit int64) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", disk.ErrInvalid, err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("node %s: %w: %w", c.id, disk.ErrOffline, err)
	}
	defer func() { _ = resp.Body.Close() }() // read-only body; a close error changes nothing
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("node %s: %w: read response: %w", c.id, disk.ErrOffline, err)
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%w: node %s response over %d bytes", disk.ErrInvalid, c.id, limit)
	}
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return data, nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("node %s: %w", c.id, disk.ErrNotFound)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return nil, fmt.Errorf("node %s: %w: %s: %s", c.id, disk.ErrInvalid, resp.Status, strings.TrimSpace(string(data)))
	default:
		return nil, fmt.Errorf("node %s: %w: %s", c.id, disk.ErrOffline, resp.Status)
	}
}
