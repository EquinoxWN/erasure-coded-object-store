# Contributing

Thanks for helping. This is a Go project; issues and pull requests are welcome.

## Set up and check your change

Needs Go 1.24 or newer (CI uses 1.26.11). No third-party modules.

```bash
make setup     # download modules (none beyond the standard library today)
make lint      # gofmt and go vet
make test      # full test suite with the race detector
make bench     # durability sweep, storage overhead and codec benchmarks
make cluster   # six disk-node processes on loopback, stopped one by one
make audit     # known-vulnerability check (as in CI)
```

A pull request is ready when `make lint`, `make test` and `make bench` pass and CI is green.

## Guidelines

- Keep each change focused; explain the problem it solves in the pull request.
- Add or update a test for every behaviour change, and update `docs/results/` when numbers change.
- Changes to the shard file format must stay readable by the previous version (new header
  fields optional); say so in the pull request.
- Significant design changes need an ADR in `docs/adr/` (copy `0001` for the format).
- Keep function comments to one short phrase; let names and tests explain the rest.
- Report security problems privately, as described in [SECURITY.md](SECURITY.md).

By contributing you agree that your work is released under the [MIT License](LICENSE).
