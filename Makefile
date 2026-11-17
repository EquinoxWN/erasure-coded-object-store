.PHONY: setup lint test bench cluster audit ci

setup:
	go mod download

lint:
	@files="$$(gofmt -l cmd internal)"; test -z "$$files" || (echo "$$files"; echo "run gofmt -w"; exit 1)
	go vet ./...

test:
	go test -race -count=1 ./...

# Durability sweep on an in-process cluster of in-memory disks: every combination of up to
# m+1 lost disks, each whole domain lost, a rebuild, and the storage overhead. Exits non-zero
# if any object is unreadable or wrong with at most m disks lost.
bench:
	go run ./cmd/erasure-coded-object-store bench -objects 20 -size 1048576 -seed 1 | tee bench.txt
	go run ./cmd/erasure-coded-object-store bench -k 6 -m 3 -disks 9 -domains 3 -objects 10 -size 1048576 -seed 1 | tee -a bench.txt
	go test -run '^$$' -bench . -benchtime 20x ./internal/rs

# Six disk-node processes on loopback, stopped one by one (runs in CI).
cluster:
	bash scripts/cluster.sh

# Known vulnerabilities in the code paths actually called (Go 1.26.11, as in CI).
audit:
	bash scripts/vulncheck.sh

ci: setup lint test bench
