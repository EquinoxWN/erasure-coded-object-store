#!/usr/bin/env bash
# Six disk-node processes on 127.0.0.1:7201-7206 (three racks of two). Stores a 1 MiB object
# with k=4 m=2, stops two nodes and reads it back, then stops a third and expects the read to
# fail. Run in CI; locally the same path is covered in process by the Go tests.
set -euo pipefail
work="$(pwd)/.tmp/cluster"
bin="$work/ecstore"
mkdir -p "$work"
go build -o "$bin" ./cmd/erasure-coded-object-store

pids=()
stop_all() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap stop_all EXIT

spec=""
for i in 1 2 3 4 5 6; do
  rack=$(( (i - 1) % 3 + 1 ))
  "$bin" serve -id "n$i" -dir "$work/disk$i" -addr "127.0.0.1:720$i" > "$work/n$i.log" 2>&1 &
  pids+=("$!")
  spec="${spec:+$spec,}n$i@rack$rack=http://127.0.0.1:720$i"
done
for i in 1 2 3 4 5 6; do
  for _ in $(seq 1 50); do
    grep -q serving "$work/n$i.log" && break
    sleep 0.1
  done
  grep -q serving "$work/n$i.log" || { echo "::error::node n$i did not start"; cat "$work/n$i.log"; exit 1; }
done

head -c 1048576 /dev/urandom > "$work/in.bin"
"$bin" put -disks "$spec" -timeout 2s photos/big.bin "$work/in.bin"

kill "${pids[1]}" "${pids[4]}"   # n2 (rack2) and n5 (rack2): a whole rack
sleep 0.5
"$bin" get -disks "$spec" -timeout 2s photos/big.bin "$work/out.bin"
cmp "$work/in.bin" "$work/out.bin"
echo "two nodes stopped: object read back and matches"

kill "${pids[0]}"                # n1: three shards now unreachable
sleep 0.5
if "$bin" get -disks "$spec" -timeout 2s photos/big.bin "$work/out2.bin"; then
  echo "::error::cluster: read succeeded with three of six nodes down (k=4 needs four shards)"
  exit 1
fi
echo "three nodes stopped: read refused as expected"
echo "::notice::cluster: 6 node processes, k=4 m=2; 1 MiB object read back with 2 nodes (one rack) down, refused with 3 down"
