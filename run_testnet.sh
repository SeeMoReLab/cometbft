#!/usr/bin/env bash
set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
TESTNET_DIR="$REPO_DIR/mytestnet"
TENDERMINT="$REPO_DIR/build/cometbft"
ABCI_CLI="$REPO_DIR/build/abci-cli"
LOAD_BIN="$REPO_DIR/build/load"
REPORT_BIN="$REPO_DIR/build/report"
DELAY_SCHEDULE="$REPO_DIR/delay_schedule.json"
LOAD_DURATION=20
LOAD_RATE=5000
LOAD_CONNECTIONS=2
LOAD_SIZE=250

# Port layout:
# node0: p2p=26656, rpc=26657, abci=26658
# node1: p2p=26659, rpc=26660, abci=26668
# node2: p2p=26661, rpc=26662, abci=26678
# node3: p2p=26663, rpc=26664, abci=26688

ABCI_PORTS=(26658 26668 26678 26688)
P2P_PORTS=(26656 26659 26661 26663)
RPC_PORTS=(26657 26660 26662 26664)

cleanup() {
  echo ""
  echo "==> Stopping all nodes and kvstore processes..."
  pkill -f "cometbft node" 2>/dev/null || true
  pkill -f "abci-cli kvstore" 2>/dev/null || true
  echo "==> Done."
}
trap cleanup EXIT INT TERM

# ── Step 1: Build binaries ───────────────────────────────────────────────────
echo "==> Building cometbft, abci-cli, load, and report..."
make build 2>&1 | tail -1
go build -o "$ABCI_CLI" ./abci/cmd/abci-cli
go build -o "$LOAD_BIN" ./test/loadtime/cmd/load
go build -o "$REPORT_BIN" ./test/loadtime/cmd/report

# ── Step 2: Generate testnet config ─────────────────────────────────────────
if [ ! -d "$TESTNET_DIR/node0" ]; then
  echo "==> Generating testnet config..."
  "$TENDERMINT" testnet \
    --v 4 \
    --o "$TESTNET_DIR" \
    --populate-persistent-peers \
    --starting-ip-address 127.0.0.1
else
  echo "==> Testnet config already exists, resetting data..."
  for i in 0 1 2 3; do
    "$TENDERMINT" unsafe_reset_all --home "$TESTNET_DIR/node$i"
  done
fi

# ── Step 3: Start kvstore + cometbft in separate Terminal windows ────────────
echo "==> Starting kvstore and cometbft nodes in new Terminal windows..."

for i in 0 1 2 3; do
  ABCI_PORT=${ABCI_PORTS[$i]}
  P2P_PORT=${P2P_PORTS[$i]}
  RPC_PORT=${RPC_PORTS[$i]}
  NODE_HOME="$TESTNET_DIR/node$i"

  osascript \
    -e "tell application \"Terminal\"" \
    -e "  do script \"echo '=== kvstore node$i ===' && $ABCI_CLI kvstore --address tcp://127.0.0.1:$ABCI_PORT\"" \
    -e "end tell"

  osascript \
    -e "tell application \"Terminal\"" \
    -e "  do script \"echo '=== cometbft node$i ===' && COMETBFT_NODE_INDEX=$i COMETBFT_PROPOSE_DELAY_SCHEDULE=\\\"$DELAY_SCHEDULE\\\" $TENDERMINT node --home $NODE_HOME --proxy_app tcp://127.0.0.1:$ABCI_PORT --p2p.laddr tcp://0.0.0.0:$P2P_PORT --rpc.laddr tcp://0.0.0.0:$RPC_PORT\"" \
    -e "end tell"
done

# ── Step 4: Wait for all nodes to be ready ───────────────────────────────────
echo "==> Waiting for nodes to be ready..."
for RPC_PORT in "${RPC_PORTS[@]}"; do
  echo -n "    Waiting for node on port $RPC_PORT..."
  for attempt in $(seq 1 30); do
    if curl -sf "http://localhost:$RPC_PORT/status" > /dev/null 2>&1; then
      echo " ready"
      break
    fi
    if [ "$attempt" -eq 30 ]; then
      echo " TIMED OUT"
      exit 1
    fi
    sleep 1
    echo -n "."
  done
done

# ── Step 4: Wait for peers to connect ────────────────────────────────────────
echo "==> Waiting for peers to connect..."
for attempt in $(seq 1 20); do
  PEERS=$(curl -s http://localhost:26657/net_info | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['n_peers'])" 2>/dev/null || echo "0")
  if [ "$PEERS" -eq 3 ]; then
    echo "    All 4 nodes peered (n_peers=3)"
    break
  fi
  echo "    node0 sees $PEERS peers, waiting..."
  sleep 2
done

# ── Step 5: Run load test ─────────────────────────────────────────────────────
echo ""
echo "==> Running load test: rate=$LOAD_RATE tx/s, duration=${LOAD_DURATION}s, connections=$LOAD_CONNECTIONS, size=${LOAD_SIZE}B"
"$LOAD_BIN" \
  --endpoints ws://localhost:26657/websocket \
  --broadcast-tx-method async \
  --connections "$LOAD_CONNECTIONS" \
  --rate "$LOAD_RATE" \
  --time "$LOAD_DURATION" \
  --size "$LOAD_SIZE"

# ── Step 6: Stop nodes before reading blockstore ─────────────────────────────
echo ""
echo "==> Stopping cometbft nodes to release blockstore lock..."
pkill -f "cometbft node" 2>/dev/null || true
sleep 2

# ── Step 7: Generate report ───────────────────────────────────────────────────
echo "==> Generating latency report..."
"$REPORT_BIN" \
  --data-dir "$TESTNET_DIR/node0/data" \
  --database-type goleveldb \
  --csv "$REPO_DIR/results.csv"

echo ""
echo "==> Latency report:"
"$REPORT_BIN" \
  --data-dir "$TESTNET_DIR/node0/data" \
  --database-type goleveldb

# ── Step 8: Throughput from CSV ───────────────────────────────────────────────
echo ""
echo "==> Throughput:"
awk -F',' '
  NR==1 { next }
  NR==2 { first=$2; last=$2; count=1; next }
  { if ($2+0 > last+0) last=$2; count++ }
  END {
    duration_s = (last - first) / 1e9
    if (duration_s > 0)
      printf "    Total tx: %d\n    Duration: %.2fs\n    Throughput: %.2f tx/sec\n", count, duration_s, count/duration_s
    else
      print "    Not enough data"
  }
' "$REPO_DIR/results.csv"

echo ""
echo "==> Raw CSV saved to: $REPO_DIR/results.csv"
