#!/usr/bin/env bash
set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
TESTNET_DIR="$REPO_DIR/mytestnet"
TENDERMINT="$REPO_DIR/build/cometbft"
ABCI_CLI="$REPO_DIR/build/abci-cli"
LOAD_BIN="$REPO_DIR/build/load"
REPORT_BIN="$REPO_DIR/build/report"
DELAY_SCHEDULE="$REPO_DIR/delay_schedule.json"
LOAD_DURATION=30
LOAD_RATE=200
LOAD_CONNECTIONS=5
LOAD_SIZE=500
EPOCH_SIZE=1000
POST_PEER_CONNECT_DELAY=5
LOG_DIR="$REPO_DIR/logs"
SERVER_LOG_DIR="$LOG_DIR/server"
CLIENT_LOG_FILE="$LOG_DIR/tendermint_client.log"
CLOSE_WINDOWS=0
INITIAL_TERMINAL_WINDOW_IDS=()

# Parse arguments.
usage() {
  echo "Usage: $0 [--close-windows|-w] <n> <f>"
  echo "  n  total number of nodes"
  echo "  f  compatibility arg from old maverick flow (ignored in CometBFT-only mode)"
  echo "  --close-windows, -w  close Terminal windows opened by this script on exit"
  exit 1
}

POSITIONAL_ARGS=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --close-windows|-w)
      CLOSE_WINDOWS=1
      shift
      ;;
    -h|--help)
      usage
      ;;
    -*)
      echo "Error: unknown option $1"
      usage
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done

[ "${#POSITIONAL_ARGS[@]}" -eq 2 ] || usage
N="${POSITIONAL_ARGS[0]}"
F="${POSITIONAL_ARGS[1]}"
[[ "$N" =~ ^[0-9]+$ ]] && [[ "$F" =~ ^[0-9]+$ ]] || { echo "Error: n and f must be non-negative integers"; usage; }
[ "$N" -gt 0 ] || { echo "Error: n must be greater than 0"; exit 1; }

if [ "$F" -ne 0 ]; then
  echo "==> Note: maverick nodes are not used in CometBFT mode; ignoring f=$F"
fi

echo "==> n=$N total cometbft nodes"
mkdir -p "$LOG_DIR" "$SERVER_LOG_DIR"
rm -f "$SERVER_LOG_DIR"/*.log
: > "$CLIENT_LOG_FILE"
echo "==> Server logs dir: $SERVER_LOG_DIR"
echo "==> Client logs file: $CLIENT_LOG_FILE"
# Port layout (per node i):
# p2p   = 26656 + i*3
# rpc   = 26657 + i*3
# abci  = 26658 + i*3
# agent = 50000 + i  (adaptive timer gRPC)
p2p_port()   { echo $(( 26656 + $1 * 3 )); }
rpc_port()   { echo $(( 26657 + $1 * 3 )); }
abci_port()  { echo $(( 26658 + $1 * 3 )); }
agent_port() { echo $(( 50000 + $1 )); }

run_and_log() {
  local log_file="$1"
  shift
  "$@" 2>&1 | tee -a "$log_file"
  return "${PIPESTATUS[0]}"
}

terminal_window_ids() {
  osascript <<'OSA' 2>/dev/null || true
  tell application "Terminal"
    set idList to id of windows
    set AppleScript's text item delimiters to linefeed
    return idList as text
  end tell
OSA
}

capture_initial_windows() {
  INITIAL_TERMINAL_WINDOW_IDS=()
  while IFS= read -r wid; do
    [ -n "$wid" ] && INITIAL_TERMINAL_WINDOW_IDS+=("$wid")
  done < <(terminal_window_ids)
}

close_new_windows() {
  local current_ids=()
  local wid
  local existed

  while IFS= read -r wid; do
    [ -n "$wid" ] && current_ids+=("$wid")
  done < <(terminal_window_ids)

  for wid in "${current_ids[@]}"; do
    existed=0
    for existing in "${INITIAL_TERMINAL_WINDOW_IDS[@]}"; do
      if [ "$existing" = "$wid" ]; then
        existed=1
        break
      fi
    done
    if [ "$existed" -eq 0 ]; then
      osascript \
        -e "set targetId to ${wid}" \
        -e 'tell application "Terminal"' \
        -e 'try' \
        -e 'if exists window id targetId then close (window id targetId)' \
        -e 'end try' \
        -e 'end tell' >/dev/null 2>&1 || true
    fi
  done
}

cleanup() {
  echo ""
  echo "==> Stopping all nodes and kvstore processes..."
  pkill -f "cometbft node" 2>/dev/null || true
  pkill -f "abci-cli kvstore" 2>/dev/null || true
  if [ "$CLOSE_WINDOWS" -eq 1 ]; then
    echo "==> Closing spawned Terminal windows..."
    close_new_windows
  fi
  echo "==> Done."
}
trap cleanup EXIT INT TERM

# Step 1: Build binaries.
echo "==> Building cometbft, abci-cli, load, and report..."
make build 2>&1 | tail -1
go build -o "$ABCI_CLI" ./abci/cmd/abci-cli
go build -o "$LOAD_BIN" ./test/loadtime/cmd/load
go build -o "$REPORT_BIN" ./test/loadtime/cmd/report

# Step 2: Generate/reset testnet config.
if [ ! -d "$TESTNET_DIR/node0" ]; then
  echo "==> Generating testnet config..."
  "$TENDERMINT" testnet \
    --v "$N" \
    --o "$TESTNET_DIR" \
    --populate-persistent-peers \
    --starting-ip-address 127.0.0.1
else
  echo "==> Testnet config already exists, resetting data..."
  for (( i=0; i<N; i++ )); do
    "$TENDERMINT" unsafe_reset_all --home "$TESTNET_DIR/node$i"
  done
fi

# Step 3: Patch config.toml for each node.
echo "==> Patching config files..."
for (( i=0; i<N; i++ )); do
  CONFIG_FILE="$TESTNET_DIR/node$i/config/config.toml"

  # Remap generated sequential loopback IP peers to 127.0.0.1 with per-node p2p ports.
  for (( j=1; j<N; j++ )); do
    sed -i '' "s|@127.0.0.$(( j + 1 )):26656|@127.0.0.1:$(p2p_port "$j")|g" "$CONFIG_FILE"
  done

  # Adaptive timer settings.
  AGENT_PORT=$(agent_port "$i")
  sed -i '' "s|adaptive_timer_addr = \".*\"|adaptive_timer_addr = \"127.0.0.1:$AGENT_PORT\"|" "$CONFIG_FILE"
  sed -i '' "s|adaptive_timer_node_index = .*|adaptive_timer_node_index = $i|" "$CONFIG_FILE"
  sed -i '' "s|adaptive_timer_epoch_size = .*|adaptive_timer_epoch_size = $EPOCH_SIZE|" "$CONFIG_FILE"
done

echo "    persistent_peers remapped to 127.0.0.1:PORT for all $N nodes"
echo "    adaptive timer ports: 50000..$(( 50000 + N - 1 ))"

# Step 4: Start kvstore + cometbft in Terminal windows.
echo "==> Starting kvstore and cometbft nodes in new Terminal windows..."
if [ "$CLOSE_WINDOWS" -eq 1 ]; then
  capture_initial_windows
fi
for (( i=0; i<N; i++ )); do
  ABCI_PORT=$(abci_port "$i")
  P2P_PORT=$(p2p_port "$i")
  RPC_PORT=$(rpc_port "$i")
  NODE_HOME="$TESTNET_DIR/node$i"
  KVSTORE_LOG_FILE="$SERVER_LOG_DIR/kvstore_${i}.log"

  osascript \
    -e "tell application \"Terminal\"" \
    -e "  do script \"echo '=== kvstore node$i ===' | tee -a \\\"$KVSTORE_LOG_FILE\\\" && $ABCI_CLI kvstore --address tcp://127.0.0.1:$ABCI_PORT 2>&1 | tee -a \\\"$KVSTORE_LOG_FILE\\\"\"" \
    -e "end tell"
  NODE_LOG_FILE="$SERVER_LOG_DIR/cometbft_${i}.log"
  osascript \
    -e "tell application \"Terminal\"" \
    -e "  do script \"echo '=== cometbft node$i ===' | tee -a \\\"$NODE_LOG_FILE\\\" && COMETBFT_NODE_INDEX=$i COMETBFT_PROPOSE_DELAY_SCHEDULE=\\\"$DELAY_SCHEDULE\\\" $TENDERMINT node --home $NODE_HOME --proxy_app tcp://127.0.0.1:$ABCI_PORT --p2p.laddr tcp://0.0.0.0:$P2P_PORT --rpc.laddr tcp://0.0.0.0:$RPC_PORT 2>&1 | tee -a \\\"$NODE_LOG_FILE\\\"\"" \
    -e "end tell"
done

# Step 5: Wait for all nodes to be ready.
echo "==> Waiting for nodes to be ready..."
for (( i=0; i<N; i++ )); do
  RPC_PORT=$(rpc_port "$i")
  echo -n "    Waiting for node$i on port $RPC_PORT..."
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

# Step 6: Wait for peers to connect.
EXPECTED_PEERS=$(( N - 1 ))
RPC0=$(rpc_port 0)
echo "==> Waiting for peers to connect (expecting $EXPECTED_PEERS peers on node0)..."
for attempt in $(seq 1 20); do
  PEERS=$(curl -s "http://localhost:$RPC0/net_info" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['n_peers'])" 2>/dev/null || echo "0")
  if [ "$PEERS" -eq "$EXPECTED_PEERS" ]; then
    echo "    All $N nodes peered (n_peers=$EXPECTED_PEERS)"
    break
  fi
  echo "    node0 sees $PEERS peers, waiting..."
  sleep 2
done

# Step 7: Run load test.
echo ""
if [ "$POST_PEER_CONNECT_DELAY" -gt 0 ]; then
  echo "==> Waiting ${POST_PEER_CONNECT_DELAY}s for cluster stabilization before load..."
  sleep "$POST_PEER_CONNECT_DELAY"
fi

echo ""
echo "==> Running load test: rate=$LOAD_RATE tx/s, duration=${LOAD_DURATION}s, connections=$LOAD_CONNECTIONS, size=${LOAD_SIZE}B"
run_and_log "$CLIENT_LOG_FILE" \
  "$LOAD_BIN" \
  --endpoints "ws://localhost:$RPC0/websocket" \
  --broadcast-tx-method async \
  --connections "$LOAD_CONNECTIONS" \
  --rate "$LOAD_RATE" \
  --time "$LOAD_DURATION" \
  --size "$LOAD_SIZE"

# Step 8: Stop nodes before reading blockstore.
echo ""
echo "==> Stopping cometbft nodes to release blockstore lock..."
pkill -f "cometbft node" 2>/dev/null || true
sleep 2

# Step 9: Generate report.
echo "==> Generating latency report..."
run_and_log "$CLIENT_LOG_FILE" \
  "$REPORT_BIN" \
  --data-dir "$TESTNET_DIR/node0/data" \
  --database-type goleveldb \
  --csv "$REPO_DIR/results.csv"

echo ""
echo "==> Latency report:"
run_and_log "$CLIENT_LOG_FILE" \
  "$REPORT_BIN" \
  --data-dir "$TESTNET_DIR/node0/data" \
  --database-type goleveldb

# Step 10: Throughput from CSV.
echo ""
echo "==> Throughput:"
awk -F',' '
  NR==1 { next }
  NR==2 { first=$2; last=$2; count=1; next }
  { if ($2+0 > last+0) last=$2; count++ }
  END {
    duration_s = (last - first) / 1e9
    if (duration_s > 0)
      printf "    Total tx: %d\\n    Duration: %.2fs\\n    Throughput: %.2f tx/sec\\n", count, duration_s, count/duration_s
    else
      print "    Not enough data"
  }
' "$REPO_DIR/results.csv" | tee -a "$CLIENT_LOG_FILE"

echo ""
echo "==> Raw CSV saved to: $REPO_DIR/results.csv"
echo "==> Server logs saved to: $SERVER_LOG_DIR"
echo "==> Client logs saved to: $CLIENT_LOG_FILE"
