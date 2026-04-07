#!/usr/bin/env bash
set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
TESTNET_DIR="$REPO_DIR/mytestnet"
TENDERMINT="$REPO_DIR/build/cometbft"
ABCI_CLI="$REPO_DIR/build/abci-cli"
LOAD_BIN="$REPO_DIR/build/load"
REPORT_BIN="$REPO_DIR/build/report"
FAILURE_SPEC="$REPO_DIR/failure_spec.xml"
LEARNING_AGENT_DIR="$REPO_DIR/../learning_agent"
LEARNING_AGENT_MAIN="$LEARNING_AGENT_DIR/main.py"
SCHEDULE_TENDERMINT="$LEARNING_AGENT_DIR/config/schedule_tendermint.csv"

LOAD_DURATION=30
LOAD_CONNECTIONS=500
LOAD_RATE=1000
LOAD_SEND_PERIOD="1s"
LOAD_SIZE=500
EPOCH_SIZE=1000
POST_PEER_CONNECT_DELAY=5
START_ALIGN_DELAY_MS=15000

LOG_DIR="$REPO_DIR/logs"
SERVER_LOG_DIR="$LOG_DIR/server"
CLIENT_LOG_FILE="$LOG_DIR/tendermint_client.log"

CLOSE_WINDOWS=0
MODE="normal"
INITIAL_TERMINAL_WINDOW_IDS=()
START_UNIX_MS=0
AGENT_HOSTS_CONFIG="$LOG_DIR/agent_hosts.config"
PYTHON_BIN="${PYTHON_BIN:-python3}"
AGENT_PIDS=()

usage() {
  echo "Usage: $0 [--close-windows|-w] <n> <f> [normal|scheduled]"
  echo "  n     total number of nodes"
  echo "  f     number of delay-injected nodes (first f nodes: 0..f-1)"
  echo "  mode  normal (default) or scheduled"
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

[ "${#POSITIONAL_ARGS[@]}" -ge 2 ] && [ "${#POSITIONAL_ARGS[@]}" -le 3 ] || usage
N="${POSITIONAL_ARGS[0]}"
F="${POSITIONAL_ARGS[1]}"
[[ "$N" =~ ^[0-9]+$ ]] && [[ "$F" =~ ^[0-9]+$ ]] || {
  echo "Error: n and f must be non-negative integers"
  usage
}
[ "$N" -gt 0 ] || { echo "Error: n must be greater than 0"; exit 1; }
[ "$F" -le "$N" ] || { echo "Error: f ($F) cannot exceed n ($N)"; exit 1; }

if [ "${#POSITIONAL_ARGS[@]}" -eq 3 ]; then
  MODE="${POSITIONAL_ARGS[2]}"
fi
case "$MODE" in
  normal|scheduled) ;;
  *)
    echo "Error: unsupported mode '$MODE'. Supported: normal, scheduled"
    usage
    ;;
esac

DERIVED_F=$(( (N - 1) / 3 ))
if [ "$F" -ne "$DERIVED_F" ]; then
  echo "==> Note: provided f=$F differs from (n-1)/3=$DERIVED_F for n=$N"
fi

echo "==> mode=$MODE, n=$N total nodes, delay-injected nodes=$F (0..$(( F - 1 )))"

if [ "$F" -gt 0 ] && [ ! -f "$FAILURE_SPEC" ]; then
  echo "Error: failure spec not found: $FAILURE_SPEC"
  exit 1
fi

if [ "$MODE" = "scheduled" ]; then
  if [ ! -f "$LEARNING_AGENT_MAIN" ]; then
    echo "Error: learning agent not found: $LEARNING_AGENT_MAIN"
    exit 1
  fi
  if [ ! -f "$SCHEDULE_TENDERMINT" ]; then
    echo "Error: schedule file not found: $SCHEDULE_TENDERMINT"
    exit 1
  fi
fi

NOW_UNIX_MS=$($PYTHON_BIN -c 'import time; print(int(time.time() * 1000))')
START_UNIX_MS=$(( NOW_UNIX_MS + START_ALIGN_DELAY_MS ))
echo "==> Synced start time (ms): $START_UNIX_MS (delay ${START_ALIGN_DELAY_MS}ms from now)"

mkdir -p "$LOG_DIR" "$SERVER_LOG_DIR"
rm -f "$SERVER_LOG_DIR"/*.log
: > "$CLIENT_LOG_FILE"

echo "==> Server logs dir: $SERVER_LOG_DIR"
echo "==> Client logs file: $CLIENT_LOG_FILE"

# Port layout (per node i):
# p2p   = 26656 + i*3
# rpc   = 26657 + i*3
# abci  = 26658 + i*3
# agent = 50000 + i
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

write_agent_hosts_config() {
  : > "$AGENT_HOSTS_CONFIG"
  for (( i=0; i<N; i++ )); do
    echo "$i 127.0.0.1 0 0 $(agent_port "$i") $(rpc_port "$i")" >> "$AGENT_HOSTS_CONFIG"
  done
}

start_learning_agents_if_enabled() {
  if [ "$MODE" != "scheduled" ]; then
    return
  fi

  if ! "$PYTHON_BIN" -c "import grpc, torch, sklearn, cryptography" >/dev/null 2>&1; then
    echo "Error: scheduled mode requires grpc/torch/sklearn/cryptography in $PYTHON_BIN"
    echo "Install learning_agent/requirements.txt and/or set PYTHON_BIN."
    exit 1
  fi

  write_agent_hosts_config
  AGENT_PIDS=()
  echo "==> Starting learning-agent replicas (scheduled mode)..."
  for (( i=0; i<N; i++ )); do
    local agent_log_file="$SERVER_LOG_DIR/agent_${i}.log"
    : > "$agent_log_file"
    "$PYTHON_BIN" "$LEARNING_AGENT_MAIN" \
      --node-id "$i" \
      --protocol tendermint \
      --hosts-config "$AGENT_HOSTS_CONFIG" \
      --model-type scheduled \
      --schedule-file "$SCHEDULE_TENDERMINT" \
      --injection-start-unix-ms "$START_UNIX_MS" \
      >"$agent_log_file" 2>&1 &
    AGENT_PIDS+=("$!")
  done

  sleep 1
  for (( i=0; i<N; i++ )); do
    if ! kill -0 "${AGENT_PIDS[$i]}" 2>/dev/null; then
      echo "Learning-agent process exited early for node$i. Last log lines:"
      tail -n 60 "$SERVER_LOG_DIR/agent_${i}.log" || true
      exit 1
    fi
  done
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
  echo "==> Stopping all nodes, kvstore, and learning-agent processes..."
  pkill -f "cometbft node" 2>/dev/null || true
  pkill -f "abci-cli kvstore" 2>/dev/null || true
  for pid in "${AGENT_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  pkill -f "learning_agent/main.py.*--protocol tendermint" 2>/dev/null || true
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

  for (( j=1; j<N; j++ )); do
    sed -i '' "s|@127.0.0.$(( j + 1 )):26656|@127.0.0.1:$(p2p_port "$j")|g" "$CONFIG_FILE"
  done

  AGENT_PORT=$(agent_port "$i")
  sed -i '' "s|adaptive_timer_addr = \".*\"|adaptive_timer_addr = \"127.0.0.1:$AGENT_PORT\"|" "$CONFIG_FILE"
  sed -i '' "s|adaptive_timer_node_index = .*|adaptive_timer_node_index = $i|" "$CONFIG_FILE"
  sed -i '' "s|adaptive_timer_epoch_size = .*|adaptive_timer_epoch_size = $EPOCH_SIZE|" "$CONFIG_FILE"
done

echo "    persistent_peers remapped to 127.0.0.1:PORT for all $N nodes"
echo "    adaptive timer ports: 50000..$(( 50000 + N - 1 ))"

# Step 4: Start learning agents if needed.
start_learning_agents_if_enabled

# Step 5: Start kvstore + cometbft in separate Terminal windows.
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
    -e "  do script \"echo '=== cometbft node$i ===' | tee -a \\\"$NODE_LOG_FILE\\\" && COMETBFT_NODE_INDEX=$i COMETBFT_PROPOSE_DELAY_FAILURE_SPEC=\\\"$FAILURE_SPEC\\\" COMETBFT_PROPOSE_DELAY_FAULTY_NODES=$F COMETBFT_PROPOSE_DELAY_START_UNIX_MS=$START_UNIX_MS $TENDERMINT node --home $NODE_HOME --proxy_app tcp://127.0.0.1:$ABCI_PORT --p2p.laddr tcp://0.0.0.0:$P2P_PORT --rpc.laddr tcp://0.0.0.0:$RPC_PORT 2>&1 | tee -a \\\"$NODE_LOG_FILE\\\"\"" \
    -e "end tell"
done

# Step 6: Wait for all nodes to be ready.
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

# Step 7: Wait for peers to connect.
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

# Step 8: Run load test.
if [ "$POST_PEER_CONNECT_DELAY" -gt 0 ]; then
  echo "==> Waiting ${POST_PEER_CONNECT_DELAY}s for cluster stabilization before load..."
  sleep "$POST_PEER_CONNECT_DELAY"
fi

echo ""
echo "==> Mode: $MODE"
echo "==> Running load: duration=${LOAD_DURATION}s, connections=$LOAD_CONNECTIONS, rate=$LOAD_RATE, send-period=${LOAD_SEND_PERIOD}, size=${LOAD_SIZE}B"
echo "==> Load start-unix-ms: $START_UNIX_MS"
run_and_log "$CLIENT_LOG_FILE" \
  "$LOAD_BIN" \
  --endpoints "ws://localhost:$RPC0/websocket" \
  --start-unix-ms "$START_UNIX_MS" \
  --connections "$LOAD_CONNECTIONS" \
  --rate "$LOAD_RATE" \
  --send-period "$LOAD_SEND_PERIOD" \
  --time "$LOAD_DURATION" \
  --size "$LOAD_SIZE"

# Step 9: Stop nodes before reading blockstore.
echo ""
echo "==> Stopping nodes to release blockstore lock..."
pkill -f "cometbft node" 2>/dev/null || true
for pid in "${AGENT_PIDS[@]}"; do
  kill "$pid" 2>/dev/null || true
done
sleep 2

# Step 10: Generate report.
REPORT_STATE_LOG="$SERVER_LOG_DIR/cometbft_0.log"
REPORT_STATE_ARGS=()
if [ -f "$REPORT_STATE_LOG" ]; then
  REPORT_STATE_ARGS+=(--state-log "$REPORT_STATE_LOG")
  echo "==> Using state log for wall-clock latency: $REPORT_STATE_LOG"
else
  echo "==> State log not found for node0; falling back to chain block timestamp latency."
fi

echo "==> Generating latency report..."
run_and_log "$CLIENT_LOG_FILE" \
  "$REPORT_BIN" \
  --data-dir "$TESTNET_DIR/node0/data" \
  --database-type goleveldb \
  "${REPORT_STATE_ARGS[@]}" \
  --csv "$REPO_DIR/results.csv"

echo ""
echo "==> Latency report:"
run_and_log "$CLIENT_LOG_FILE" \
  "$REPORT_BIN" \
  --data-dir "$TESTNET_DIR/node0/data" \
  --database-type goleveldb \
  "${REPORT_STATE_ARGS[@]}"

# Step 11: Throughput from CSV.
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
