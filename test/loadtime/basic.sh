#!/bin/sh

set -euo pipefail

# A basic invocation of the loadtime tool.

./build/load \
    --connections 1 --time 10 --rate 1000 --size 1024 --send-period 1s \
    --endpoints ws://localhost:26657/websocket

