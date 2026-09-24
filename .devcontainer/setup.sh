#!/bin/bash
set -e

echo "[*] installing go..."
if ! command -v go &>/dev/null; then
    wget -q https://go.dev/dl/go1.21.5.linux-amd64.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
    rm go1.21.5.linux-amd64.tar.gz
    echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
    export PATH=$PATH:/usr/local/go/bin
fi

echo "[*] building engine..."
cd worker/engine
go mod init flood_engine 2>/dev/null || true
go build -ldflags="-s -w" -o flood_engine main.go
cd ../..

echo "[*] installing python deps..."
pip install --quiet websockets

echo "[+] setup done"