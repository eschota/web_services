#!/usr/bin/env python3
"""AutoRig rooms: presence and state for the multiplayer viewer (Multiplayer V3, 2026-10-10).

Every scene is a room. Players of the Unity viewer connect to wss://autorig.online/api/rooms/ws, send their
position / yaw / animation state at about 10 Hz and get back the other ACTIVE players of the room at 10 Hz.

  GET /api/rooms                      per-room player counts (public, no personal data)
  GET /api/rooms/busiest[?among=a,b]  the room with the most active people (the session agent's tool)
  GET /api/rooms/health               counters
  WS  /api/rooms/ws                   the room socket (anonymous)

Design rules
  * No personal data: a connection gets a random id; no name, no account, no address is stored beyond a counter
    of open sockets per address (memory only, dropped on disconnect).
  * A player's avatar is announced only when the run behind it belongs to a PUBLIC task. A private task is
    never shown: its avatar is null (peers see a neutral marker).
  * Rate limits: token bucket per socket, frames <= 1 KiB, one hello per 2 s, sockets per address, players
    per room, a state older than 15 s drops a player out of the snapshots.
  * Standalone service: restarting it never touches autorig-storage.

Run: /srv/autorig/venv/bin/python3 /srv/autorig/rooms/rooms_api.py   (127.0.0.1:8264)
"""
import asyncio
import json
import math
import os
import random
import re
import time
from http import HTTPStatus

import httpx
from websockets.asyncio.server import serve
from websockets.datastructures import Headers
from websockets.http11 import Response

HOST = os.environ.get("ROOMS_HOST", "127.0.0.1")
PORT = int(os.environ.get("ROOMS_PORT", "8264"))
MT = os.environ.get("ROOMS_MT", "http://127.0.0.1:8251")
WEB = os.environ.get("ROOMS_WEB", "http://127.0.0.1:8200")
PUBLIC = os.environ.get("ROOMS_PUBLIC", "https://autorig.online")
DEFAULT_ROOM = os.environ.get("ROOMS_DEFAULT", "sponza")      # where newcomers meet while every room is empty
TICK = 0.1                    # 10 Hz
MAX_PER_ROOM = 24
MAX_VISIBLE = 6               # avatars sent to one client (nearest first); the viewer shows at most as many
MAX_PER_IP = 6
STATE_TTL = 15.0              # a state older than this is no longer shown
IDLE_CLOSE = 90.0             # a socket silent for this long is closed
RING_R = 1.7
MAX_AREA = 60.0               # a room's walk area is at most this wide (m)
ALLOWED_ORIGINS = {"https://autorig.online", "https://www.autorig.online", None}

ROOM_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,63}$")
RUN_RE = re.compile(r"^[0-9a-f]{20}$")
ANIMS = {"idle", "walk", "run", "jump", "fall", "dance", "wave", "other"}


class Player:
    __slots__ = ("id", "ws", "ip", "room", "run", "avatar", "x", "y", "z", "yaw", "anim", "speed", "active",
                 "t_state", "t_msg", "tokens", "t_tok", "moved", "spawned", "t_hello", "sent_sig")

    def __init__(self, ws, ip):
        self.id = "p" + "".join(random.choice("0123456789abcdef") for _ in range(8))
        self.ws, self.ip = ws, ip
        self.room = None
        self.run = ""
        self.avatar = None
        self.x = self.y = self.z = 0.0
        self.yaw = 0.0
        self.anim = "idle"
        self.speed = 0.0
        self.active = False
        self.t_state = 0.0
        self.t_msg = time.time()
        self.tokens = 30.0
        self.t_tok = time.time()
        self.moved = False
        self.spawned = False
        self.t_hello = 0.0
        self.sent_sig = None


class Room:
    def __init__(self, rid):
        self.id = rid
        self.players = {}
        self.area = None               # [minx, minz, maxx, maxz], fixed by the first player
        self.dirty = True
        self.last_send = 0.0


ROOMS = {}
IPS = {}
AVATARS = {}                           # run -> (expires, dict|None)
COUNTERS = {"connections": 0, "hello": 0, "rejected": 0, "rate_limited": 0}
HTTP = None


def fnum(v, lo=-1000.0, hi=1000.0, default=0.0):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(f):
        return default
    return max(lo, min(hi, f))


def is_active(p, now):
    return p.active and p.room is not None and now - p.t_state < STATE_TTL


def room_stats(now=None):
    now = now or time.time()
    out = []
    for r in ROOMS.values():
        if not r.players:
            continue
        act = sum(1 for p in r.players.values() if is_active(p, now))
        out.append({"id": r.id, "players": len(r.players), "active": act})
    out.sort(key=lambda x: (-x["active"], -x["players"], x["id"]))
    return out


def stats_doc(among=None):
    rooms = room_stats()
    if among:
        rooms = [r for r in rooms if r["id"] in among]
    busiest = rooms[0] if rooms and rooms[0]["active"] > 0 else None
    return {"schema": "autorig.rooms/1", "rooms": rooms, "busiest": busiest, "default_room": DEFAULT_ROOM,
            "total_players": sum(r["players"] for r in rooms), "total_active": sum(r["active"] for r in rooms),
            "max_per_room": MAX_PER_ROOM, "ts": round(time.time(), 1)}


# ------------------------------------------------------------------------------------------------ avatars
async def resolve_avatar(run):
    """The public avatar of a run, or None. Private tasks, runs without a task and unrigged runs give None."""
    if not RUN_RE.match(run or ""):
        return None
    now = time.time()
    hit = AVATARS.get(run)
    if hit and hit[0] > now:
        return hit[1]
    av = None
    try:
        r = await HTTP.get(f"{MT}/api/mt/runs/{run}")
        task = ((r.json().get("request") or {}).get("task_id") or "") if r.status_code == 200 else ""
        if re.match(r"^[0-9a-f-]{8,40}$", task):
            t = await HTTP.get(f"{WEB}/api/task/{task}")
            if t.status_code == 200 and t.json().get("is_public") is True:
                rj = await HTTP.get(f"{MT}/api/mt/files/{run}/rig/rig.json")
                if rj.status_code == 200:
                    d = rj.json()
                    built = str(d.get("built_at") or "")
                    av = {"url": f"{PUBLIC}/api/mt/files/{run}/rig/rigged.glb" + (f"?v={built}" if built else ""),
                          "h": round(fnum(d.get("height_m"), 0.3, 6.0, 1.7), 3),
                          "fw": "-z" if str(d.get("forward_axis", "+z")).startswith("-") else "+z"}
    except Exception as exc:                                      # noqa: BLE001
        print("avatar", run, repr(exc)[:120], flush=True)
        av = None
    AVATARS[run] = (now + (300 if av else 60), av)
    if len(AVATARS) > 500:
        for k in sorted(AVATARS, key=lambda k: AVATARS[k][0])[:100]:
            AVATARS.pop(k, None)
    return av


# ------------------------------------------------------------------------------------------------ spawning
def clamp_area(room, x, z, margin=0.0):
    a = room.area
    if not a:
        return x, z
    return (max(a[0] - margin, min(a[2] + margin, x)), max(a[1] - margin, min(a[3] + margin, z)))


def ring_spawn(room, me, anchor):
    """Next to the people who are active right now: on a ring round their centre, inside the walk area, at least
    1.1 m from everybody, turned toward the group. Alone: at the scene's own spawn."""
    now = time.time()
    others = [p for p in room.players.values() if p is not me and is_active(p, now)]
    ax, az = anchor
    if not others:
        x, z = clamp_area(room, ax, az)
        return {"x": round(x, 2), "z": round(z, 2), "yaw": 0.0, "near": 0}
    cx = sum(p.x for p in others) / len(others)
    cz = sum(p.z for p in others) / len(others)
    best = None
    for k in range(24):
        ang = (k + len(room.players)) * 2.399963             # golden angle: slots spread evenly
        rad = RING_R + 0.9 * (k // 8)
        x, z = clamp_area(room, cx + math.cos(ang) * rad, cz + math.sin(ang) * rad)
        d = min(math.hypot(x - p.x, z - p.z) for p in others)
        if best is None or d > best[0]:
            best = (d, x, z)
        if d >= 1.1:
            break
    _, x, z = best
    yaw = math.degrees(math.atan2(cx - x, cz - z))
    return {"x": round(x, 2), "z": round(z, 2), "yaw": round(yaw, 1), "near": len(others)}


# ------------------------------------------------------------------------------------------------ sockets
async def send(p, doc):
    try:
        await asyncio.wait_for(p.ws.send(json.dumps(doc, separators=(",", ":"))), 2.0)
        return True
    except Exception:                                             # noqa: BLE001
        return False


def drop(p):
    r = ROOMS.get(p.room) if p.room else None
    if r:
        r.players.pop(p.id, None)
        r.dirty = True
        for o in r.players.values():
            o.sent_sig = None
        if not r.players:
            ROOMS.pop(r.id, None)
    p.room = None
    n = IPS.get(p.ip, 1) - 1
    if n <= 0:
        IPS.pop(p.ip, None)
    else:
        IPS[p.ip] = n


def take_token(p):
    now = time.time()
    p.tokens = min(30.0, p.tokens + (now - p.t_tok) * 20.0)       # 20 msgs/s sustained, burst 30
    p.t_tok = now
    if p.tokens < 1.0:
        return False
    p.tokens -= 1.0
    return True


async def on_hello(p, m):
    now = time.time()
    if p.room is not None:
        return
    rid = str(m.get("room") or "").lower()
    if not ROOM_RE.match(rid):
        await send(p, {"t": "err", "code": "bad_room"})
        return
    room = ROOMS.get(rid)
    if room is None:
        room = ROOMS[rid] = Room(rid)
    if len(room.players) >= MAX_PER_ROOM:
        await send(p, {"t": "err", "code": "room_full"})
        COUNTERS["rejected"] += 1
        return
    area = m.get("area")
    if room.area is None and isinstance(area, list) and len(area) == 4:
        a = [fnum(v, -500, 500) for v in area]
        if a[2] - a[0] >= 2 and a[3] - a[1] >= 2:
            cx, cz = (a[0] + a[2]) / 2, (a[1] + a[3]) / 2
            hw, hd = min(MAX_AREA, a[2] - a[0]) / 2, min(MAX_AREA, a[3] - a[1]) / 2
            room.area = [cx - hw, cz - hd, cx + hw, cz + hd]
    run = str(m.get("run") or "")
    p.avatar = await resolve_avatar(run)
    p.run = run if p.avatar else ""
    p.active = bool(m.get("act", True))
    anchor = (fnum(m.get("ax")), fnum(m.get("az")))
    p.t_state = now
    sp = None
    if p.active:
        sp = ring_spawn(room, p, anchor)
        p.spawned = True
    else:
        x, z = clamp_area(room, *anchor)
        sp = {"x": round(x, 2), "z": round(z, 2), "yaw": 0.0, "near": 0}
    p.x, p.z, p.yaw = sp["x"], sp["z"], sp["yaw"]
    p.room = rid
    room.players[p.id] = p
    room.dirty = True
    COUNTERS["hello"] += 1
    await send(p, {"t": "welcome", "id": p.id, "room": rid, "spawn": sp, "hz": 10, "max_visible": MAX_VISIBLE,
                   "area": room.area, "n": len(room.players), "avatar": p.avatar is not None})


async def on_message(p, m):
    t = m.get("t")
    now = time.time()
    p.t_msg = now
    if t == "hello":
        if now - p.t_hello < 2.0:
            return
        p.t_hello = now
        await on_hello(p, m)
    elif t == "s" and p.room:
        x, z = clamp_area(ROOMS[p.room], fnum(m.get("x")), fnum(m.get("z")), 2.0)
        if math.hypot(x - p.x, z - p.z) > 0.4:
            p.moved = True
        p.x, p.y, p.z = x, fnum(m.get("y"), -200, 500), z
        p.yaw = fnum(m.get("r"), -720, 720)
        a = str(m.get("a") or "idle")
        p.anim = a if a in ANIMS else "other"
        p.speed = fnum(m.get("v"), 0, 20)
        p.t_state = now
        ROOMS[p.room].dirty = True
    elif t == "act" and p.room:
        on = bool(m.get("on"))
        was = p.active
        p.active = on
        p.t_state = now
        ROOMS[p.room].dirty = True
        if on and not was and not p.spawned and not p.moved:
            # the player was away when joining: now that they are here, place them next to the others
            sp = ring_spawn(ROOMS[p.room], p, (p.x, p.z))
            p.spawned = True
            if sp["near"]:
                p.x, p.z, p.yaw = sp["x"], sp["z"], sp["yaw"]
                await send(p, {"t": "spawn", **sp})
    elif t == "bye":
        raise ConnectionError("bye")


def snapshot_for(room, me, now):
    cand = [p for p in room.players.values() if p is not me and is_active(p, now)]
    cand.sort(key=lambda p: (p.x - me.x) ** 2 + (p.z - me.z) ** 2)
    out = []
    for p in cand[:MAX_VISIBLE]:
        o = {"id": p.id, "x": round(p.x, 2), "y": round(p.y, 2), "z": round(p.z, 2), "r": round(p.yaw, 1),
             "a": p.anim, "v": round(p.speed, 2)}
        if p.avatar:
            o["u"], o["h"], o["fw"] = p.avatar["url"], p.avatar["h"], p.avatar["fw"]
        out.append(o)
    return out


async def ticker():
    while True:
        await asyncio.sleep(TICK)
        now = time.time()
        for room in list(ROOMS.values()):
            if not room.dirty and now - room.last_send < 1.0:
                continue
            room.dirty = False
            keepalive = now - room.last_send >= 1.0
            room.last_send = now
            na = sum(1 for p in room.players.values() if is_active(p, now))
            for me in list(room.players.values()):
                peers = snapshot_for(room, me, now)
                sig = (tuple((o["id"], o["x"], o["y"], o["z"], o["r"], o["a"]) for o in peers), na, len(room.players))
                if sig == me.sent_sig and not keepalive:
                    continue
                me.sent_sig = sig
                await send(me, {"t": "snap", "peers": peers, "n": len(room.players), "na": na})


async def handler(ws):
    ip = ws.request.headers.get("X-Real-IP") or (ws.remote_address[0] if ws.remote_address else "?")
    if IPS.get(ip, 0) >= MAX_PER_IP:
        COUNTERS["rejected"] += 1
        await ws.close(1013, "busy")
        return
    IPS[ip] = IPS.get(ip, 0) + 1
    p = Player(ws, ip)
    COUNTERS["connections"] += 1
    try:
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), IDLE_CLOSE)
            except asyncio.TimeoutError:
                break
            if not isinstance(raw, str) or len(raw) > 1024:
                continue
            if not take_token(p):
                COUNTERS["rate_limited"] += 1
                continue
            try:
                m = json.loads(raw)
            except ValueError:
                continue
            if isinstance(m, dict):
                await on_message(p, m)
    except Exception:                                             # noqa: BLE001  (closed socket, bye)
        pass
    finally:
        drop(p)


# ------------------------------------------------------------------------------------------------ plain HTTP
def reply(status, doc):
    body = json.dumps(doc, separators=(",", ":")).encode()
    h = Headers([("Content-Type", "application/json"), ("Content-Length", str(len(body))),
                 ("Cache-Control", "no-store"), ("Access-Control-Allow-Origin", "*")])
    return Response(status, HTTPStatus(status).phrase, h, body)


async def process_request(connection, request):
    path, _, query = request.path.partition("?")
    path = path.rstrip("/") or "/"
    if path == "/api/rooms/ws":
        origin = request.headers.get("Origin")
        if origin not in ALLOWED_ORIGINS:
            return reply(403, {"error_string": "origin_not_allowed"})
        return None
    among = None
    for part in query.split("&"):
        if part.startswith("among="):
            among = {s for s in part[6:].split(",") if ROOM_RE.match(s)} or None
    if path == "/api/rooms":
        return reply(200, stats_doc(among))
    if path == "/api/rooms/busiest":
        d = stats_doc(among)
        return reply(200, {"busiest": d["busiest"], "default_room": d["default_room"],
                           "total_active": d["total_active"], "rooms": d["rooms"], "ts": d["ts"]})
    if path == "/api/rooms/health":
        return reply(200, {"ok": True, **COUNTERS, "rooms": len(ROOMS)})
    return reply(404, {"error_string": "not_found"})


async def main():
    global HTTP
    HTTP = httpx.AsyncClient(timeout=4.0)
    asyncio.get_event_loop().create_task(ticker())
    async with serve(handler, HOST, PORT, process_request=process_request, max_size=2048, ping_interval=20,
                     ping_timeout=20, compression=None, max_queue=4):
        print(f"rooms on {HOST}:{PORT}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
