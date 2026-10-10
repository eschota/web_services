"""Country (and US state) of an IP address from a local MaxMind-format database.

Used by the adult-site gate (site_mode.py) to keep adult content away from
jurisdictions whose law asks for an age check we do not offer. Pure Python, no
network and no third-party package: it reads any MaxMind DB (.mmdb) file, for
example DB-IP "IP to City Lite" (CC BY 4.0) or MaxMind GeoLite2-City.

The file lives outside Git at AUTORIG_GEOIP_MMDB (default
/srv/autorig/data/geoip/country.mmdb). Without it every lookup answers None and
the gate treats the country as unknown (see site_mode geo.unknown_country).
"""
from __future__ import annotations

import ipaddress
import os
import struct
import threading
from typing import Any, Dict, Optional, Tuple

DEFAULT_DB_PATH = "/srv/autorig/data/geoip/country.mmdb"
_METADATA_MARKER = b"\xab\xcd\xefMaxMind.com"

# DB-IP Lite ships US subdivisions by English name only; GeoLite2 also has iso_code.
US_STATE_CODES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "district of columbia": "DC",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID", "illinois": "IL",
    "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
    "maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA",
    "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "puerto rico": "PR", "guam": "GU",
}


class MMDBError(ValueError):
    pass


class MMDBReader:
    """Minimal reader of the MaxMind DB format 2.0 (search tree + data section)."""

    def __init__(self, data: bytes):
        self._buf = data
        at = data.rfind(_METADATA_MARKER)
        if at < 0:
            raise MMDBError("no MaxMind metadata marker")
        meta_start = at + len(_METADATA_MARKER)
        meta, _ = self._decode(meta_start, meta_start)
        if not isinstance(meta, dict):
            raise MMDBError("metadata is not a map")
        self.metadata = meta
        self.node_count = int(meta["node_count"])
        self.record_size = int(meta["record_size"])
        self.ip_version = int(meta["ip_version"])
        if self.record_size not in (24, 28, 32):
            raise MMDBError(f"unsupported record size {self.record_size}")
        self._node_bytes = self.record_size * 2 // 8
        self._tree_size = self._node_bytes * self.node_count
        self._data_start = self._tree_size + 16
        self._ipv4_start = 0
        if self.ip_version == 6:
            node = 0
            for _ in range(96):
                if node >= self.node_count:
                    break
                node = self._record(node, 0)
            self._ipv4_start = node

    # ------------------------------------------------------------- search tree
    def _record(self, node: int, bit: int) -> int:
        off = node * self._node_bytes
        b = self._buf
        if self.record_size == 24:
            o = off + bit * 3
            return (b[o] << 16) | (b[o + 1] << 8) | b[o + 2]
        if self.record_size == 28:
            if bit == 0:
                return ((b[off + 3] & 0xF0) << 20) | (b[off] << 16) | (b[off + 1] << 8) | b[off + 2]
            return ((b[off + 3] & 0x0F) << 24) | (b[off + 4] << 16) | (b[off + 5] << 8) | b[off + 6]
        o = off + bit * 4
        return struct.unpack(">I", b[o:o + 4])[0]

    def lookup(self, ip: str) -> Optional[Any]:
        addr = ipaddress.ip_address(ip)
        if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
            addr = addr.ipv4_mapped
        packed = addr.packed
        if addr.version == 6 and self.ip_version == 4:
            return None
        node = self._ipv4_start if addr.version == 4 else 0
        bits = len(packed) * 8
        for i in range(bits):
            if node >= self.node_count:
                break
            bit = (packed[i >> 3] >> (7 - (i & 7))) & 1
            node = self._record(node, bit)
        if node == self.node_count:
            return None
        if node < self.node_count:
            raise MMDBError("search tree did not end in a record")
        offset = self._tree_size + (node - self.node_count)
        value, _ = self._decode(offset, self._data_start)
        return value

    # ------------------------------------------------------------ data section
    def _decode(self, off: int, base: int) -> Tuple[Any, int]:
        b = self._buf
        ctrl = b[off]
        off += 1
        typ = ctrl >> 5
        if typ == 1:                                    # pointer
            ss = (ctrl >> 3) & 0x3
            vvv = ctrl & 0x7
            if ss == 0:
                ptr, off = (vvv << 8) | b[off], off + 1
            elif ss == 1:
                ptr, off = ((vvv << 16) | (b[off] << 8) | b[off + 1]) + 2048, off + 2
            elif ss == 2:
                ptr, off = ((vvv << 24) | (b[off] << 16) | (b[off + 1] << 8) | b[off + 2]) + 526336, off + 3
            else:
                ptr, off = struct.unpack(">I", b[off:off + 4])[0], off + 4
            value, _ = self._decode(base + ptr, base)
            return value, off
        if typ == 0:                                    # extended type
            typ = 7 + b[off]
            off += 1
        size = ctrl & 0x1F
        if size == 29:
            size, off = 29 + b[off], off + 1
        elif size == 30:
            size, off = 285 + ((b[off] << 8) | b[off + 1]), off + 2
        elif size == 31:
            size, off = 65821 + ((b[off] << 16) | (b[off + 1] << 8) | b[off + 2]), off + 3
        if typ == 2:
            return b[off:off + size].decode("utf-8", "replace"), off + size
        if typ == 3:
            return struct.unpack(">d", b[off:off + 8])[0], off + 8
        if typ == 4:
            return bytes(b[off:off + size]), off + size
        if typ in (5, 6, 9, 10):
            return int.from_bytes(b[off:off + size], "big") if size else 0, off + size
        if typ == 8:
            raw = b[off:off + size].rjust(4, b"\x00")
            return struct.unpack(">i", raw)[0], off + size
        if typ == 7:
            out: Dict[Any, Any] = {}
            for _ in range(size):
                key, off = self._decode(off, base)
                val, off = self._decode(off, base)
                out[key] = val
            return out, off
        if typ == 11:
            arr = []
            for _ in range(size):
                val, off = self._decode(off, base)
                arr.append(val)
            return arr, off
        if typ == 14:
            return bool(size), off
        if typ == 15:
            return struct.unpack(">f", b[off:off + 4])[0], off + 4
        if typ in (12, 13):
            return None, off
        raise MMDBError(f"unknown data type {typ}")


_LOCK = threading.Lock()
_READER: Dict[str, Tuple[float, Optional[MMDBReader]]] = {}


def db_path() -> str:
    return os.environ.get("AUTORIG_GEOIP_MMDB", "").strip() or DEFAULT_DB_PATH


def _reader(path: Optional[str] = None) -> Optional[MMDBReader]:
    path = path or db_path()
    try:
        mtime = os.stat(path).st_mtime
    except OSError:
        return None
    with _LOCK:
        hit = _READER.get(path)
        if hit and hit[0] == mtime:
            return hit[1]
        try:
            with open(path, "rb") as fh:
                reader: Optional[MMDBReader] = MMDBReader(fh.read())
        except (OSError, MMDBError, KeyError, IndexError, ValueError) as exc:
            print(f"[geo] cannot read {path}: {type(exc).__name__}: {exc}")
            reader = None
        _READER[path] = (mtime, reader)
        return reader


def database_info(path: Optional[str] = None) -> Dict[str, Any]:
    reader = _reader(path)
    if reader is None:
        return {"loaded_bool": False, "path_string": path or db_path()}
    meta = reader.metadata
    return {"loaded_bool": True, "path_string": path or db_path(),
            "database_type_string": str(meta.get("database_type") or ""),
            "build_epoch_int": int(meta.get("build_epoch") or 0)}


def locate(ip: Optional[str], path: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """(country ISO alpha-2, subdivision code) for an IP, or (None, None).

    The subdivision code is the ISO 3166-2 suffix when the database has it,
    else the US state code from its English name, else None."""
    if not ip:
        return None, None
    reader = _reader(path)
    if reader is None:
        return None, None
    try:
        rec = reader.lookup(ip.strip())
    except (ValueError, MMDBError, IndexError, struct.error):
        return None, None
    if not isinstance(rec, dict):
        return None, None
    country = None
    for key in ("country", "registered_country"):
        node = rec.get(key)
        if isinstance(node, dict) and node.get("iso_code"):
            country = str(node["iso_code"]).upper()
            break
    region = None
    subs = rec.get("subdivisions")
    if isinstance(subs, list) and subs and isinstance(subs[0], dict):
        first = subs[0]
        if first.get("iso_code"):
            region = str(first["iso_code"]).upper()
        elif country == "US":
            names = first.get("names") if isinstance(first.get("names"), dict) else {}
            region = US_STATE_CODES.get(str(names.get("en") or "").strip().lower())
    return country, region
