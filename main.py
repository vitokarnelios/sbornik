#!/usr/bin/env python3
"""
SING-BOX VLESS BULK TESTER
- 7 источников из sources.txt
- TCP precheck перед запуском sing-box
- 5 тестовых сайтов (telegram, instagram, youtube, gemini, google)
- LIVE = минимум 3 из 5 сервисов
- MAX_NODES = 100 живых нод
- Живые ноды: subs/vless_001.txt
- Архив живых: subs/live_archive.txt
- Статистика: stats/*.json
- Без SNI-мутаций
- Без приоритетов по архиву
"""

import os
import requests
import base64
import json
import subprocess
import time
import threading
import logging
import hashlib
import socket

from collections import defaultdict
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# PATHS
# ============================================================

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

SUBS_DIR = os.path.join(BASE_PATH, "subs")
LOG_DIR = os.path.join(BASE_PATH, "logs")
STATS_DIR = os.path.join(BASE_PATH, "stats")

SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")

SUBSCRIPTION_FILE = os.path.join(SUBS_DIR, "vless_001.txt")
LIVE_ARCHIVE_FILE = os.path.join(SUBS_DIR, "live_archive.txt")

NODE_STATS_FILE = os.path.join(STATS_DIR, "node_stats.json")
SOURCE_STATS_FILE = os.path.join(STATS_DIR, "source_stats.json")
SERVICE_STATS_FILE = os.path.join(STATS_DIR, "service_stats.json")
RUN_STATS_FILE = os.path.join(STATS_DIR, "run_stats.json")

for d in (SUBS_DIR, LOG_DIR, STATS_DIR):
    os.makedirs(d, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

log_file = os.path.join(
    LOG_DIR,
    f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


# ============================================================
# CONFIG
# ============================================================

MAX_NODES = 100
MAX_THREADS = 40

SOURCE_TIMEOUT = 15
SERVICE_TIMEOUT = 4
TCP_TIMEOUT = 2.0
SINGBOX_START_WAIT = 1.0

NEXT_PORT = 11000
MAX_PORT = 15000

MIN_SUCCESS_SERVICES = 3

GOOD_CODES = {200, 204, 301, 302}


# ============================================================
# SERVICES
# ============================================================

SERVICES = {
    "telegram": "https://telegram.org",
    "instagram": "https://www.instagram.com",
    "youtube": "https://www.youtube.com",
    "gemini": "https://gemini.google.com",
    "google": "https://www.google.com",
}


# ============================================================
# SOURCES
# ============================================================

if not os.path.exists(SOURCES_FILE):
    logger.error("sources.txt не найден")
    raise SystemExit(1)

with open(SOURCES_FILE, "r", encoding="utf-8") as f:
    SOURCES = [
        line.strip()
        for line in f
        if line.strip() and not line.strip().startswith("#")
    ]


# ============================================================
# GLOBAL STATE
# ============================================================

STATS_LOCK = threading.Lock()
PORT_LOCK = threading.Lock()

NODE_RESULTS = {}

RUN_STATS = {
    "run": 0,
    "vless_found": 0,
    "unique_nodes": 0,
    "checked": 0,
    "live": 0,
    "dead": 0,
    "tcp_failed": 0,
    "service_attempts": 0,
    "service_success": 0,
    "elapsed": 0.0,
}


# ============================================================
# JSON HELPERS
# ============================================================

def load_json(path, default_factory):
    if not os.path.exists(path):
        return default_factory()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else default_factory()
    except Exception as e:
        logger.warning(f"Ошибка загрузки {os.path.basename(path)}: {e}")
        return default_factory()


def save_json(path, data):
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f"Ошибка сохранения {os.path.basename(path)}: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def empty_node_stats():
    return {}


def empty_source_stats():
    return {}


def empty_service_stats():
    return {
        name: {"attempts": 0, "success": 0, "failed": 0}
        for name in SERVICES
    }


def empty_run_stats():
    return {
        "runs": 0,
        "totals": {
            "vless_found": 0,
            "unique_nodes": 0,
            "checked": 0,
            "live": 0,
            "dead": 0,
            "tcp_failed": 0,
            "service_attempts": 0,
            "service_success": 0,
            "service_failed": 0,
            "archive_checked": 0,
            "archive_live": 0,
            "archive_dead": 0,
        },
        "history": [],
    }


NODE_STATS = load_json(NODE_STATS_FILE, empty_node_stats)
SOURCE_STATS = load_json(SOURCE_STATS_FILE, empty_source_stats)
SERVICE_STATS = load_json(SERVICE_STATS_FILE, empty_service_stats)
RUN_STATS_FILE_DATA = load_json(RUN_STATS_FILE, empty_run_stats)


# ============================================================
# HELPERS
# ============================================================

def node_id(uri):
    return hashlib.sha256(
        uri.strip().encode("utf-8", errors="ignore")
    ).hexdigest()[:16]


def percent(a, b):
    return round(a / b * 100, 2) if b else 0.0


def ensure_source_bucket(source):
    return SOURCE_STATS.setdefault(source, {
        "downloads_ok": 0,
        "downloads_failed": 0,
        "found": 0,
        "tested": 0,
        "live": 0,
        "dead": 0,
    })


def ensure_node_record(uri):
    nid = node_id(uri)
    rec = NODE_STATS.setdefault(nid, {
        "attempts": 0,
        "live": 0,
        "dead": 0,
        "last_status": "unknown",
        "sources": [],
        "first_seen": "",
        "last_seen": "",
    })
    return nid, rec


def record_node_source(uri, sources):
    nid, rec = ensure_node_record(uri)
    now = datetime.now().isoformat(timespec="seconds")

    if not rec.get("first_seen"):
        rec["first_seen"] = now
    rec["last_seen"] = now

    srcs = rec.setdefault("sources", [])
    for s in sources:
        if s not in srcs:
            srcs.append(s)
    if len(srcs) > 30:
        del srcs[:-30]

    return nid, rec


# ============================================================
# PORTS
# ============================================================

_next_port = NEXT_PORT


def allocate_port():
    global _next_port
    with PORT_LOCK:
        start = _next_port
        while True:
            port = _next_port
            _next_port += 1
            if _next_port > MAX_PORT:
                _next_port = NEXT_PORT

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                pass
            finally:
                s.close()

            if _next_port == start:
                raise RuntimeError("No free ports")


# ============================================================
# TCP PRECHECK
# ============================================================

def tcp_precheck(uri):
    try:
        p = urlparse(uri)
        host = p.hostname
        port = p.port
        if not host or not port:
            return False, "missing host/port"

        with socket.create_connection((host, port), timeout=TCP_TIMEOUT):
            pass
        return True, None
    except Exception as e:
        return False, str(e)


# ============================================================
# SOURCES
# ============================================================

def decode_base64_content(text):
    try:
        text = text.strip()
        if "://" in text:
            return [text]

        padding = len(text) % 4
        if padding:
            text += "=" * (4 - padding)

        decoded = base64.b64decode(text).decode(
            "utf-8", errors="ignore"
        )
        return decoded.splitlines()
    except Exception:
        return []


def fetch_source(url):
    try:
        r = requests.get(
            url,
            timeout=SOURCE_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if r.status_code != 200:
            return []

        result = []
        for line in r.text.splitlines():
            line = line.strip()
            if not line:
                continue
            if "://" not in line and len(line) > 50:
                result.extend(decode_base64_content(line))
            else:
                result.append(line)
        return result
    except Exception as e:
        logger.debug(f"Ошибка загрузки {url}: {e}")
        return []


def is_valid_vless(line):
    return line.lower().startswith("vless://")


# ============================================================
# VLESS -> SING-BOX JSON
# ============================================================

def parse_vless_to_json(uri, listen_port):
    try:
        parsed = urlparse(uri)
        netloc = parsed.netloc

        if "@" not in netloc:
            return None

        uuid, server_part = netloc.split("@", 1)

        if ":" in server_part:
            server_address, server_port_str = server_part.split(":", 1)
            server_port = int(server_port_str)
        else:
            server_address = server_part
            server_port = 443

        params_q = parse_qs(parsed.query)
        params = {k.lower(): v[0] for k, v in params_q.items() if v}

        node_name = unquote(parsed.fragment) if parsed.fragment else "vless-node"

        outbound = {
            "type": "vless",
            "tag": node_name,
            "server": server_address,
            "server_port": server_port,
            "uuid": uuid,
        }

        flow = params.get("flow", "")
        if flow:
            outbound["flow"] = flow

        security = params.get("security", "")
        transport_type = params.get("type", "tcp")

        # REALITY
        if (
            "pbk" in params
            or params.get("security") == "reality"
            or security == "reality"
        ):
            outbound["tls"] = {
                "enabled": True,
                "server_name": params.get("sni", server_address),
                "utls": {
                    "enabled": True,
                    "fingerprint": params.get("fp", "chrome"),
                },
                "reality": {
                    "enabled": True,
                    "public_key": params.get("pbk", ""),
                    "short_id": params.get("sid", ""),
                },
            }

        # TLS
        elif security == "tls" or params.get("security") == "tls":
            outbound["tls"] = {
                "enabled": True,
                "server_name": params.get("sni", server_address),
                "utls": {
                    "enabled": True,
                    "fingerprint": params.get("fp", "chrome"),
                },
            }
            if "alpn" in params:
                outbound["tls"]["alpn"] = params["alpn"].split(",")

        # TRANSPORT
        if transport_type == "grpc":
            outbound["transport"] = {
                "type": "grpc",
                "service_name": params.get("serviceName", ""),
            }

        elif transport_type == "xhttp":
            outbound["transport"] = {
                "type": "xhttp",
                "path": params.get("path", "/"),
                "host": [params["host"]] if params.get("host") else [],
                "mode": params.get("mode", "auto"),
            }

        elif transport_type == "ws":
            outbound["transport"] = {
                "type": "ws",
                "path": params.get("path", "/"),
                "headers": {
                    "Host": params.get("host", server_address),
                },
            }

        return {
            "log": {"level": "error"},
            "inbounds": [
                {
                    "type": "socks",
                    "tag": "socks-in",
                    "listen": "127.0.0.1",
                    "listen_port": listen_port,
                }
            ],
            "outbounds": [outbound],
        }
    except Exception:
        return None


# ============================================================
# SING-BOX PROCESS
# ============================================================

def start_singbox(config, config_path):
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)

    try:
        process = subprocess.Popen(
            ["sing-box", "run", "-c", config_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
    except Exception as e:
        return None, str(e)

    time.sleep(SINGBOX_START_WAIT)

    if process.poll() is not None:
        try:
            err = process.stderr.read()
        except Exception:
            err = ""
        return None, err.strip() or f"sing-box exited with code {process.returncode}"

    return process, None


def stop_singbox(process):
    if not process:
        return
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
    except Exception:
        pass


# ============================================================
# SERVICE TEST
# ============================================================

def test_service(proxy_port, url):
    proxies = {
        "http": f"socks5h://127.0.0.1:{proxy_port}",
        "https": f"socks5h://127.0.0.1:{proxy_port}",
    }
    try:
        r = requests.get(
            url,
            proxies=proxies,
            timeout=SERVICE_TIMEOUT,
            allow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/154.0.0.0 Safari/537.36"
                )
            },
        )
        return r.status_code in GOOD_CODES, r.status_code
    except Exception as e:
        return False, str(e)


def run_services(proxy_port):
    results = {}
    success_count = 0

    for name, url in SERVICES.items():
        ok, code = test_service(proxy_port, url)
        results[name] = {"ok": ok, "result": code}
        if ok:
            success_count += 1

        with STATS_LOCK:
            item = SERVICE_STATS.setdefault(
                name, {"attempts": 0, "success": 0, "failed": 0}
            )
            item["attempts"] += 1
            if ok:
                item["success"] += 1
            else:
                item["failed"] += 1

            RUN_STATS["service_attempts"] += 1
            if ok:
                RUN_STATS["service_success"] += 1

    return results, success_count


# ============================================================
# ONE NODE
# ============================================================

def check_one(uri, index, total, sources_for_node):
    nid, node_rec = record_node_source(uri, sources_for_node)

    with STATS_LOCK:
        node_rec["attempts"] += 1
        node_rec["last_status"] = "testing"
        RUN_STATS["checked"] += 1

    # TCP PRECHECK
    tcp_ok, tcp_error = tcp_precheck(uri)

    if not tcp_ok:
        with STATS_LOCK:
            RUN_STATS["dead"] += 1
            RUN_STATS["tcp_failed"] += 1
            node_rec["dead"] += 1
            node_rec["last_status"] = "tcp_dead"
            for src in sources_for_node:
                b = ensure_source_bucket(src)
                b["tested"] += 1
                b["dead"] += 1

        NODE_RESULTS[nid] = {
            "uri": uri,
            "live": False,
            "service_success": 0,
            "error": f"tcp: {tcp_error}",
        }

        print(f"[{index}/{total}] TCP DEAD | {nid} | {tcp_error}")
        return

    # PORT
    try:
        local_port = allocate_port()
    except Exception as e:
        with STATS_LOCK:
            RUN_STATS["dead"] += 1
            node_rec["dead"] += 1
            node_rec["last_status"] = "port_fail"
            for src in sources_for_node:
                b = ensure_source_bucket(src)
                b["tested"] += 1
                b["dead"] += 1

        NODE_RESULTS[nid] = {
            "uri": uri,
            "live": False,
            "service_success": 0,
            "error": f"port: {e}",
        }
        print(f"[{index}/{total}] PORT DEAD | {nid} | {e}")
        return

    # CONFIG
    config = parse_vless_to_json(uri, local_port)
    if not config:
        with STATS_LOCK:
            RUN_STATS["dead"] += 1
            node_rec["dead"] += 1
            node_rec["last_status"] = "config_fail"
            for src in sources_for_node:
                b = ensure_source_bucket(src)
                b["tested"] += 1
                b["dead"] += 1

        NODE_RESULTS[nid] = {
            "uri": uri,
            "live": False,
            "service_success": 0,
            "error": "config parse failed",
        }
        print(f"[{index}/{total}] CONFIG DEAD | {nid}")
        return

    config_path = os.path.join(LOG_DIR, f"singbox_{nid}_{local_port}.json")

    process = None
    service_results = {}
    service_success = 0
    live = False

    try:
        process, start_err = start_singbox(config, config_path)

        if process is None:
            with STATS_LOCK:
                RUN_STATS["dead"] += 1
                node_rec["dead"] += 1
                node_rec["last_status"] = "start_fail"
                for src in sources_for_node:
                    b = ensure_source_bucket(src)
                    b["tested"] += 1
                    b["dead"] += 1

            NODE_RESULTS[nid] = {
                "uri": uri,
                "live": False,
                "service_success": 0,
                "error": f"start: {start_err}",
            }
            print(f"[{index}/{total}] START DEAD | {nid} | {start_err}")
            return

        service_results, service_success = run_services(local_port)
        live = service_success >= MIN_SUCCESS_SERVICES

        with STATS_LOCK:
            if live:
                RUN_STATS["live"] += 1
                node_rec["live"] += 1
                node_rec["last_status"] = "live"
                for src in sources_for_node:
                    b = ensure_source_bucket(src)
                    b["tested"] += 1
                    b["live"] += 1
            else:
                RUN_STATS["dead"] += 1
                node_rec["dead"] += 1
                node_rec["last_status"] = "dead"
                for src in sources_for_node:
                    b = ensure_source_bucket(src)
                    b["tested"] += 1
                    b["dead"] += 1

        NODE_RESULTS[nid] = {
            "uri": uri,
            "live": live,
            "service_success": service_success,
            "service_results": service_results,
            "error": None,
        }

        status = "LIVE" if live else "DEAD"
        print(
            f"[{index}/{total}] {status} | {nid} | "
            f"{service_success}/{len(SERVICES)}"
        )

    except Exception as e:
        logger.exception(f"check_one failed for {uri}")
        with STATS_LOCK:
            RUN_STATS["dead"] += 1
            node_rec["dead"] += 1
            node_rec["last_status"] = "exception"
            for src in sources_for_node:
                b = ensure_source_bucket(src)
                b["tested"] += 1
                b["dead"] += 1

        NODE_RESULTS[nid] = {
            "uri": uri,
            "live": False,
            "service_success": 0,
            "error": f"exception: {e}",
        }
        print(f"[{index}/{total}] EXCEPTION DEAD | {nid} | {e}")

    finally:
        stop_singbox(process)
        if os.path.exists(config_path):
            try:
                os.remove(config_path)
            except Exception:
                pass


# ============================================================
# ARCHIVE
# ============================================================

def load_archive():
    if not os.path.exists(LIVE_ARCHIVE_FILE):
        return []

    try:
        with open(LIVE_ARCHIVE_FILE, "r", encoding="utf-8") as f:
            lines = [x.strip() for x in f if x.strip()]
    except Exception as e:
        logger.warning(f"Ошибка чтения live_archive.txt: {e}")
        return []

    unique = []
    seen = set()
    for line in lines:
        if not line.lower().startswith("vless://"):
            continue
        nid = node_id(line)
        if nid in seen:
            continue
        seen.add(nid)
        unique.append(line)
    return unique


def save_unique_vless(path, lines):
    unique = []
    seen = set()

    for line in lines:
        line = line.strip()
        if not line.lower().startswith("vless://"):
            continue
        nid = node_id(line)
        if nid in seen:
            continue
        seen.add(nid)
        unique.append(line)

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for line in unique:
            f.write(line + "\n")
    os.replace(tmp, path)

    return unique


# ============================================================
# PRINT
# ============================================================

def print_source_stats():
    print()
    print("=" * 80)
    print("=== SOURCES — CUMULATIVE ===")
    print("=" * 80)

    for source, item in SOURCE_STATS.items():
        rate = percent(item.get("live", 0), item.get("tested", 0))
        print(
            f"{source}\n"
            f"  ok={item.get('downloads_ok', 0)} "
            f"err={item.get('downloads_failed', 0)} "
            f"found={item.get('found', 0)} "
            f"tested={item.get('tested', 0)} "
            f"live={item.get('live', 0)} "
            f"dead={item.get('dead', 0)} "
            f"rate={rate:.2f}%"
        )


def print_service_stats():
    print()
    print("=" * 80)
    print("=== SERVICES — CUMULATIVE ===")
    print("=" * 80)

    for name, item in SERVICE_STATS.items():
        rate = percent(item.get("success", 0), item.get("attempts", 0))
        print(
            f"{name:<12} "
            f"attempts={item.get('attempts', 0):<7} "
            f"success={item.get('success', 0):<7} "
            f"failed={item.get('failed', 0):<7} "
            f"{rate:6.2f}%"
        )


def print_totals():
    totals = RUN_STATS_FILE_DATA["totals"]

    print()
    print("=" * 80)
    print("=== ALL-TIME TOTALS ===")
    print("=" * 80)
    print(f"Runs:              {RUN_STATS_FILE_DATA['runs']}")
    print(f"VLESS found:       {totals['vless_found']}")
    print(f"Unique nodes:      {totals['unique_nodes']}")
    print(f"Checked:           {totals['checked']}")
    print(f"LIVE:              {totals['live']}")
    print(f"DEAD:              {totals['dead']}")
    print(f"TCP failed:        {totals['tcp_failed']}")
    print(f"Service attempts:  {totals['service_attempts']}")
    print(f"Service success:   {totals['service_success']}")
    print(f"Service failed:    {totals['service_failed']}")
    print(f"Archive checked:   {totals['archive_checked']}")
    print(f"Archive LIVE:      {totals['archive_live']}")
    print(f"Archive DEAD:      {totals['archive_dead']}")


# ============================================================
# TOTALS UPDATE
# ============================================================

def update_totals(run_stats, archive_checked, archive_live, archive_dead):
    RUN_STATS_FILE_DATA["runs"] += 1
    t = RUN_STATS_FILE_DATA["totals"]

    for key in (
        "vless_found",
        "unique_nodes",
        "checked",
        "live",
        "dead",
        "tcp_failed",
        "service_attempts",
        "service_success",
    ):
        t[key] += run_stats.get(key, 0)

    t["service_failed"] += (
        run_stats["service_attempts"] - run_stats["service_success"]
    )
    t["archive_checked"] += archive_checked
    t["archive_live"] += archive_live
    t["archive_dead"] += archive_dead


# ============================================================
# MAIN
# ============================================================

def main():
    start_time = time.time()

    # check sing-box
    try:
        v = subprocess.run(
            ["sing-box", "version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        print(v.stdout.strip())
    except Exception as e:
        logger.error(f"sing-box не найден: {e}")
        return

    print()
    print("=" * 70)
    print("=== SING-BOX VLESS BULK TESTER ===")
    print("=" * 70)

    run_number = RUN_STATS_FILE_DATA["runs"] + 1

    print(f"Run: {run_number}")
    print(f"Threads: {MAX_THREADS}")
    print(
        f"Services: {len(SERVICES)} "
        f"(LIVE = {MIN_SUCCESS_SERVICES}/{len(SERVICES)})"
    )
    print(f"Max nodes: {MAX_NODES}")
    print(f"TCP precheck timeout: {TCP_TIMEOUT}s")
    print(f"Sources: {len(SOURCES)}")

    # --------------------------------------------------------
    # FETCH SOURCES
    # --------------------------------------------------------

    print()
    print("=== DOWNLOADING SOURCES ===")

    source_nodes = defaultdict(set)
    unique_nodes = {}
    node_to_sources = defaultdict(set)

    total_found = 0

    for source in SOURCES:
        nodes = fetch_source(source)

        if nodes:
            bucket = ensure_source_bucket(source)
            bucket["downloads_ok"] += 1
        else:
            bucket = ensure_source_bucket(source)
            bucket["downloads_failed"] += 1
            print(f"ERROR: {source}")
            continue

        ids = set()
        for line in nodes:
            if not is_valid_vless(line):
                continue
            nid = node_id(line)
            if nid in ids:
                continue
            ids.add(nid)
            unique_nodes.setdefault(nid, line.strip())
            node_to_sources[nid].add(source)

        source_nodes[source] = ids
        bucket["found"] += len(ids)
        total_found += len(ids)

        print(f"OK: {source}\n  VLESS found: {len(ids)}")

    RUN_STATS["vless_found"] = total_found
    RUN_STATS["unique_nodes"] = len(unique_nodes)

    print()
    print(f"VLESS found: {total_found}")
    print(f"Unique nodes: {len(unique_nodes)}")

    if not unique_nodes:
        print("Нет нод для тестирования.")
        update_totals(RUN_STATS, 0, 0, 0)
        save_json(RUN_STATS_FILE, RUN_STATS_FILE_DATA)
        save_json(NODE_STATS_FILE, NODE_STATS)
        save_json(SOURCE_STATS_FILE, SOURCE_STATS)
        save_json(SERVICE_STATS_FILE, SERVICE_STATS)
        return

    # --------------------------------------------------------
    # TEST
    # --------------------------------------------------------

    current_nodes = list(unique_nodes.items())

    print()
    print("=" * 70)
    print(f"=== TESTING {len(current_nodes)} NODES ===")
    print("=" * 70)

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
        futures = {}
        for index, (nid, uri) in enumerate(current_nodes, start=1):
            futures[ex.submit(
                check_one,
                uri,
                index,
                len(current_nodes),
                sorted(node_to_sources[nid]),
            )] = nid

        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception:
                logger.exception(f"Worker failed: {futures[fut]}")

    # --------------------------------------------------------
    # LIVE
    # --------------------------------------------------------

    current_live = [
        r["uri"] for r in NODE_RESULTS.values() if r.get("live")
    ]
    current_live = list(dict.fromkeys(current_live))

    print()
    print(f"Fresh LIVE nodes: {len(current_live)}")

    # --------------------------------------------------------
    # ARCHIVE + SUBSCRIPTION
    # --------------------------------------------------------

    archive_before = load_archive()
    archive_by_id = {node_id(u): u for u in archive_before}

    for uri in current_live:
        archive_by_id[node_id(uri)] = uri

    archive_checked = archive_live = archive_dead = 0
    selected_nodes = list(current_live)

    # добор из архива до MAX_NODES
    need = max(0, MAX_NODES - len(selected_nodes))

    if need > 0:
        print()
        print("=" * 70)
        print(f"=== ARCHIVE FILL: NEED {need} ===")
        print("=" * 70)

        selected_ids = {node_id(x) for x in selected_nodes}
        current_source_ids = set(unique_nodes.keys())

        candidates = [
            (nid, uri) for nid, uri in archive_by_id.items()
            if nid not in current_source_ids and nid not in selected_ids
        ]

        print(f"Archive candidates: {len(candidates)}")

        for i, (nid, uri) in enumerate(candidates, start=1):
            if len(selected_nodes) >= MAX_NODES:
                break

            check_one(
                uri,
                i,
                len(candidates),
                sources_for_node=[],
            )

            archive_checked += 1
            r = NODE_RESULTS.get(nid)

            if r and r.get("live"):
                archive_live += 1
                selected_nodes.append(uri)
            else:
                archive_dead += 1
                archive_by_id.pop(nid, None)

        print()
        print(f"Archive checked: {archive_checked}")
        print(f"Archive LIVE:    {archive_live}")
        print(f"Archive DEAD:    {archive_dead}")
        print(f"Subscription:    {len(selected_nodes)}")
    else:
        print()
        print("Fresh LIVE >= target. Archive fill skipped.")

    save_unique_vless(
        LIVE_ARCHIVE_FILE,
        list(archive_by_id.values()),
    )

    selected_nodes = list(dict.fromkeys(selected_nodes))
    if len(selected_nodes) > MAX_NODES:
        selected_nodes = selected_nodes[:MAX_NODES]

    subscription_nodes = save_unique_vless(
        SUBSCRIPTION_FILE,
        selected_nodes,
    )

    # --------------------------------------------------------
    # TOTALS + SAVE
    # --------------------------------------------------------

    RUN_STATS["elapsed"] = time.time() - start_time

    update_totals(RUN_STATS, archive_checked, archive_live, archive_dead)

    RUN_STATS_FILE_DATA["history"].append({
        "run": run_number,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "vless_found": RUN_STATS["vless_found"],
        "unique_nodes": RUN_STATS["unique_nodes"],
        "checked":
