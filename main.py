#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GITHUB MAIN — SING-BOX VLESS TESTER

Логика:
- источники берутся ТОЛЬКО из sources.txt
- никаких зашитых списков источников
- sing-box остаётся движком
- SNI mutation полностью отключена
- SNI используется только как штатный параметр VLESS TLS/Reality
- 5 сервисов
- LIVE = минимум 3 успешных сервиса из 5
- сначала проверяются все свежие ноды из sources.txt
- затем, если LIVE < 100, перепроверяется live_archive.txt
- мёртвые архивные ноды удаляются
- архив содержит только ноды, которые когда-либо были подтверждены LIVE
- архив не ограничен по размеру
- единственная подписка: subs/vless_001.txt
- единственный архив: subs/live_archive.txt
- одинаковый URI проверяется один раз за запуск
- принадлежность ноды к источникам сохраняется
- TCP precheck перед запуском sing-box
"""

import base64
import hashlib
import json
import logging
import os
import queue
import shutil
import socket
import subprocess
import tempfile
import threading
import time

from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, unquote, urlparse

import requests


# ============================================================
# PATHS
# ============================================================

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")

SUBS_DIR = os.path.join(BASE_PATH, "subs")
LOGS_DIR = os.path.join(BASE_PATH, "logs")
STATS_DIR = os.path.join(BASE_PATH, "stats")
TEMP_DIR = os.path.join(BASE_PATH, "temp")

SUBSCRIPTION_FILE = os.path.join(SUBS_DIR, "vless_001.txt")
ARCHIVE_FILE = os.path.join(SUBS_DIR, "live_archive.txt")

STATS_FILE = os.path.join(STATS_DIR, "stats.json")
ERROR_LOG = os.path.join(LOGS_DIR, "singbox_errors.log")


# ============================================================
# SETTINGS
# ============================================================

SINGBOX_BIN = os.environ.get("SINGBOX_BIN", "sing-box")

MAX_NODES = 100
MAX_THREADS = 40

SOURCE_TIMEOUT = 15
SERVICE_TIMEOUT = 4
TCP_TIMEOUT = 2.0
SINGBOX_START_WAIT = 1.2

PORT_START = 20000
PORT_END = 55000

GOOD_CODES = {200, 204, 301, 302}

TEST_URLS = [
    "https://telegram.org",
    "https://www.instagram.com",
    "https://www.youtube.com",
    "https://gemini.google.com",
    "https://www.google.com",
]


# ============================================================
# DIRECTORIES
# ============================================================

for directory in (
    SUBS_DIR,
    LOGS_DIR,
    STATS_DIR,
    TEMP_DIR,
):
    os.makedirs(directory, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("main")


# ============================================================
# PORT POOL
# ============================================================

PORT_QUEUE = queue.Queue()

for port in range(PORT_START, PORT_START + MAX_THREADS):
    PORT_QUEUE.put(port)


# ============================================================
# STATS LOCK
# ============================================================

STATS_LOCK = threading.Lock()


# ============================================================
# STATS
# ============================================================

def make_dimension_bucket():
    return {
        "tested": 0,
        "live": 0,
        "dead": 0,
    }


def make_source_bucket():
    return {
        "downloads_ok": 0,
        "downloads_failed": 0,
        "found": 0,
        "tested": 0,
        "live": 0,
        "dead": 0,
    }


def make_service_bucket():
    return {
        "attempts": 0,
        "success": 0,
        "failed": 0,
    }


def default_stats():
    return {
        "version": 3,

        "sources": {},

        "protocols": {},

        "transports": {},

        "security": {},

        "services": {
            url: make_service_bucket()
            for url in TEST_URLS
        },

        "totals": {
            "runs": 0,

            "source_downloads_ok": 0,
            "source_downloads_failed": 0,

            "source_nodes_found": 0,
            "unique_nodes": 0,

            "source_nodes_tested": 0,
            "source_live": 0,
            "source_dead": 0,

            "archive_tested": 0,
            "archive_live": 0,
            "archive_dead": 0,

            "tcp_failed": 0,

            "service_attempts": 0,
            "service_success": 0,
            "service_failed": 0,

            "subscription_live": 0,
        },

        "run_history": [],
    }


def load_stats():
    if not os.path.exists(STATS_FILE):
        return default_stats()

    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return default_stats()

        if data.get("version") != 3:
            return default_stats()

        base = default_stats()

        for key in (
            "sources",
            "protocols",
            "transports",
            "security",
            "services",
            "totals",
            "run_history",
        ):
            if key in data:
                base[key] = data[key]

        for url in TEST_URLS:
            if url not in base["services"]:
                base["services"][url] = make_service_bucket()

        return base

    except Exception as e:
        logger.warning("Stats load failed: %s", e)
        return default_stats()


def save_stats(stats):
    tmp = STATS_FILE + ".tmp"

    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(
            stats,
            f,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(tmp, STATS_FILE)


# ============================================================
# HELPERS
# ============================================================

def node_id(uri):
    return hashlib.sha256(
        uri.strip().encode("utf-8")
    ).hexdigest()[:16]


def clean_uri(uri):
    return uri.strip()


def source_name(url):
    return url.split("/")[-1] or url


def ensure_source_bucket(stats, source):
    if source not in stats["sources"]:
        stats["sources"][source] = make_source_bucket()

    return stats["sources"][source]


def ensure_dimension_bucket(container, key):
    if key not in container:
        container[key] = make_dimension_bucket()

    return container[key]


# ============================================================
# SOURCES.TXT
# ============================================================

def load_sources():
    if not os.path.exists(SOURCES_FILE):
        raise FileNotFoundError(
            f"sources.txt not found: {SOURCES_FILE}"
        )

    sources = []

    with open(
        SOURCES_FILE,
        "r",
        encoding="utf-8",
        errors="ignore",
    ) as f:

        for line in f:
            line = line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            if not line.startswith(("http://", "https://")):
                continue

            if line not in sources:
                sources.append(line)

    return sources


# ============================================================
# SOURCE DECODING
# ============================================================

def decode_base64_text(text):
    text = text.strip()

    if not text:
        return ""

    candidates = [
        text,
        text.replace("\n", "").replace("\r", ""),
    ]

    for candidate in candidates:
        try:
            padding = "=" * (-len(candidate) % 4)

            decoded = base64.b64decode(
                candidate + padding,
                validate=False,
            ).decode(
                "utf-8",
                errors="ignore",
            )

            if "vless://" in decoded:
                return decoded

        except Exception:
            pass

    return ""


def extract_vless(text):
    result = []

    for raw_line in text.splitlines():

        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("vless://"):
            result.append(line)
            continue

        if "vless://" in line:
            start = line.find("vless://")

            value = line[start:].strip()

            if value:
                result.append(value)

    if result:
        return result

    decoded = decode_base64_text(text)

    if decoded:
        for line in decoded.splitlines():

            line = line.strip()

            if line.startswith("vless://"):
                result.append(line)

    return result


# ============================================================
# DOWNLOAD SOURCE
# ============================================================

def download_source(url):
    try:

        response = requests.get(
            url,
            timeout=SOURCE_TIMEOUT,
            headers={
                "User-Agent": "Mozilla/5.0",
            },
        )

        response.raise_for_status()

        nodes = extract_vless(response.text)

        # Убираем дубликаты внутри одного источника.
        unique = []
        seen = set()

        for uri in nodes:
            uri = clean_uri(uri)

            if not uri.startswith("vless://"):
                continue

            if uri in seen:
                continue

            seen.add(uri)
            unique.append(uri)

        return True, unique, None

    except Exception as e:
        return False, [], str(e)


# ============================================================
# VLESS PARSER
# ============================================================

def parse_vless(uri, local_port):
    parsed = urlparse(uri)

    if parsed.scheme.lower() != "vless":
        raise ValueError("Not VLESS URI")

    uuid = unquote(parsed.username or "")

    server = parsed.hostname

    if not server:
        raise ValueError("Missing server")

    port = parsed.port

    if not port:
        raise ValueError("Missing port")

    params_raw = parse_qs(
        parsed.query,
        keep_blank_values=True,
    )

    params = {
        key.lower(): values[0]
        for key, values in params_raw.items()
        if values
    }

    security = params.get("security", "").lower()

    if not security:
        security = "none"

    transport = params.get("type", "tcp").lower()

    if transport == "raw":
        transport_name = "raw"
    else:
        transport_name = transport

    # --------------------------------------------------------
    # OUTBOUND
    # --------------------------------------------------------

    outbound = {
        "type": "vless",
        "server": server,
        "server_port": port,
        "uuid": uuid,
    }

    flow = params.get("flow")

    if flow:
        outbound["flow"] = flow

    # --------------------------------------------------------
    # TLS / REALITY
    # --------------------------------------------------------

    if security in ("tls", "reality"):

        tls = {
            "enabled": True,
            "server_name": params.get(
                "sni",
                server,
            ),
        }

        fingerprint = params.get("fp")

        if fingerprint:
            tls["utls"] = {
                "enabled": True,
                "fingerprint": fingerprint,
            }

        if security == "reality":

            reality = {
                "enabled": True,
            }

            public_key = (
                params.get("pbk")
                or params.get("publickey")
            )

            short_id = (
                params.get("sid")
                or params.get("shortid")
            )

            if public_key:
                reality["public_key"] = public_key

            if short_id:
                reality["short_id"] = short_id

            tls["reality"] = reality

        alpn = params.get("alpn")

        if alpn:
            tls["alpn"] = [
                x.strip()
                for x in alpn.split(",")
                if x.strip()
            ]

        outbound["tls"] = tls

    # --------------------------------------------------------
    # TRANSPORT
    # --------------------------------------------------------

    if transport == "ws":

        transport_config = {
            "type": "ws",
            "path": params.get("path", "/"),
        }

        host = params.get("host")

        if host:
            transport_config["headers"] = {
                "Host": host,
            }

        outbound["transport"] = transport_config

    elif transport == "grpc":

        outbound["transport"] = {
            "type": "grpc",
            "service_name": params.get(
                "servicename",
                "",
            ),
        }

    elif transport == "httpupgrade":

        outbound["transport"] = {
            "type": "httpupgrade",
            "path": params.get("path", "/"),
        }

        host = params.get("host")

        if host:
            outbound["transport"]["host"] = host

    elif transport == "xhttp":

        transport_config = {
            "type": "xhttp",
            "path": params.get("path", "/"),
        }

        host = params.get("host")

        if host:
            transport_config["host"] = host

        mode = params.get("mode")

        if mode:
            transport_config["mode"] = mode

        outbound["transport"] = transport_config

    # tcp/raw = стандартный transport sing-box

    # --------------------------------------------------------
    # CONFIG
    # --------------------------------------------------------

    config = {
        "log": {
            "level": "error",
        },

        "inbounds": [
            {
                "type": "mixed",
                "tag": "proxy",
                "listen": "127.0.0.1",
                "listen_port": local_port,
            }
        ],

        "outbounds": [
            outbound,

            {
                "type": "direct",
                "tag": "direct",
            },

            {
                "type": "block",
                "tag": "block",
            },
        ],

        "route": {
            "final": "proxy",
        },
    }

    meta = {
        "server": server,
        "port": port,
        "protocol": "vless",
        "transport": transport_name,
        "security": security,
    }

    return config, meta


# ============================================================
# TCP PRECHECK
# ============================================================

def tcp_precheck(server, port):
    try:

        sock = socket.create_connection(
            (server, port),
            timeout=TCP_TIMEOUT,
        )

        sock.close()

        return True, ""

    except Exception as e:

        return False, str(e)


# ============================================================
# SERVICE TEST
# ============================================================

def test_services(local_port):
    results = {}

    proxies = {
        "http": f"socks5h://127.0.0.1:{local_port}",
        "https": f"socks5h://127.0.0.1:{local_port}",
    }

    session = requests.Session()

    for url in TEST_URLS:

        try:

            response = session.get(
                url,
                proxies=proxies,
                timeout=SERVICE_TIMEOUT,
                allow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0",
                },
            )

            results[url] = (
                response.status_code in GOOD_CODES
            )

        except Exception:

            results[url] = False

    session.close()

    return results


# ============================================================
# SING-BOX CHECK
# ============================================================

def check_single_uri(uri, local_port):
    config_path = os.path.join(
        TEMP_DIR,
        f"config_{local_port}.json",
    )

    proc = None

    try:

        config, meta = parse_vless(
            uri,
            local_port,
        )

        tcp_ok, tcp_error = tcp_precheck(
            meta["server"],
            meta["port"],
        )

        if not tcp_ok:

            return {
                "uri": uri,
                "id": node_id(uri),
                "protocol": meta["protocol"],
                "transport": meta["transport"],
                "security": meta["security"],
                "tcp_ok": False,
                "tcp_error": tcp_error,
                "services": {},
                "success_count": 0,
                "live": False,
                "error": "TCP precheck failed",
            }

        with open(
            config_path,
            "w",
            encoding="utf-8",
        ) as f:

            json.dump(
                config,
                f,
                ensure_ascii=False,
                indent=2,
            )

        # ----------------------------------------------------
        # CONFIG CHECK
        # ----------------------------------------------------

        check = subprocess.run(
            [
                SINGBOX_BIN,
                "check",
                "-c",
                config_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )

        if check.returncode != 0:

            error_text = (
                check.stderr.strip()
                or check.stdout.strip()
                or "sing-box config check failed"
            )

            with open(
                ERROR_LOG,
                "a",
                encoding="utf-8",
            ) as log:

                log.write(
                    f"\n{uri}\n{error_text}\n"
                )

            return {
                "uri": uri,
                "id": node_id(uri),
                "protocol": meta["protocol"],
                "transport": meta["transport"],
                "security": meta["security"],
                "tcp_ok": True,
                "tcp_error": "",
                "services": {},
                "success_count": 0,
                "live": False,
                "error": error_text,
            }

        # ----------------------------------------------------
        # START SING-BOX
        # ----------------------------------------------------

        proc = subprocess.Popen(
            [
                SINGBOX_BIN,
                "run",
                "-c",
                config_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

        deadline = time.time() + SINGBOX_START_WAIT

        while time.time() < deadline:

            if proc.poll() is not None:

                stderr_text = ""

                try:
                    stderr_text = (
                        proc.stderr.read() or ""
                    ).strip()
                except Exception:
                    pass

                return {
                    "uri": uri,
                    "id": node_id(uri),
                    "protocol": meta["protocol"],
                    "transport": meta["transport"],
                    "security": meta["security"],
                    "tcp_ok": True,
                    "tcp_error": "",
                    "services": {},
                    "success_count": 0,
                    "live": False,
                    "error": (
                        stderr_text
                        or "sing-box exited immediately"
                    ),
                }

            time.sleep(0.05)

        # ----------------------------------------------------
        # ALL SERVICES
        # ----------------------------------------------------

        service_results = test_services(
            local_port
        )

        success_count = sum(
            1
            for value in service_results.values()
            if value
        )

        live = (
            success_count >= 3
        )

        return {
            "uri": uri,
            "id": node_id(uri),
            "protocol": meta["protocol"],
            "transport": meta["transport"],
            "security": meta["security"],
            "tcp_ok": True,
            "tcp_error": "",
            "services": service_results,
            "success_count": success_count,
            "live": live,
            "error": "",
        }

    except Exception as e:

        return {
            "uri": uri,
            "id": node_id(uri),
            "protocol": "vless",
            "transport": "unknown",
            "security": "unknown",
            "tcp_ok": False,
            "tcp_error": "",
            "services": {},
            "success_count": 0,
            "live": False,
            "error": str(e),
        }

    finally:

        if proc is not None:

            try:
                proc.terminate()
                proc.wait(timeout=1)
            except Exception:

                try:
                    proc.kill()
                except Exception:
                    pass

        try:
            if os.path.exists(config_path):
                os.remove(config_path)
        except Exception:
            pass


# ============================================================
# NODE WORKER
# ============================================================

def check_node(task):
    index, total, uri = task

    port = PORT_QUEUE.get()

    try:

        result = check_single_uri(
            uri,
            port,
        )

        state = (
            "LIVE"
            if result["live"]
            else "DEAD"
        )

        if not result["tcp_ok"]:

            logger.info(
                "[%d/%d] %s | %s | %s/%s | TCP FAIL",
                index,
                total,
                state,
                result["id"],
                result["transport"],
                result["security"],
            )

        else:

            logger.info(
                "[%d/%d] %s | %s | %s/%s | %d/5",
                index,
                total,
                state,
                result["id"],
                result["transport"],
                result["security"],
                result["success_count"],
            )

        return result

    finally:

        PORT_QUEUE.put(port)


# ============================================================
# ARCHIVE
# ============================================================

def load_archive():
    if not os.path.exists(ARCHIVE_FILE):
        return []

    result = []
    seen = set()

    with open(
        ARCHIVE_FILE,
        "r",
        encoding="utf-8",
        errors="ignore",
    ) as f:

        for line in f:

            uri = clean_uri(line)

            if not uri.startswith("vless://"):
                continue

            if uri in seen:
                continue

            seen.add(uri)
            result.append(uri)

    return result


def save_archive(nodes):
    tmp = ARCHIVE_FILE + ".tmp"

    with open(
        tmp,
        "w",
        encoding="utf-8",
    ) as f:

        for uri in nodes:
            f.write(uri + "\n")

    os.replace(
        tmp,
        ARCHIVE_FILE,
    )


# ============================================================
# UPDATE STATS
# ============================================================

def update_stats_for_result(
    stats,
    result,
    source_memberships,
    archive=False,
):
    protocol = result["protocol"]
    transport = result["transport"]
    security = result["security"]

    protocol_bucket = ensure_dimension_bucket(
        stats["protocols"],
        protocol,
    )

    transport_bucket = ensure_dimension_bucket(
        stats["transports"],
        transport,
    )

    security_bucket = ensure_dimension_bucket(
        stats["security"],
        security,
    )

    for bucket in (
        protocol_bucket,
        transport_bucket,
        security_bucket,
    ):
        bucket["tested"] += 1

        if result["live"]:
            bucket["live"] += 1
        else:
            bucket["dead"] += 1

    # --------------------------------------------------------
    # SERVICES
    # --------------------------------------------------------

    for url, success in result["services"].items():

        if url not in stats["services"]:
            stats["services"][url] = make_service_bucket()

        bucket = stats["services"][url]

        bucket["attempts"] += 1

        if success:
            bucket["success"] += 1
        else:
            bucket["failed"] += 1

        stats["totals"]["service_attempts"] += 1

        if success:
            stats["totals"]["service_success"] += 1
        else:
            stats["totals"]["service_failed"] += 1

    if not result["tcp_ok"]:

        stats["totals"]["tcp_failed"] += 1

    # --------------------------------------------------------
    # SOURCE STATS
    # --------------------------------------------------------

    if not archive:

        for source in source_memberships:

            bucket = ensure_source_bucket(
                stats,
                source,
            )

            bucket["tested"] += 1

            if result["live"]:
                bucket["live"] += 1
            else:
                bucket["dead"] += 1

        stats["totals"]["source_nodes_tested"] += 1

        if result["live"]:
            stats["totals"]["source_live"] += 1
        else:
            stats["totals"]["source_dead"] += 1

    else:

        stats["totals"]["archive_tested"] += 1

        if result["live"]:
            stats["totals"]["archive_live"] += 1
        else:
            stats["totals"]["archive_dead"] += 1


# ============================================================
# SING-BOX VERSION
# ============================================================

def show_singbox_version():
    try:

        result = subprocess.run(
            [
                SINGBOX_BIN,
                "version",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
        )

        logger.info(
            "%s",
            result.stdout.strip(),
        )

        return True

    except Exception as e:

        logger.error(
            "Cannot run sing-box: %s",
            e,
        )

        return False


# ============================================================
# MAIN
# ============================================================

def main():

    logger.info("=" * 70)
    logger.info("=== SING-BOX VLESS MAIN ===")
    logger.info("=" * 70)

    logger.info(
        "Sources are loaded from: %s",
        SOURCES_FILE,
    )

    logger.info(
        "Threads: %d",
        MAX_THREADS,
    )

    logger.info(
        "Services: %d",
        len(TEST_URLS),
    )

    logger.info(
        "LIVE requirement: 3/%d",
        len(TEST_URLS),
    )

    logger.info(
        "Subscription target: %d",
        MAX_NODES,
    )

    logger.info(
        "SNI mutation: DISABLED",
    )

    logger.info(
        "Archive: %s",
        ARCHIVE_FILE,
    )

    # --------------------------------------------------------
    # LOAD STATS
    # --------------------------------------------------------

    stats = load_stats()

    stats["totals"]["runs"] += 1

    # --------------------------------------------------------
    # SING-BOX
    # --------------------------------------------------------

    if not show_singbox_version():
        raise RuntimeError(
            "sing-box is not available"
        )

    # --------------------------------------------------------
    # LOAD SOURCES
    # --------------------------------------------------------

    sources = load_sources()

    logger.info(
        "Sources: %d",
        len(sources),
    )

    # --------------------------------------------------------
    # DOWNLOAD SOURCES
    # --------------------------------------------------------

    logger.info("=" * 70)
    logger.info("=== DOWNLOADING SOURCES ===")
    logger.info("=" * 70)

    all_nodes = {}
    source_memberships = {}

    source_found_total = 0

    for source in sources:

        name = source_name(source)

        bucket = ensure_source_bucket(
            stats,
            source,
        )

        ok, nodes, error = download_source(
            source
        )

        if not ok:

            bucket["downloads_failed"] += 1

            stats["totals"][
                "source_downloads_failed"
            ] += 1

            logger.error(
                "SOURCE FAILED: %s | %s",
                name,
                error,
            )

            continue

        bucket["downloads_ok"] += 1

        stats["totals"][
            "source_downloads_ok"
        ] += 1

        bucket["found"] = len(nodes)

        source_found_total += len(nodes)

        for uri in nodes:

            if uri not in all_nodes:
                all_nodes[uri] = uri

            if uri not in source_memberships:
                source_memberships[uri] = []

            if source not in source_memberships[uri]:
                source_memberships[uri].append(source)

        logger.info(
            "SOURCE OK: %s | VLESS: %d",
            name,
            len(nodes),
        )

    stats["totals"]["source_nodes_found"] = (
        stats["totals"].get(
            "source_nodes_found",
            0,
        )
        + source_found_total
    )

    stats["totals"]["unique_nodes"] = (
        len(all_nodes)
    )

    logger.info(
        "VLESS found across sources: %d",
        source_found_total,
    )

    logger.info(
        "Unique nodes: %d",
        len(all_nodes),
    )

    # --------------------------------------------------------
    # FRESH NODE TEST
    # --------------------------------------------------------

    logger.info("=" * 70)
    logger.info("=== TESTING FRESH NODES ===")
    logger.info("=" * 70)

    fresh_nodes = list(all_nodes.keys())

    fresh_live = []
    fresh_dead = []

    results_by_uri = {}

    tasks = [
        (
            index,
            len(fresh_nodes),
            uri,
        )
        for index, uri
        in enumerate(fresh_nodes, 1)
    ]

    with ThreadPoolExecutor(
        max_workers=MAX_THREADS
    ) as executor:

        for result in executor.map(
            check_node,
            tasks,
        ):

            results_by_uri[
                result["uri"]
            ] = result

            update_stats_for_result(
                stats,
                result,
                source_memberships.get(
                    result["uri"],
                    [],
                ),
                archive=False,
            )

            if result["live"]:
                fresh_live.append(
                    result["uri"]
                )
            else:
                fresh_dead.append(
                    result["uri"]
                )

    logger.info(
        "Fresh LIVE: %d",
        len(fresh_live),
    )

    logger.info(
        "Fresh DEAD: %d",
        len(fresh_dead),
    )

    # --------------------------------------------------------
    # ARCHIVE LOAD
    # --------------------------------------------------------

    archive_nodes = load_archive()

    logger.info(
        "Archive before update: %d",
        len(archive_nodes),
    )

    archive_map = {
        uri: uri
        for uri in archive_nodes
    }

    # --------------------------------------------------------
    # FRESH LIVE -> ARCHIVE
    # FRESH DEAD -> REMOVE FROM ARCHIVE
    # --------------------------------------------------------

    for uri in fresh_live:
        archive_map[uri] = uri

    for uri in fresh_dead:
        archive_map.pop(uri, None)

    # --------------------------------------------------------
    # CURRENT SOURCE NODES MUST NOT BE RECHECKED FROM ARCHIVE
    # --------------------------------------------------------

    current_source_ids = {
        node_id(uri)
        for uri in fresh_nodes
    }

    # --------------------------------------------------------
    # SUBSCRIPTION
    # --------------------------------------------------------

    subscription = []

    seen_subscription = set()

    for uri in fresh_live:

        if uri in seen_subscription:
            continue

        seen_subscription.add(uri)

        subscription.append(uri)

        if len(subscription) >= MAX_NODES:
            break

    # --------------------------------------------------------
    # ARCHIVE FILL
    # --------------------------------------------------------

    if len(subscription) < MAX_NODES:

        logger.info("=" * 70)
        logger.info("=== RECHECKING ARCHIVE ===")
        logger.info("=" * 70)

        archive_candidates = []

        # Новые/последние записи проверяем первыми.
        for uri in reversed(
            list(archive_map.keys())
        ):

            if node_id(uri) in current_source_ids:
                continue

            if uri in seen_subscription:
                continue

            archive_candidates.append(uri)

        logger.info(
            "Archive candidates: %d",
            len(archive_candidates),
        )

        for uri in archive_candidates:

            if len(subscription) >= MAX_NODES:
                break

            port = PORT_QUEUE.get()

            try:

                result = check_single_uri(
                    uri,
                    port,
                )

            finally:

                PORT_QUEUE.put(port)

            update_stats_for_result(
                stats,
                result,
                [],
                archive=True,
            )

            if result["live"]:

                archive_map[uri] = uri

                if uri not in seen_subscription:

                    seen_subscription.add(uri)

                    subscription.append(uri)

                logger.info(
                    "ARCHIVE LIVE | %s | %d/5",
                    result["id"],
                    result["success_count"],
                )

            else:

                archive_map.pop(
                    uri,
                    None,
                )

                logger.info(
                    "ARCHIVE DEAD | %s",
                    result["id"],
                )

    # --------------------------------------------------------
    # SAVE ARCHIVE
    # --------------------------------------------------------

    final_archive = list(
        archive_map.keys()
    )

    save_archive(
        final_archive
    )

    # --------------------------------------------------------
    # SAVE SUBSCRIPTION
    # --------------------------------------------------------

    with open(
        SUBSCRIPTION_FILE,
        "w",
        encoding="utf-8",
    ) as f:

        for uri in subscription:
            f.write(uri + "\n")

    stats["totals"][
        "subscription_live"
    ] = len(subscription)

    # --------------------------------------------------------
    # RUN HISTORY
    # --------------------------------------------------------

    run_summary = {
        "timestamp": time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),

        "sources": len(sources),

        "source_nodes_found":
            source_found_total,

        "unique_nodes":
            len(fresh_nodes),

        "fresh_live":
            len(fresh_live),

        "fresh_dead":
            len(fresh_dead),

        "archive_before":
            len(archive_nodes),

        "archive_after":
            len(final_archive),

        "subscription":
            len(subscription),
    }

    stats["run_history"].append(
        run_summary
    )

    # Не даём истории бесконечно разрастаться.
    if len(stats["run_history"]) > 1000:
        stats["run_history"] = (
            stats["run_history"][-1000:]
        )

    save_stats(stats)

    # --------------------------------------------------------
    # REPORT
    # --------------------------------------------------------

    logger.info("=" * 70)
    logger.info("=== FINAL RESULT ===")
    logger.info("=" * 70)

    logger.info(
        "Sources: %d",
        len(sources),
    )

    logger.info(
        "VLESS found: %d",
        source_found_total,
    )

    logger.info(
        "Unique nodes: %d",
        len(fresh_nodes),
    )

    logger.info(
        "Fresh LIVE: %d",
        len(fresh_live),
    )

    logger.info(
        "Fresh DEAD: %d",
        len(fresh_dead),
    )

    logger.info(
        "Archive before: %d",
        len(archive_nodes),
    )

    logger.info(
        "Archive after: %d",
        len(final_archive),
    )

    logger.info(
        "Subscription: %d",
        len(subscription),
    )

    logger.info(
        "Subscription file: %s",
        SUBSCRIPTION_FILE,
    )

    logger.info(
        "Archive file: %s",
        ARCHIVE_FILE,
    )

    logger.info(
        "Stats file: %s",
        STATS_FILE,
    )

    logger.info("=" * 70)


if __name__ == "__main__":
    main()
