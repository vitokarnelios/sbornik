#!/usr/bin/env python3

import os
import requests
import base64
import random
import json
import subprocess
import time
import queue
import threading
import logging
import hashlib

from collections import defaultdict
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote, urlencode, urlunparse
from concurrent.futures import ThreadPoolExecutor


# ============================================================
# PATHS
# ============================================================

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

FINAL_DIR = os.path.join(BASE_PATH, "subs")
LOG_DIR = os.path.join(BASE_PATH, "logs")
SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")

SERVICE_STATS_FILE = os.path.join(BASE_PATH, "stats", "service_stats.json")
NODE_STATS_FILE = os.path.join(BASE_PATH, "stats", "node_stats.json")
SOURCE_STATS_FILE = os.path.join(BASE_PATH, "stats", "source_stats.json")
RUN_STATS_FILE = os.path.join(BASE_PATH, "stats", "run_stats.json")

ALIVE_ARCHIVE_FILE = os.path.join(FINAL_DIR, "live_archive.txt")
SUBSCRIPTION_FILE = os.path.join(FINAL_DIR, "vless_001.txt")

XRAY_LOG = os.path.join(LOG_DIR, "errors.log")

os.makedirs(FINAL_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_PATH, "stats"), exist_ok=True)


# ============================================================
# SETTINGS
# ============================================================

MAX_NODES = 100
MAX_THREADS = 40

SOURCE_TIMEOUT = 15
SERVICE_TIMEOUT = 2

SINGBOX_START_WAIT = 1.5

NEXT_PORT = 11000
MAX_PORT = 11000 + MAX_THREADS

GOOD_CODES = {200, 204, 301, 302}

TEST_URLS = [
    "https://telegram.org",
    "https://www.instagram.com",
    "https://www.youtube.com",
    "https://gemini.google.com",
    "https://www.google.com",
]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)

logger = logging.getLogger("main")


# ============================================================
# GLOBALS
# ============================================================

port_queue = queue.Queue()

for port in range(NEXT_PORT, MAX_PORT):
    port_queue.put(port)


stats_lock = threading.Lock()
source_lock = threading.Lock()
archive_lock = threading.Lock()

node_source_map = defaultdict(set)

current_run_number = 0


# ============================================================
# SOURCES
# ============================================================

def load_sources():
    sources = []

    if not os.path.exists(SOURCES_FILE):
        return sources

    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            sources.append(line)

    return sources


SOURCES = load_sources()


# ============================================================
# JSON HELPERS
# ============================================================

def load_json(path, default):
    try:
        if not os.path.exists(path):
            return default

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return data

    except Exception:
        return default


def save_json_stats(path, data):
    tmp_path = path + ".tmp"

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(
                data,
                f,
                ensure_ascii=False,
                indent=2
            )

        os.replace(tmp_path, path)

    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


# ============================================================
# STATS
# ============================================================

service_stats = load_json(
    SERVICE_STATS_FILE,
    {}
)

node_stats = load_json(
    NODE_STATS_FILE,
    {}
)

source_stats = load_json(
    SOURCE_STATS_FILE,
    {}
)

run_stats = load_json(
    RUN_STATS_FILE,
    {}
)


def ensure_service_bucket(service):
    if service not in service_stats:
        service_stats[service] = {
            "attempts": 0,
            "success": 0,
            "dead": 0,
            "last_run": None
        }


def ensure_source_bucket(source):
    if source not in source_stats:
        source_stats[source] = {
            "runs": 0,
            "fetch_attempts": 0,
            "fetch_success": 0,
            "lines_total": 0,
            "vless_total": 0,
            "unique_nodes": 0,
            "nodes_tested": 0,
            "original_live": 0,
            "dead": 0,
            "last_run": None,
            "recent_node_ids": []
        }


def ensure_node_bucket(node_id):
    if node_id not in node_stats:
        node_stats[node_id] = {
            "attempts": 0,
            "live": 0,
            "dead": 0,
            "sources": [],
            "last_status": None,
            "last_run": None
        }


def prune_node_stats():
    max_nodes = 30000

    if len(node_stats) <= max_nodes:
        return

    items = list(node_stats.items())

    items.sort(
        key=lambda x: x[1].get("last_run", ""),
        reverse=True
    )

    node_stats.clear()

    for node_id, data in items[:max_nodes]:
        node_stats[node_id] = data


# ============================================================
# NODE ID
# ============================================================

def node_id_from_uri(uri):
    """
    ID зависит от самой URI-ноды.
    SNI mutation больше не используется.
    """

    return hashlib.sha256(
        uri.encode("utf-8", errors="ignore")
    ).hexdigest()[:24]


# ============================================================
# SOURCE -> NODE
# ============================================================

def record_node_source(node_id, source):
    with source_lock:
        node_source_map[node_id].add(source)

        ensure_node_bucket(node_id)

        sources = node_stats[node_id].setdefault(
            "sources",
            []
        )

        if source not in sources:
            sources.append(source)

        if len(sources) > 30:
            del sources[:-30]


# ============================================================
# FETCH SOURCE
# ============================================================

def fetch_source(source):
    try:
        response = requests.get(
            source,
            timeout=SOURCE_TIMEOUT,
            headers={
                "User-Agent": "Mozilla/5.0"
            }
        )

        response.raise_for_status()

        text = response.text.strip()

        lines = [
            x.strip()
            for x in text.splitlines()
            if x.strip()
        ]

        decoded_lines = []

        for line in lines:
            try:
                decoded = base64.b64decode(
                    line + "=" * (-len(line) % 4)
                ).decode(
                    "utf-8",
                    errors="ignore"
                )

                if "vless://" in decoded:
                    decoded_lines.extend(
                        x.strip()
                        for x in decoded.splitlines()
                        if x.strip()
                    )

            except Exception:
                pass

        if decoded_lines:
            lines = decoded_lines

        vless_nodes = [
            line
            for line in lines
            if line.startswith("vless://")
        ]

        return {
            "success": True,
            "lines_total": len(lines),
            "nodes": vless_nodes
        }

    except Exception as e:
        return {
            "success": False,
            "lines_total": 0,
            "nodes": [],
            "error": str(e)
        }


# ============================================================
# VLESS PARSER
# ============================================================

def parse_vless_to_json(uri, listen_port):
    """
    РАБОЧАЯ ЛОГИКА.
    Не менять без необходимости.
    """

    parsed = urlparse(uri)

    uuid = unquote(parsed.username or "")

    server_part = parsed.netloc.split("@")[-1]

    if ":" not in server_part:
        return None

    server_address, server_port = server_part.split(":", 1)

    try:
        server_port = int(server_port)
    except Exception:
        return None

    params = parse_qs(parsed.query)

    def get_param(name, default=""):
        value = params.get(name)

        if not value:
            return default

        return unquote(value[0])

    node_name = hashlib.sha256(
        uri.encode("utf-8", errors="ignore")
    ).hexdigest()[:16]

    outbound = {
        "type": "vless",
        "tag": node_name,
        "server": server_address,
        "server_port": server_port,
        "uuid": uuid,
    }

    flow = get_param("flow")

    if flow:
        outbound["flow"] = flow

    security = get_param("security")

    if security == "reality":
        outbound["tls"] = {
            "enabled": True,
            "server_name": get_param(
                "sni",
                server_address
            ),
            "utls": {
                "enabled": True,
                "fingerprint": get_param(
                    "fp",
                    "chrome"
                )
            },
            "reality": {
                "enabled": True,
                "public_key": get_param("pbk", ""),
                "short_id": get_param("sid", "")
            }
        }

    elif security == "tls":
        outbound["tls"] = {
            "enabled": True,
            "server_name": get_param(
                "sni",
                server_address
            )
        }

        alpn = get_param("alpn")

        if alpn:
            outbound["tls"]["alpn"] = [
                x
                for x in alpn.split(",")
                if x
            ]

    transport = get_param("type", "tcp")

    if transport == "grpc":
        outbound["transport"] = {
            "type": "grpc",
            "service_name": get_param(
                "serviceName",
                ""
            )
        }

    elif transport == "xhttp":
        outbound["transport"] = {
            "type": "xhttp",
            "path": get_param(
                "path",
                "/"
            ),
            "host": [
                get_param("host", "")
            ],
            "mode": get_param(
                "mode",
                ""
            )
        }

    elif transport == "ws":
        headers = {}

        host = get_param("host")

        if host:
            headers["Host"] = host

        outbound["transport"] = {
            "type": "ws",
            "path": get_param(
                "path",
                "/"
            ),
            "headers": headers
        }

    config = {
        "log": {
            "level": "error"
        },
        "inbounds": [
            {
                "type": "socks",
                "tag": "socks-in",
                "listen": "127.0.0.1",
                "listen_port": listen_port
            }
        ],
        "outbounds": [
            outbound
        ]
    }

    return config


# ============================================================
# SERVICE CHECK
# ============================================================

def check_service(service, proxy):
    ensure_service_bucket(service)

    try:
        response = requests.get(
            service,
            proxies={
                "http": proxy,
                "https": proxy
            },
            timeout=SERVICE_TIMEOUT,
            allow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0"
            }
        )

        code = response.status_code
        success = code in GOOD_CODES

    except Exception:
        code = None
        success = False

    with stats_lock:
        bucket = service_stats[service]

        bucket["attempts"] += 1
        bucket["last_run"] = datetime.now().isoformat()

        if success:
            bucket["success"] += 1
        else:
            bucket["dead"] += 1

    return success, code


# ============================================================
# SING-BOX CHECK
# ============================================================

def check_single_uri(uri, listen_port):
    temp_config_path = os.path.join(
        BASE_PATH,
        f"temp_{listen_port}.json"
    )

    singbox_log_path = os.path.join(
        LOG_DIR,
        f"singbox_{listen_port}.log"
    )

    process = None
    log_file = None

    try:
        config = parse_vless_to_json(
            uri,
            listen_port
        )

        if not config:
            return False

        with open(
            temp_config_path,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                config,
                f,
                ensure_ascii=False,
                indent=2
            )

        singbox_path = os.path.join(
            BASE_PATH,
            "sing-box",
            "sing-box"
        )

        if os.name == "nt":
            singbox_path = os.path.join(
                BASE_PATH,
                "sing-box",
                "sing-box.exe"
            )

        if not os.path.exists(singbox_path):
            singbox_path = "sing-box"

        log_file = open(
            singbox_log_path,
            "w",
            encoding="utf-8",
            errors="ignore"
        )

        process = subprocess.Popen(
            [
                singbox_path,
                "run",
                "-c",
                temp_config_path
            ],
            stdout=log_file,
            stderr=log_file
        )

        for _ in range(15):
            time.sleep(0.1)

            if process.poll() is not None:
                return False

        proxy = (
            f"socks5h://127.0.0.1:{listen_port}"
        )

        success_count = 0

        # ВАЖНО:
        # проверяются ВСЕ 5 сервисов.
        # Даже после 3 успешных проверка не останавливается.
        for service in TEST_URLS:
            success, code = check_service(
                service,
                proxy
            )

            if success:
                success_count += 1

        return success_count >= 3

    except Exception as e:
        try:
            with open(
                XRAY_LOG,
                "a",
                encoding="utf-8"
            ) as f:
                f.write(
                    f"{datetime.now().isoformat()} "
                    f"{uri[:120]} "
                    f"{e}\n"
                )
        except Exception:
            pass

        return False

    finally:
        if process is not None:
            try:
                if process.poll() is None:
                    process.terminate()

                    try:
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        process.kill()

            except Exception:
                pass

        if log_file is not None:
            try:
                log_file.close()
            except Exception:
                pass

        try:
            if os.path.exists(temp_config_path):
                os.remove(temp_config_path)
        except Exception:
            pass

        try:
            if os.path.exists(singbox_log_path):
                os.remove(singbox_log_path)
        except Exception:
            pass


# ============================================================
# NODE TEST
# ============================================================

def test_node(uri):
    port = port_queue.get()

    try:
        node_id = node_id_from_uri(uri)

        live = check_single_uri(
            uri,
            port
        )

        with stats_lock:
            ensure_node_bucket(node_id)

            node_stats[node_id]["attempts"] += 1
            node_stats[node_id]["last_run"] = datetime.now().isoformat()

            if live:
                node_stats[node_id]["live"] += 1
                node_stats[node_id]["last_status"] = "LIVE"
            else:
                node_stats[node_id]["dead"] += 1
                node_stats[node_id]["last_status"] = "DEAD"

        return {
            "uri": uri,
            "node_id": node_id,
            "live": live
        }

    finally:
        port_queue.put(port)


# ============================================================
# SOURCE STATS
# ============================================================

def update_source_result(
    node_id,
    live,
    current_run
):
    sources = node_source_map.get(
        node_id,
        set()
    )

    with stats_lock:
        for source in sources:
            ensure_source_bucket(source)

            bucket = source_stats[source]

            bucket["nodes_tested"] += 1

            if live:
                bucket["original_live"] += 1
            else:
                bucket["dead"] += 1

            bucket["last_run"] = current_run


# ============================================================
# ARCHIVE
# ============================================================

def load_archive():
    if not os.path.exists(ALIVE_ARCHIVE_FILE):
        return []

    result = []

    try:
        with open(
            ALIVE_ARCHIVE_FILE,
            "r",
            encoding="utf-8"
        ) as f:
            for line in f:
                line = line.strip()

                if (
                    line
                    and line.startswith("vless://")
                ):
                    result.append(line)

    except Exception:
        pass

    # Убираем только точные дубли
    seen = set()
    unique = []

    for uri in result:
        if uri not in seen:
            seen.add(uri)
            unique.append(uri)

    return unique


def save_archive(nodes):
    tmp = ALIVE_ARCHIVE_FILE + ".tmp"

    unique = []
    seen = set()

    for uri in nodes:
        if not uri:
            continue

        if uri in seen:
            continue

        seen.add(uri)
        unique.append(uri)

    with open(
        tmp,
        "w",
        encoding="utf-8"
    ) as f:
        for uri in unique:
            f.write(uri + "\n")

    os.replace(
        tmp,
        ALIVE_ARCHIVE_FILE
    )


# ============================================================
# SING-BOX VERSION
# ============================================================

def get_singbox_path():
    if os.name == "nt":
        candidates = [
            os.path.join(
                BASE_PATH,
                "sing-box",
                "sing-box.exe"
            ),
            os.path.join(
                BASE_PATH,
                "sing-box.exe"
            )
        ]
    else:
        candidates = [
            os.path.join(
                BASE_PATH,
                "sing-box",
                "sing-box"
            ),
            os.path.join(
                BASE_PATH,
                "sing-box"
            )
        ]

    for path in candidates:
        if os.path.exists(path):
            return path

    return "sing-box"


def get_singbox_version():
    try:
        result = subprocess.run(
            [
                get_singbox_path(),
                "version"
            ],
            capture_output=True,
            text=True,
            timeout=10
        )

        output = (
            result.stdout
            or result.stderr
            or ""
        ).strip()

        if output:
            return output.splitlines()[0]

    except Exception:
        pass

    return "unknown"


# ============================================================
# RUN STATS
# ============================================================

def get_next_run_number():
    if not isinstance(run_stats, dict):
        return 1

    runs = run_stats.get("runs", [])

    if not isinstance(runs, list):
        return 1

    return len(runs) + 1


def save_run_report(report):
    if not isinstance(run_stats, dict):
        run_stats.clear()

    runs = run_stats.setdefault(
        "runs",
        []
    )

    runs.append(report)

    save_json_stats(
        RUN_STATS_FILE,
        run_stats
    )


# ============================================================
# MAIN
# ============================================================

def main():

    global current_run_number
    global SOURCES

    SOURCES = load_sources()

    current_run_number = get_next_run_number()

    logger.info(
        "=== VLESS BULK TESTER ==="
    )

    logger.info(
        "sing-box: %s",
        get_singbox_version()
    )

    logger.info(
        "Sources: %d",
        len(SOURCES)
    )

    # --------------------------------------------------------
    # RUN INITIALIZATION
    # --------------------------------------------------------

    run_started = datetime.now().isoformat()

    for source in SOURCES:
        ensure_source_bucket(source)

        source_stats[source]["runs"] += 1
        source_stats[source]["last_run"] = run_started

    # --------------------------------------------------------
    # STEP 1 — DOWNLOAD SOURCES
    # --------------------------------------------------------

    all_nodes = []
    source_nodes = defaultdict(set)

    for source in SOURCES:

        result = fetch_source(source)

        ensure_source_bucket(source)

        bucket = source_stats[source]

        bucket["fetch_attempts"] += 1

        if result["success"]:
            bucket["fetch_success"] += 1

            bucket["lines_total"] = result[
                "lines_total"
            ]

            bucket["vless_total"] = len(
                result["nodes"]
            )

            for uri in result["nodes"]:
                source_nodes[source].add(uri)
                all_nodes.append(uri)

        else:
            bucket["lines_total"] = 0
            bucket["vless_total"] = 0

    # --------------------------------------------------------
    # STEP 2 — UNIQUE NODES
    # --------------------------------------------------------

    unique_nodes = []
    seen = set()

    for uri in all_nodes:

        if uri in seen:
            continue

        seen.add(uri)
        unique_nodes.append(uri)

    # Source membership must be preserved.
    for source, nodes in source_nodes.items():

        source_stats[source]["unique_nodes"] = len(nodes)

        for uri in nodes:
            node_id = node_id_from_uri(uri)
            record_node_source(
                node_id,
                source
            )

    logger.info(
        "VLESS found: %d",
        len(all_nodes)
    )

    logger.info(
        "Unique nodes: %d",
        len(unique_nodes)
    )

    # --------------------------------------------------------
    # STEP 3 — TEST ALL FRESH NODES
    # --------------------------------------------------------

    fresh_results = []

    with ThreadPoolExecutor(
        max_workers=MAX_THREADS
    ) as executor:

        futures = [
            executor.submit(
                test_node,
                uri
            )
            for uri in unique_nodes
        ]

        for future in futures:
            try:
                result = future.result()

                fresh_results.append(result)

                update_source_result(
                    result["node_id"],
                    result["live"],
                    run_started
                )

            except Exception:
                pass

    # --------------------------------------------------------
    # FRESH LIVE
    # --------------------------------------------------------

    fresh_live = [
        result["uri"]
        for result in fresh_results
        if result["live"]
    ]

    fresh_dead = {
        result["uri"]
        for result in fresh_results
        if not result["live"]
    }

    # --------------------------------------------------------
    # STEP 4 — ARCHIVE
    # --------------------------------------------------------

    old_archive = load_archive()

    # Fresh nodes that were tested this run are not
    # tested again as archive candidates.
    fresh_set = set(unique_nodes)

    archive_candidates = [
        uri
        for uri in old_archive
        if uri not in fresh_set
    ]

    final_live = list(fresh_live)

    # --------------------------------------------------------
    # If fewer than 100 fresh LIVE:
    # recheck archive candidates.
    # --------------------------------------------------------

    if len(final_live) < MAX_NODES:

        need = MAX_NODES - len(final_live)

        archive_results = []

        candidates = archive_candidates[:]

        if candidates:
            logger.info(
                "Archive recheck: %d",
                len(candidates)
            )

        with ThreadPoolExecutor(
            max_workers=MAX_THREADS
        ) as executor:

            futures = [
                executor.submit(
                    test_node,
                    uri
                )
                for uri in candidates
            ]

            for future in futures:

                if len(final_live) >= MAX_NODES:
                    break

                try:
                    result = future.result()

                    archive_results.append(
                        result
                    )

                    update_source_result(
                        result["node_id"],
                        result["live"],
                        run_started
                    )

                    if result["live"]:
                        final_live.append(
                            result["uri"]
                        )

                except Exception:
                    pass

    # --------------------------------------------------------
    # BUILD NEW ARCHIVE
    # --------------------------------------------------------

    archive_live = []

    # Existing archive nodes that were not checked this run
    # remain in archive.
    checked_archive = {
        result["uri"]
        for result in locals().get(
            "archive_results",
            []
        )
    }

    archive_live.extend(
        uri
        for uri in old_archive
        if uri not in fresh_set
        and uri not in checked_archive
    )

    # Add all fresh LIVE.
    archive_live.extend(
        fresh_live
    )

    # Add archive nodes that survived recheck.
    for result in locals().get(
        "archive_results",
        []
    ):
        if result["live"]:
            archive_live.append(
                result["uri"]
            )

    # Remove exact duplicates.
    archive_unique = []
    archive_seen = set()

    for uri in archive_live:
        if uri in archive_seen:
            continue

        archive_seen.add(uri)
        archive_unique.append(uri)

    save_archive(
        archive_unique
    )

    # --------------------------------------------------------
    # FINAL SUBSCRIPTION
    # --------------------------------------------------------

    # Fresh LIVE first, then archive LIVE.
    subscription_nodes = []

    for uri in final_live:
        if uri not in subscription_nodes:
            subscription_nodes.append(uri)

        if len(subscription_nodes) >= MAX_NODES:
            break

    if len(subscription_nodes) < MAX_NODES:

        for uri in archive_unique:

            if uri in subscription_nodes:
                continue

            subscription_nodes.append(uri)

            if len(subscription_nodes) >= MAX_NODES:
                break

    # --------------------------------------------------------
    # IMPORTANT:
    # 0 LIVE -> DO NOT TOUCH EXISTING SUBSCRIPTION
    # --------------------------------------------------------

    if len(subscription_nodes) > 0:

        with open(
            SUBSCRIPTION_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            for uri in subscription_nodes:
                f.write(uri + "\n")

    # --------------------------------------------------------
    # SAVE STATS
    # --------------------------------------------------------

    prune_node_stats()

    save_json_stats(
        SERVICE_STATS_FILE,
        service_stats
    )

    save_json_stats(
        NODE_STATS_FILE,
        node_stats
    )

    save_json_stats(
        SOURCE_STATS_FILE,
        source_stats
    )

    # --------------------------------------------------------
    # RUN REPORT
    # --------------------------------------------------------

    run_finished = datetime.now().isoformat()

    live_count = len(fresh_live)

    archive_checked_count = len(
        locals().get(
            "archive_results",
            []
        )
    )

    archive_live_count = sum(
        1
        for result in locals().get(
            "archive_results",
            []
        )
        if result["live"]
    )

    report = {
        "run": current_run_number,
        "started": run_started,
        "finished": run_finished,

        "sources": len(SOURCES),

        "vless_found": len(all_nodes),
        "unique_nodes": len(unique_nodes),

        "fresh_tested": len(fresh_results),
        "fresh_live": live_count,
        "fresh_dead": len(fresh_dead),

        "archive_before": len(old_archive),
        "archive_rechecked": archive_checked_count,
        "archive_live_after_recheck": archive_live_count,

        "archive_after": len(archive_unique),

        "subscription_count": len(
            subscription_nodes
        ),

        "services": TEST_URLS,
        "live_rule": "3 of 5"
    }

    save_run_report(
        report
    )

    # --------------------------------------------------------
    # FINAL OUTPUT
    # --------------------------------------------------------

    logger.info(
        "LIVE: %d",
        live_count
    )

    logger.info(
        "Subscription: %d",
        len(subscription_nodes)
    )

    logger.info(
        "Archive: %d",
        len(archive_unique)
    )


if __name__ == "__main__":
    main()
