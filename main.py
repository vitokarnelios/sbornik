#!/usr/bin/env python3

import os
import requests
import base64
import json
import subprocess
import time
import queue
import threading
import logging
import hashlib

from collections import defaultdict
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor


# =========================================================
# PATHS
# =========================================================

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

FINAL_DIR = os.path.join(BASE_PATH, "subs")
LOG_DIR = os.path.join(BASE_PATH, "logs")

os.makedirs(FINAL_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# =========================================================
# LOGGING
# =========================================================

log_file = os.path.join(
    LOG_DIR,
    f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# =========================================================
# FILES
# =========================================================

SOURCES_FILE = os.path.join(
    BASE_PATH,
    "sources.txt"
)

SERVICE_STATS_FILE = os.path.join(
    BASE_PATH,
    "service_stats.json"
)

NODE_STATS_FILE = os.path.join(
    BASE_PATH,
    "node_stats.json"
)

SOURCE_STATS_FILE = os.path.join(
    BASE_PATH,
    "source_stats.json"
)

RUN_STATS_FILE = os.path.join(
    BASE_PATH,
    "run_stats.json"
)

# ЕДИНСТВЕННЫЙ архив LIVE
ALIVE_ARCHIVE_FILE = os.path.join(
    FINAL_DIR,
    "live_archive.txt"
)

SUBSCRIPTION_FILE = os.path.join(
    FINAL_DIR,
    "vless_001.txt"
)


# =========================================================
# LIMITS
# =========================================================

MAX_NODES = 100
MAX_THREADS = 40

MAX_NODE_STATS = 30000


# =========================================================
# SOURCES
# =========================================================

if not os.path.exists(SOURCES_FILE):
    logger.error("Файл sources.txt не найден")
    raise SystemExit(1)


with open(
    SOURCES_FILE,
    "r",
    encoding="utf-8"
) as f:

    SOURCES = [
        line.strip()
        for line in f
        if line.strip()
        and not line.strip().startswith("#")
    ]


# =========================================================
# JSON HELPERS
# =========================================================

def load_json_stats(path, default_factory):

    if not os.path.exists(path):
        return default_factory()

    try:

        with open(
            path,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        return (
            data
            if isinstance(data, dict)
            else default_factory()
        )

    except Exception as e:

        logger.warning(
            f"Ошибка загрузки "
            f"{os.path.basename(path)}: {e}"
        )

        return default_factory()


def save_json_stats(
    path,
    data,
    label
):

    tmp = path + ".tmp"

    try:

        with open(
            tmp,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                data,
                f,
                indent=2,
                ensure_ascii=False
            )

        os.replace(
            tmp,
            path
        )

        logger.info(
            f"💾 {label}: "
            f"{os.path.basename(path)}"
        )

    except Exception as e:

        logger.warning(
            f"Ошибка сохранения {label}: {e}"
        )

        try:

            if os.path.exists(tmp):
                os.remove(tmp)

        except Exception:
            pass


# =========================================================
# SOURCE STATS
# =========================================================

SOURCE_STATS = load_json_stats(
    SOURCE_STATS_FILE,
    lambda: {}
)


def ensure_source_bucket(source):

    bucket = SOURCE_STATS.setdefault(
        source,
        {
            "runs": 0,
            "fetch_attempts": 0,
            "fetch_success": 0,
            "lines_total": 0,
            "vless_total": 0,
            "unique_nodes": 0,
            "nodes_tested": 0,
            "original_live": 0,
            "dead": 0,
            "last_run": ""
        }
    )

    return bucket


# =========================================================
# NODE STATS
# =========================================================

NODE_STATS = load_json_stats(
    NODE_STATS_FILE,
    lambda: {}
)


def node_id(vless_uri):

    return hashlib.sha256(
        vless_uri.encode(
            "utf-8",
            errors="ignore"
        )
    ).hexdigest()[:24]


def extract_sni(vless_uri):

    try:

        q = parse_qs(
            urlparse(vless_uri).query
        )

        return q.get(
            "sni",
            [""]
        )[0].strip().lower()

    except Exception:

        return ""


def ensure_node_record(vless_uri):

    nid = node_id(vless_uri)

    rec = NODE_STATS.setdefault(
        nid,
        {
            "attempts": 0,
            "live": 0,
            "original_live": 0,
            "dead": 0,
            "last_status": "unknown",
            "original_sni": extract_sni(vless_uri),
            "sources": [],
            "first_seen": "",
            "last_seen": ""
        }
    )

    return nid, rec


def record_node_source(
    vless_uri,
    source_names
):

    nid, rec = ensure_node_record(
        vless_uri
    )

    now = datetime.now().isoformat(
        timespec="seconds"
    )

    if not rec.get("first_seen"):
        rec["first_seen"] = now

    rec["last_seen"] = now

    sources = rec.setdefault(
        "sources",
        []
    )

    for src in source_names:

        if src not in sources:
            sources.append(src)

    if len(sources) > 30:
        del sources[:-30]

    return nid, rec


def prune_node_stats():

    if len(NODE_STATS) <= MAX_NODE_STATS:
        return

    ranked = sorted(
        NODE_STATS.items(),
        key=lambda kv:
            kv[1].get(
                "last_seen",
                ""
            ),
        reverse=True
    )[:MAX_NODE_STATS]

    NODE_STATS.clear()

    NODE_STATS.update(
        dict(ranked)
    )


# =========================================================
# PORT QUEUE
# =========================================================

port_queue = queue.Queue()

for i in range(MAX_THREADS):

    port_queue.put(
        11000 + i
    )


# =========================================================
# TEST SITES
# =========================================================

TEST_URLS = [
    "https://telegram.org",
    "https://www.instagram.com",
    "https://www.youtube.com",
    "https://gemini.google.com",
    "https://www.google.com"
]


# =========================================================
# BASE64
# =========================================================

def decode_base64_content(text):

    try:

        text = text.strip()

        if "://" in text:
            return [text]

        padding = len(text) % 4

        if padding:
            text += "=" * (
                4 - padding
            )

        decoded = base64.b64decode(
            text
        ).decode(
            "utf-8",
            errors="ignore"
        )

        return decoded.splitlines()

    except Exception:

        return []


# =========================================================
# FETCH SOURCE
# =========================================================

def fetch_source(url):

    try:

        headers = {
            "User-Agent":
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36"
        }

        r = requests.get(
            url,
            timeout=15,
            headers=headers
        )

        if r.status_code != 200:
            return []

        final_lines = []

        for line in r.text.splitlines():

            line = line.strip()

            if not line:
                continue

            if (
                "://" not in line
                and len(line) > 50
            ):

                final_lines.extend(
                    decode_base64_content(
                        line
                    )
                )

            else:

                final_lines.append(
                    line
                )

        return final_lines

    except Exception as e:

        logger.debug(
            f"Ошибка загрузки "
            f"{url}: {e}"
        )

        return []


# =========================================================
# VLESS
# =========================================================

def is_valid_vless(line):

    return line.lower().startswith(
        "vless://"
    )


# =========================================================
# VLESS → SING-BOX JSON
# НЕ МЕНЯТЬ
# =========================================================

def parse_vless_to_json(
    vless_uri,
    listen_port
):

    try:

        parsed = urlparse(vless_uri)

        netloc = parsed.netloc

        if "@" not in netloc:
            return None

        uuid, server_part = netloc.split(
            "@",
            1
        )

        if ":" in server_part:

            server_address, server_port_str = (
                server_part.split(":", 1)
            )

            server_port = int(
                server_port_str
            )

        else:

            server_address = server_part
            server_port = 443

        query_params = parse_qs(
            parsed.query
        )

        params = {
            k.lower(): v[0]
            for k, v in query_params.items()
            if v
        }

        node_name = (
            unquote(parsed.fragment)
            if parsed.fragment
            else "vless-node"
        )

        outbound = {
            "type": "vless",
            "tag": node_name,
            "server": server_address,
            "server_port": server_port,
            "uuid": uuid,
        }

        flow = params.get(
            "flow",
            ""
        )

        if flow:
            outbound["flow"] = flow

        security = params.get(
            "security",
            ""
        )

        transport_type = params.get(
            "type",
            "tcp"
        )

        # ===== REALITY =====

        if (
            "pbk" in params
            or params.get("security") == "reality"
            or security == "reality"
        ):

            outbound["tls"] = {

                "enabled": True,

                "server_name":
                    params.get(
                        "sni",
                        server_address
                    ),

                "utls": {
                    "enabled": True,
                    "fingerprint":
                        params.get(
                            "fp",
                            "chrome"
                        )
                },

                "reality": {
                    "enabled": True,
                    "public_key":
                        params.get(
                            "pbk",
                            ""
                        ),
                    "short_id":
                        params.get(
                            "sid",
                            ""
                        )
                }
            }

        # ===== TLS =====

        elif (
            security == "tls"
            or params.get("security") == "tls"
        ):

            outbound["tls"] = {

                "enabled": True,

                "server_name":
                    params.get(
                        "sni",
                        server_address
                    ),

                "utls": {
                    "enabled": True,
                    "fingerprint":
                        params.get(
                            "fp",
                            "chrome"
                        )
                }
            }

            if "alpn" in params:

                outbound["tls"]["alpn"] = (
                    params["alpn"].split(",")
                )

        # ===== GRPC =====

        if transport_type == "grpc":

            outbound["transport"] = {

                "type": "grpc",

                "service_name":
                    params.get(
                        "serviceName",
                        ""
                    )
            }

        # ===== XHTTP =====

        elif transport_type == "xhttp":

            outbound["transport"] = {

                "type": "xhttp",

                "path":
                    params.get(
                        "path",
                        "/"
                    ),

                "host":
                    [params["host"]]
                    if params.get("host")
                    else [],

                "mode":
                    params.get(
                        "mode",
                        "auto"
                    )
            }

        # ===== WS =====

        elif transport_type == "ws":

            outbound["transport"] = {

                "type": "ws",

                "path":
                    params.get(
                        "path",
                        "/"
                    ),

                "headers": {
                    "Host":
                        params.get(
                            "host",
                            server_address
                        )
                }
            }

        config = {

            "log": {
                "level": "error"
            },

            "inbounds": [

                {
                    "type": "socks",

                    "tag": "socks-in",

                    "listen":
                        "127.0.0.1",

                    "listen_port":
                        listen_port
                }
            ],

            "outbounds": [
                outbound
            ]
        }

        return config

    except Exception:

        return None


# =========================================================
# SERVICE STATS
# =========================================================

def empty_service_stats():

    return {
        url: {
            "attempts": 0,
            "success": 0
        }
        for url in TEST_URLS
    }


def load_service_stats():

    if not os.path.exists(
        SERVICE_STATS_FILE
    ):

        return empty_service_stats()

    try:

        with open(
            SERVICE_STATS_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            loaded = json.load(f)

        stats = {}

        for url, value in loaded.items():

            if isinstance(value, dict):

                stats[url] = {
                    "attempts": int(
                        value.get(
                            "attempts",
                            0
                        )
                    ),
                    "success": int(
                        value.get(
                            "success",
                            0
                        )
                    )
                }

        # Старый ChatGPT не переносим
        # в новую статистику.

        for url in TEST_URLS:

            if url not in stats:

                stats[url] = {
                    "attempts": 0,
                    "success": 0
                }

        return stats

    except Exception:

        return empty_service_stats()


def save_service_stats(stats):

    try:

        with open(
            SERVICE_STATS_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                stats,
                f,
                indent=2,
                ensure_ascii=False
            )

    except Exception as e:

        logger.warning(
            f"Ошибка сохранения "
            f"статистики сервисов: {e}"
        )


SERVICE_STATS = load_service_stats()


# =========================================================
# CHECK ONE NODE
# =========================================================

def check_single_uri(
    vless_uri,
    local_port,
    service_stats=None,
    stats_lock=None
):

    temp_config_path = os.path.join(
        BASE_PATH,
        f"temp_{local_port}.json"
    )

    temp_log_path = os.path.join(
        BASE_PATH,
        f"singbox_{local_port}.log"
    )

    config = parse_vless_to_json(
        vless_uri,
        local_port
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
            f
        )

    proc = None
    log_file = None

    try:

        log_file = open(
            temp_log_path,
            "w",
            encoding="utf-8"
        )

        proc = subprocess.Popen(
            [
                "sing-box",
                "run",
                "-c",
                temp_config_path
            ],
            stdout=log_file,
            stderr=log_file
        )

        for _ in range(15):
            time.sleep(0.1)

        if proc.poll() is not None:
            return False

        proxies = {
            "http":
                f"socks5h://127.0.0.1:{local_port}",

            "https":
                f"socks5h://127.0.0.1:{local_port}"
        }

        headers = {
            "User-Agent":
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/154.0.0.0 "
                "Safari/537.36"
        }

        success_count = 0

        # ВСЕ 5 сайтов проверяются.
        # Даже после получения 3 успехов.

        for url in TEST_URLS:

            try:

                response = requests.get(
                    url,
                    proxies=proxies,
                    timeout=2,
                    headers=headers,
                    allow_redirects=True
                )

                success = response.status_code in [
                    200,
                    204,
                    301,
                    302
                ]

                if service_stats is not None:

                    if stats_lock:

                        with stats_lock:

                            service_stats[url][
                                "attempts"
                            ] += 1

                            if success:

                                service_stats[url][
                                    "success"
                                ] += 1

                    else:

                        service_stats[url][
                            "attempts"
                        ] += 1

                        if success:

                            service_stats[url][
                                "success"
                            ] += 1

                if success:
                    success_count += 1

            except Exception:

                if service_stats is not None:

                    if stats_lock:

                        with stats_lock:

                            service_stats[url][
                                "attempts"
                            ] += 1

                    else:

                        service_stats[url][
                            "attempts"
                        ] += 1

                continue

        # LIVE = минимум 3 из 5

        return success_count >= 3

    except Exception:

        return False

    finally:

        try:

            if log_file:
                log_file.close()

        except Exception:
            pass

        if proc:

            proc.terminate()

            try:

                proc.wait(
                    timeout=1
                )

            except subprocess.TimeoutExpired:

                proc.kill()

        for f in [
            temp_config_path,
            temp_log_path
        ]:

            if os.path.exists(f):

                try:
                    os.remove(f)

                except Exception:
                    pass


# =========================================================
# WORKER
# =========================================================

def worker(
    task_queue,
    result_list,
    lock,
    node_sources_map,
    run_stats,
    count_source_stats=True,
    archive_mode=False
):

    while True:

        try:

            vless_uri = task_queue.get(
                timeout=0.5
            )

        except queue.Empty:

            break

        local_port = port_queue.get()

        try:

            nid, node_rec = record_node_source(
                vless_uri,
                node_sources_map.get(
                    vless_uri,
                    []
                )
            )

            with lock:

                node_rec["attempts"] = (
                    node_rec.get(
                        "attempts",
                        0
                    ) + 1
                )

                node_rec["last_status"] = (
                    "testing"
                )

                run_stats[
                    "nodes_tested"
                ] += 1

                if archive_mode:

                    run_stats[
                        "archive_nodes_tested"
                    ] += 1

            is_alive = check_single_uri(
                vless_uri,
                local_port,
                SERVICE_STATS,
                lock
            )

            with lock:

                if is_alive:

                    result_list.append(
                        vless_uri
                    )

                    node_rec["live"] += 1
                    node_rec[
                        "original_live"
                    ] += 1

                    node_rec[
                        "last_status"
                    ] = "original_live"

                    run_stats[
                        "original_live"
                    ] += 1

                    if archive_mode:

                        run_stats[
                            "archive_live"
                        ] += 1

                    if count_source_stats:

                        for src in node_sources_map.get(
                            vless_uri,
                            []
                        ):

                            bucket = (
                                ensure_source_bucket(
                                    src
                                )
                            )

                            bucket[
                                "nodes_tested"
                            ] += 1

                            bucket[
                                "original_live"
                            ] += 1

                else:

                    node_rec["dead"] += 1

                    node_rec[
                        "last_status"
                    ] = "dead"

                    run_stats[
                        "dead"
                    ] += 1

                    if archive_mode:

                        run_stats[
                            "archive_dead"
                        ] += 1

                    if count_source_stats:

                        for src in node_sources_map.get(
                            vless_uri,
                            []
                        ):

                            bucket = (
                                ensure_source_bucket(
                                    src
                                )
                            )

                            bucket[
                                "nodes_tested"
                            ] += 1

                            bucket[
                                "dead"
                            ] += 1

        except Exception as e:

            logger.debug(
                f"Ошибка проверки ноды: {e}"
            )

            with lock:

                run_stats[
                    "dead"
                ] += 1

                if archive_mode:

                    run_stats[
                        "archive_dead"
                    ] += 1

        finally:

            port_queue.put(
                local_port
            )

            task_queue.task_done()


# =========================================================
# RUN CHECKS
# =========================================================

def run_node_checks(
    nodes,
    node_sources_map,
    run_stats,
    count_source_stats=True,
    archive_mode=False
):

    if not nodes:
        return []

    task_queue = queue.Queue()

    for node in nodes:
        task_queue.put(node)

    result_list = []

    lock = threading.Lock()

    with ThreadPoolExecutor(
        max_workers=MAX_THREADS
    ) as executor:

        executor.map(
            lambda _:
                worker(
                    task_queue,
                    result_list,
                    lock,
                    node_sources_map,
                    run_stats,
                    count_source_stats,
                    archive_mode
                ),
            range(MAX_THREADS)
        )

    return result_list


# =========================================================
# ARCHIVE
# =========================================================

def load_alive_archive():

    if not os.path.exists(
        ALIVE_ARCHIVE_FILE
    ):

        return []

    try:

        with open(
            ALIVE_ARCHIVE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            nodes = [
                line.strip()
                for line in f
                if (
                    line.strip()
                    and is_valid_vless(
                        line.strip()
                    )
                )
            ]

        return list(
            dict.fromkeys(nodes)
        )

    except Exception as e:

        logger.warning(
            f"Ошибка загрузки "
            f"live_archive.txt: {e}"
        )

        return []


def save_alive_archive(nodes):

    unique = list(
        dict.fromkeys(
            node
            for node in nodes
            if (
                node
                and is_valid_vless(node)
            )
        )
    )

    tmp = (
        ALIVE_ARCHIVE_FILE
        + ".tmp"
    )

    with open(
        tmp,
        "w",
        encoding="utf-8"
    ) as f:

        if unique:

            f.write(
                "\n".join(unique)
            )

            f.write("\n")

    os.replace(
        tmp,
        ALIVE_ARCHIVE_FILE
    )


# =========================================================
# RUN HISTORY
# =========================================================

def load_run_history():

    if not os.path.exists(
        RUN_STATS_FILE
    ):

        return {
            "runs": []
        }

    try:

        with open(
            RUN_STATS_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        if (
            isinstance(data, dict)
            and isinstance(
                data.get("runs"),
                list
            )
        ):

            return data

        # Совместимость со старым
        # форматом одного запуска.

        if isinstance(data, dict):

            return {
                "runs": [data]
            }

    except Exception:

        pass

    return {
        "runs": []
    }


def save_run_history(run_stats):

    history = load_run_history()

    history.setdefault(
        "runs",
        []
    )

    history["runs"].append(
        run_stats
    )

    save_json_stats(
        RUN_STATS_FILE,
        history,
        "Единый отчёт по запускам"
    )


# =========================================================
# MAIN
# =========================================================

def main():

    logger.info(
        "=== SING-BOX STATUS ==="
    )

    try:

        sb_v = subprocess.run(
            ["sing-box", "version"],
            capture_output=True,
            text=True
        )

        logger.info(
            sb_v.stdout.strip()
        )

    except Exception as e:

        logger.error(
            f"sing-box не найден! {e}"
        )

        return

    # =====================================================
    # RUN STATS
    # =====================================================

    started_at = datetime.now().isoformat(
        timespec="seconds"
    )

    run_stats = {

        "started_at":
            started_at,

        "sources":
            len(SOURCES),

        "source_fetch_success":
            0,

        "lines_total":
            0,

        "vless_total":
            0,

        "unique_nodes":
            0,

        "nodes_tested":
            0,

        "original_live":
            0,

        "dead":
            0,

        "archive_nodes_tested":
            0,

        "archive_live":
            0,

        "archive_dead":
            0
    }

    # =====================================================
    # STEP 1 — SOURCES
    # =====================================================

    logger.info(
        "\n--- STEP 1: FETCHING SOURCES ---"
    )

    all_nodes = []

    source_nodes = defaultdict(list)

    for url in SOURCES:

        bucket = ensure_source_bucket(
            url
        )

        bucket["runs"] += 1

        bucket["fetch_attempts"] += 1

        bucket["last_run"] = (
            started_at
        )

        nodes = fetch_source(
            url
        )

        if nodes:

            bucket[
                "fetch_success"
            ] += 1

            run_stats[
                "source_fetch_success"
            ] += 1

        bucket[
            "lines_total"
        ] += len(nodes)

        run_stats[
            "lines_total"
        ] += len(nodes)

        logger.info(
            f"Loaded {len(nodes)} lines "
            f"from {url[:80]}..."
        )

        all_nodes.extend(
            nodes
        )

        for line in nodes:

            if is_valid_vless(line):

                line = line.strip()

                source_nodes[
                    url
                ].append(line)

                bucket[
                    "vless_total"
                ] += 1

                run_stats[
                    "vless_total"
                ] += 1

    # =====================================================
    # STEP 2 — UNIQUE
    # =====================================================

    logger.info(
        "\n--- STEP 2: VALIDATION ---"
    )

    unique_nodes = []

    seen = set()

    for line in all_nodes:

        line = line.strip()

        if not is_valid_vless(line):
            continue

        if line in seen:
            continue

        seen.add(line)

        unique_nodes.append(
            line
        )

    run_stats[
        "unique_nodes"
    ] = len(unique_nodes)

    logger.info(
        f"Unique VLESS configs: "
        f"{len(unique_nodes)}"
    )

    # =====================================================
    # SOURCE → NODE MAP
    # =====================================================

    node_sources_map = defaultdict(
        list
    )

    for src, nodes in source_nodes.items():

        unique_source_nodes = set(
            nodes
        )

        bucket = ensure_source_bucket(
            src
        )

        bucket[
            "unique_nodes"
        ] += len(
            unique_source_nodes
        )

        for node in unique_source_nodes:

            node_sources_map[
                node
            ].append(src)

            nid, rec = ensure_node_record(
                node
            )

            if src not in rec.setdefault(
                "sources",
                []
            ):

                rec[
                    "sources"
                ].append(src)

            if len(
                rec["sources"]
            ) > 30:

                del rec[
                    "sources"
                ][:-30]

    # =====================================================
    # LOAD CUMULATIVE ARCHIVE
    # =====================================================

    archive_before = (
        load_alive_archive()
    )

    archive_set = set(
        archive_before
    )

    # =====================================================
    # STEP 3 — ALL FRESH NODES
    # =====================================================

    logger.info(
        f"\n--- STEP 3: LIVE CHECK "
        f"({len(unique_nodes)} nodes, "
        f"threads: {MAX_THREADS}) ---"
    )

    start_time = time.time()

    fresh_results = run_node_checks(
        unique_nodes,
        node_sources_map,
        run_stats,
        count_source_stats=True,
        archive_mode=False
    )

    elapsed = (
        time.time()
        - start_time
    )

    logger.info(
        f"⏱️ Время проверки: "
        f"{elapsed:.1f} сек"
    )

    fresh_live_nodes = list(
        dict.fromkeys(
            fresh_results
        )
    )

    fresh_live_set = set(
        fresh_live_nodes
    )

    fresh_dead_set = (
        set(unique_nodes)
        - fresh_live_set
    )

    logger.info(
        f"\n--- RESULT: Found "
        f"{len(fresh_live_nodes)} "
        f"live nodes ---"
    )

    # =====================================================
    # UPDATE ARCHIVE:
    # свежие DEAD удаляем,
    # свежие LIVE добавляем
    # =====================================================

    new_archive = []

    for node in archive_before:

        # Нода была свежей.
        # Если она DEAD — удалить.
        # Если LIVE — добавим ниже заново.
        if node in unique_nodes:
            continue

        new_archive.append(
            node
        )

    # Все свежие LIVE
    # добавляются в накопительный архив.

    for node in fresh_live_nodes:

        if node not in new_archive:

            new_archive.append(
                node
            )

    # =====================================================
    # ARCHIVE RECHECK
    #
    # Проверяем архив только если
    # свежих LIVE меньше 100.
    #
    # Проверяем партиями, пока не наберём
    # достаточно LIVE.
    # =====================================================

    archive_live_nodes = []

    need = (
        MAX_NODES
        - len(fresh_live_nodes)
    )

    archive_candidates = [
        node
        for node in archive_before
        if (
            node not in unique_nodes
            and node not in fresh_live_set
        )
    ]

    archive_checked = set()

    if need > 0 and archive_candidates:

        logger.info(
            f"\n--- ARCHIVE RECHECK "
            f"({len(archive_candidates)} "
            f"candidates, need {need}) ---"
        )

        # Проверяем партиями.
        # Это позволяет не гонять весь огромный
        # архив, когда 100 LIVE уже набраны.

        position = 0

        while (
            position < len(archive_candidates)
            and len(archive_live_nodes) < need
        ):

            batch = archive_candidates[
                position:
                position + MAX_THREADS
            ]

            position += len(batch)

            batch_results = run_node_checks(
                batch,
                node_sources_map,
                run_stats,
                count_source_stats=False,
                archive_mode=True
            )

            batch_live = list(
                dict.fromkeys(
                    batch_results
                )
            )

            archive_live_nodes.extend(
                batch_live
            )

            archive_checked.update(
                batch
            )

            # Чтобы не было дублей.

            archive_live_nodes = list(
                dict.fromkeys(
                    archive_live_nodes
                )
            )

        archive_live_set = set(
            archive_live_nodes
        )

    else:

        archive_live_set = set()

    # =====================================================
    # ARCHIVE UPDATE AFTER RECHECK
    # =====================================================

    # Удаляем из архива проверенные DEAD.
    # Проверенные LIVE оставляем.
    # Непроверенные старые остаются.

    final_archive = []

    for node in archive_before:

        # Свежая нода уже обработана выше.
        if node in unique_nodes:
            continue

        # Архивная нода была перепроверена.
        if node in archive_checked:

            if node in archive_live_set:

                final_archive.append(
                    node
                )

            # DEAD сюда не возвращаем.

            continue

        # Архивная нода ещё не проверялась.
        # Оставляем её для следующих запусков.

        final_archive.append(
            node
        )

    # Свежие LIVE.

    for node in fresh_live_nodes:

        if node not in final_archive:

            final_archive.append(
                node
            )

    # LIVE из архивной перепроверки.

    for node in archive_live_nodes:

        if node not in final_archive:

            final_archive.append(
                node
            )

    final_archive = list(
        dict.fromkeys(
            final_archive
        )
    )

    # БЕЗ ЛИМИТА.
    save_alive_archive(
        final_archive
    )

    # =====================================================
    # FINAL SUBSCRIPTION
    # =====================================================

    alive_nodes = []

    # Сначала свежие LIVE.

    for node in fresh_live_nodes:

        if node not in alive_nodes:

            alive_nodes.append(
                node
            )

        if len(alive_nodes) >= MAX_NODES:
            break

    # Затем архивные LIVE.

    if len(alive_nodes) < MAX_NODES:

        for node in archive_live_nodes:

            if node in alive_nodes:
                continue

            alive_nodes.append(
                node
            )

            if len(alive_nodes) >= MAX_NODES:
                break

    logger.info(
        f"Final LIVE: "
        f"{len(alive_nodes)}"
    )

    logger.info(
        f"live_archive.txt updated: "
        f"{len(final_archive)} nodes"
    )

    # =====================================================
    # ZERO LIVE
    # =====================================================

    if not alive_nodes:

        logger.warning(
            "0 live nodes, "
            "skipping subscription update"
        )

        logger.warning(
            "Существующая "
            "vless_001.txt НЕ изменена"
        )

    else:

        with open(
            SUBSCRIPTION_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            f.write(
                "\n".join(
                    alive_nodes[:MAX_NODES]
                )
            )

            f.write("\n")

        logger.info(
            f"Subscription updated: "
            f"{SUBSCRIPTION_FILE}"
        )

    # =====================================================
    # PROTOCOL STATS
    # =====================================================

    types_count = {
        "reality": 0,
        "tls": 0,
        "grpc": 0,
        "xhttp": 0,
        "ws": 0,
        "other": 0
    }

    for node in alive_nodes:

        if (
            "security=reality"
            in node
            or "pbk=" in node
        ):

            types_count[
                "reality"
            ] += 1

        elif "type=grpc" in node:

            types_count[
                "grpc"
            ] += 1

        elif "type=xhttp" in node:

            types_count[
                "xhttp"
            ] += 1

        elif "type=ws" in node:

            types_count[
                "ws"
            ] += 1

        elif "security=tls" in node:

            types_count[
                "tls"
            ] += 1

        else:

            types_count[
                "other"
            ] += 1

    logger.info(
        "\n--- STATS BY PROTOCOL ---"
    )

    for proto, count in types_count.items():

        if count > 0:

            logger.info(
                f"{proto}: {count}"
            )

    # =====================================================
    # SERVICE STATS
    # =====================================================

    logger.info(
        "\n--- TEST SITE STATS ---"
    )

    for url in TEST_URLS:

        data = SERVICE_STATS.get(
            url,
            {
                "attempts": 0,
                "success": 0
            }
        )

        attempts = data.get(
            "attempts",
            0
        )

        success = data.get(
            "success",
            0
        )

        rate = (
            success / attempts * 100
            if attempts
            else 0
        )

        logger.info(
            f"{url}: "
            f"{success}/{attempts} "
            f"успешных "
            f"({rate:.1f}%)"
        )

    # =====================================================
    # SAVE STATS
    # =====================================================

    save_service_stats(
        SERVICE_STATS
    )

    prune_node_stats()

    save_json_stats(
        SOURCE_STATS_FILE,
        SOURCE_STATS,
        "Статистика источников"
    )

    save_json_stats(
        NODE_STATS_FILE,
        NODE_STATS,
        "Статистика нод"
    )

    # =====================================================
    # RUN REPORT
    # =====================================================

    run_stats[
        "finished_at"
    ] = datetime.now().isoformat(
        timespec="seconds"
    )

    run_stats[
        "fresh_live"
    ] = len(
        fresh_live_nodes
    )

    run_stats[
        "fresh_dead"
    ] = len(
        fresh_dead_set
    )

    run_stats[
        "archive_before"
    ] = len(
        archive_before
    )

    run_stats[
        "archive_candidates"
    ] = len(
        archive_candidates
    )

    run_stats[
        "archive_rechecked"
    ] = len(
        archive_checked
    )

    run_stats[
        "archive_live"
    ] = len(
        archive_live_nodes
    )

    run_stats[
        "archive_dead"
    ] = (
        len(archive_checked)
        - len(archive_live_set)
    )

    run_stats[
        "final_live"
    ] = len(
        alive_nodes
    )

    run_stats[
        "archive_after"
    ] = len(
        final_archive
    )

    save_run_history(
        run_stats
    )


# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    main()
