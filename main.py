#!/usr/bin/env python3
"""
SING-BOX VLESS MAIN TESTER

Логика:
- Загружает VLESS из sources.txt
- Дедуплицирует одинаковые VLESS URI
- Проверяет каждую уникальную ноду через sing-box
- Использует ОРИГИНАЛЬНЫЙ SNI из VLESS
- SNI mutation полностью отключена
- Проверяет 5 сервисов
- LIVE = минимум 3 успешных сервиса из 5
- Все 5 сервисов проверяются полностью
- Собирает статистику источников, нод, сервисов и запусков
- Единственный архив нод: alive_archive.txt
- В архив попадают только подтверждённые LIVE-ноды
- Если свежих LIVE < 100:
    повторно проверяет архивные ноды
    DEAD архивные ноды удаляет из архива
    LIVE архивные использует для заполнения подписки
- Подписка: subs/vless_001.txt
- Никаких archive.txt / alive.txt / all_nodes.txt
- SNI-статистики нет
"""

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

SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")

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

ALIVE_ARCHIVE_FILE = os.path.join(
    FINAL_DIR,
    "live_archive.txt"
)

SUBSCRIPTION_FILE = os.path.join(
    FINAL_DIR,
    "vless_001.txt"
)


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
        logging.FileHandler(
            log_file,
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# =========================================================
# CONFIG
# =========================================================

MAX_NODES = 100
MAX_THREADS = 40

SOURCE_TIMEOUT = 15
SERVICE_TIMEOUT = 4

SINGBOX_START_WAIT = 1.5

MIN_SUCCESS_SERVICES = 3

GOOD_CODES = {
    200,
    204,
    301,
    302
}


TEST_URLS = [
    "https://telegram.org",
    "https://www.instagram.com",
    "https://www.youtube.com",
    "https://gemini.google.com",
    "https://www.google.com",
]


# =========================================================
# SOURCES
# =========================================================

if not os.path.exists(SOURCES_FILE):

    logger.error(
        "Файл sources.txt не найден: "
        f"{SOURCES_FILE}"
    )

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
# STOP EVENT
# =========================================================

stop_event = threading.Event()


# =========================================================
# STATS
# =========================================================

def empty_service_stats():

    return {
        url: {
            "attempts": 0,
            "success": 0,
            "failed": 0
        }
        for url in TEST_URLS
    }


def load_json_stats(
    path,
    default_factory
):

    if not os.path.exists(path):

        return default_factory()

    try:

        with open(
            path,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        if isinstance(data, dict):

            return data

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
            f"Сохранено: {label}"
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


SERVICE_STATS = load_json_stats(
    SERVICE_STATS_FILE,
    empty_service_stats
)

SOURCE_STATS = load_json_stats(
    SOURCE_STATS_FILE,
    lambda: {}
)

NODE_STATS = load_json_stats(
    NODE_STATS_FILE,
    lambda: {}
)


# =========================================================
# NODE ID
# =========================================================

def node_id(vless_uri):

    return hashlib.sha256(
        vless_uri.encode(
            "utf-8",
            errors="ignore"
        )
    ).hexdigest()[:24]


# =========================================================
# SNI
# =========================================================

def extract_sni(vless_uri):

    try:

        parsed = urlparse(vless_uri)

        params = parse_qs(
            parsed.query
        )

        return params.get(
            "sni",
            [""]
        )[0].strip().lower()

    except Exception:

        return ""


# =========================================================
# NODE RECORD
# =========================================================

def ensure_node_record(
    vless_uri
):

    nid = node_id(
        vless_uri
    )

    record = NODE_STATS.setdefault(
        nid,
        {
            "attempts": 0,
            "live": 0,
            "dead": 0,
            "last_status": "unknown",
            "original_sni": extract_sni(
                vless_uri
            ),
            "sources": [],
            "first_seen": "",
            "last_seen": ""
        }
    )

    return nid, record


# =========================================================
# SOURCE RECORD
# =========================================================

def ensure_source_bucket(
    source
):

    return SOURCE_STATS.setdefault(
        source,
        {
            "runs": 0,
            "downloads_ok": 0,
            "downloads_failed": 0,
            "found": 0,
            "tested": 0,
            "live": 0,
            "dead": 0,
            "last_run": ""
        }
    )


# =========================================================
# RECORD NODE SOURCES
# =========================================================

def record_node_sources(
    vless_uri,
    source_names
):

    nid, record = ensure_node_record(
        vless_uri
    )

    now = datetime.now().isoformat(
        timespec="seconds"
    )

    if not record.get(
        "first_seen"
    ):

        record["first_seen"] = now

    record["last_seen"] = now

    sources = record.setdefault(
        "sources",
        []
    )

    for source in source_names:

        if source not in sources:

            sources.append(
                source
            )

    return nid, record


# =========================================================
# BASE64
# =========================================================

def decode_base64_content(
    text
):

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
# NORMALIZE SOURCE
# =========================================================

def normalize_source_lines(
    text
):

    result = []

    for line in text.splitlines():

        line = line.strip()

        if not line:

            continue

        if line.startswith("#"):

            continue

        if "://" not in line and len(line) > 50:

            decoded = decode_base64_content(
                line
            )

            result.extend(
                decoded
            )

        else:

            result.append(
                line
            )

    return result


# =========================================================
# FETCH SOURCE
# =========================================================

def fetch_source(
    url
):

    headers = {
        "User-Agent":
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/154.0.0.0 Safari/537.36"
    }

    try:

        response = requests.get(
            url,
            timeout=SOURCE_TIMEOUT,
            headers=headers
        )

        if response.status_code != 200:

            logger.warning(
                f"HTTP {response.status_code}: "
                f"{url}"
            )

            return None

        return normalize_source_lines(
            response.text
        )

    except Exception as e:

        logger.warning(
            f"Ошибка загрузки source: "
            f"{url} | {e}"
        )

        return None


# =========================================================
# VLESS
# =========================================================

def is_valid_vless(
    line
):

    return line.lower().startswith(
        "vless://"
    )


# =========================================================
# PARSE VLESS -> SING-BOX
# =========================================================

def parse_vless_to_json(
    vless_uri,
    listen_port
):

    try:

        parsed = urlparse(
            vless_uri
        )

        if parsed.scheme.lower() != "vless":

            return None

        if "@" not in parsed.netloc:

            return None

        uuid, server_part = parsed.netloc.split(
            "@",
            1
        )

        uuid = unquote(
            uuid
        ).strip()

        server_part = server_part.strip()

        if not uuid or not server_part:

            return None

        # -------------------------------------------------
        # SERVER / PORT
        # -------------------------------------------------

        if server_part.startswith("["):

            closing = server_part.find("]")

            if closing == -1:

                return None

            server_address = server_part[
                1:closing
            ]

            remainder = server_part[
                closing + 1:
            ]

            if remainder.startswith(":"):

                server_port = int(
                    remainder[1:]
                )

            else:

                server_port = 443

        else:

            if ":" in server_part:

                server_address, port_text = (
                    server_part.rsplit(
                        ":",
                        1
                    )
                )

                server_port = int(
                    port_text
                )

            else:

                server_address = server_part

                server_port = 443

        params_raw = parse_qs(
            parsed.query
        )

        params = {
            key.lower(): values[0]
            for key, values
            in params_raw.items()
            if values
        }

        transport_type = params.get(
            "type",
            "tcp"
        ).lower()

        security = params.get(
            "security",
            "none"
        ).lower()

        flow = params.get(
            "flow",
            ""
        )

        outbound = {

            "type": "vless",

            "server":
                server_address,

            "server_port":
                server_port,

            "uuid":
                uuid
        }

        if flow:

            outbound["flow"] = flow

        # -------------------------------------------------
        # TLS / REALITY
        # -------------------------------------------------

        if (
            security == "reality"
            or "pbk" in params
        ):

            server_name = params.get(
                "sni",
                params.get(
                    "servername",
                    server_address
                )
            )

            tls = {

                "enabled": True,

                "server_name":
                    server_name,

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

            outbound["tls"] = tls

        elif security == "tls":

            server_name = params.get(
                "sni",
                params.get(
                    "servername",
                    server_address
                )
            )

            tls = {

                "enabled": True,

                "server_name":
                    server_name,

                "utls": {

                    "enabled": True,

                    "fingerprint":
                        params.get(
                            "fp",
                            "chrome"
                        )
                }
            }

            if params.get("alpn"):

                tls["alpn"] = [
                    x.strip()
                    for x in params["alpn"].split(",")
                    if x.strip()
                ]

            outbound["tls"] = tls

        # -------------------------------------------------
        # TRANSPORT
        # -------------------------------------------------

        if transport_type in (
            "tcp",
            "raw"
        ):

            pass

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

        elif transport_type == "grpc":

            outbound["transport"] = {

                "type": "grpc",

                "service_name":
                    params.get(
                        "servicename",
                        ""
                    )
            }

        elif transport_type == "httpupgrade":

            outbound["transport"] = {

                "type": "httpupgrade",

                "path":
                    params.get(
                        "path",
                        "/"
                    ),

                "host":
                    params.get(
                        "host",
                        ""
                    )
            }

        elif transport_type == "xhttp":

            outbound["transport"] = {

                "type": "xhttp",

                "path":
                    params.get(
                        "path",
                        "/"
                    ),

                "host":
                    [
                        params["host"]
                    ]
                    if params.get("host")
                    else [],

                "mode":
                    params.get(
                        "mode",
                        "auto"
                    )
            }

        else:

            return None

        # -------------------------------------------------
        # CONFIG
        # -------------------------------------------------

        config = {

            "log": {
                "level": "error"
            },

            "inbounds": [

                {

                    "type": "socks",

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

    except Exception as e:

        logger.debug(
            f"Ошибка parse VLESS: {e}"
        )

        return None


# =========================================================
# SERVICE CHECK
# =========================================================

def run_services(
    local_port,
    service_stats,
    stats_lock
):

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
            "Chrome/154.0.0.0 Safari/537.36"
    }

    success_count = 0

    results = {}

    for url in TEST_URLS:

        try:

            response = requests.get(
                url,
                proxies=proxies,
                timeout=SERVICE_TIMEOUT,
                headers=headers,
                allow_redirects=True
            )

            success = (
                response.status_code
                in GOOD_CODES
            )

        except Exception:

            success = False

        with stats_lock:

            item = service_stats.setdefault(
                url,
                {
                    "attempts": 0,
                    "success": 0,
                    "failed": 0
                }
            )

            item["attempts"] += 1

            if success:

                item["success"] += 1

            else:

                item["failed"] += 1

        results[url] = success

        if success:

            success_count += 1

    return (
        success_count,
        results
    )


# =========================================================
# CHECK ONE NODE
# =========================================================

def check_single_node(
    vless_uri,
    local_port,
    stats_lock
):

    config_path = os.path.join(
        BASE_PATH,
        f"temp_{local_port}.json"
    )

    log_path = os.path.join(
        LOG_DIR,
        f"singbox_{local_port}.log"
    )

    config = parse_vless_to_json(
        vless_uri,
        local_port
    )

    if not config:

        return {
            "live": False,
            "success_count": 0,
            "error": "invalid_config"
        }

    process = None
    log_handle = None

    try:

        with open(
            config_path,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                config,
                f,
                ensure_ascii=False
            )

        log_handle = open(
            log_path,
            "w",
            encoding="utf-8"
        )

        process = subprocess.Popen(
            [
                "sing-box",
                "run",
                "-c",
                config_path
            ],
            stdout=log_handle,
            stderr=log_handle
        )

        # -------------------------------------------------
        # WAIT FOR SING-BOX
        # -------------------------------------------------

        deadline = (
            time.time()
            + SINGBOX_START_WAIT
        )

        while time.time() < deadline:

            if process.poll() is not None:

                return {
                    "live": False,
                    "success_count": 0,
                    "error": "singbox_exit"
                }

            time.sleep(0.1)

        if process.poll() is not None:

            return {
                "live": False,
                "success_count": 0,
                "error": "singbox_exit"
            }

        # -------------------------------------------------
        # TEST ALL 5 SERVICES
        # -------------------------------------------------

        success_count, service_results = (
            run_services(
                local_port,
                SERVICE_STATS,
                stats_lock
            )
        )

        live = (
            success_count
            >= MIN_SUCCESS_SERVICES
        )

        return {
            "live": live,
            "success_count": success_count,
            "service_results": service_results,
            "error": ""
        }

    except Exception as e:

        return {
            "live": False,
            "success_count": 0,
            "error": str(e)
        }

    finally:

        if process:

            try:

                process.terminate()

                process.wait(
                    timeout=1
                )

            except Exception:

                try:

                    process.kill()

                except Exception:

                    pass

        if log_handle:

            try:
                log_handle.close()

            except Exception:
                pass

        for path in (
            config_path,
            log_path
        ):

            try:

                if os.path.exists(path):

                    os.remove(path)

            except Exception:

                pass


# =========================================================
# LOAD ALIVE ARCHIVE
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

            lines = [
                line.strip()
                for line in f
                if line.strip()
                and is_valid_vless(line.strip())
            ]

        # deduplicate
        return list(
            dict.fromkeys(lines)
        )

    except Exception as e:

        logger.warning(
            f"Ошибка загрузки live_archive.txt: {e}"
        )

        return []


# =========================================================
# SAVE ALIVE ARCHIVE
# =========================================================

def save_alive_archive(
    archive_nodes
):

    unique = list(
        dict.fromkeys(
            node.strip()
            for node in archive_nodes
            if node.strip()
            and is_valid_vless(
                node.strip()
            )
        )
    )

    with open(
        ALIVE_ARCHIVE_FILE,
        "w",
        encoding="utf-8"
    ) as f:

        if unique:

            f.write(
                "\n".join(unique)
            )

            f.write("\n")


# =========================================================
# TEST CURRENT SOURCES
# =========================================================

def test_nodes(
    nodes,
    node_sources_map,
    run_stats,
    context_name
):

    results = []

    task_queue = queue.Queue()

    for node in nodes:

        task_queue.put(
            node
        )

    lock = threading.Lock()

    port_queue = queue.Queue()

    for port in range(
        11000,
        11000 + MAX_THREADS
    ):

        port_queue.put(
            port
        )

    def worker():

        while not stop_event.is_set():

            try:

                node = task_queue.get(
                    timeout=0.5
                )

            except queue.Empty:

                return

            port = port_queue.get()

            nid = node_id(
                node
            )

            try:

                sources = node_sources_map.get(
                    node,
                    []
                )

                nid, record = (
                    record_node_sources(
                        node,
                        sources
                    )
                )

                with lock:

                    record["attempts"] = (
                        record.get(
                            "attempts",
                            0
                        ) + 1
                    )

                    record["last_status"] = (
                        "testing"
                    )

                    run_stats[
                        "checked"
                    ] += 1

                    if context_name == "source":

                        for src in sources:

                            bucket = (
                                ensure_source_bucket(
                                    src
                                )
                            )

                            bucket[
                                "tested"
                            ] += 1

                check = check_single_node(
                    node,
                    port,
                    lock
                )

                success_count = check.get(
                    "success_count",
                    0
                )

                is_live = check.get(
                    "live",
                    False
                )

                with lock:

                    if is_live:

                        record["live"] = (
                            record.get(
                                "live",
                                0
                            ) + 1
                        )

                        record["last_status"] = (
                            "live"
                        )

                        run_stats[
                            "live"
                        ] += 1

                        if context_name == "source":

                            for src in sources:

                                ensure_source_bucket(
                                    src
                                )[
                                    "live"
                                ] += 1

                        results.append(
                            node
                        )

                        logger.info(
                            f"LIVE | "
                            f"{nid} | "
                            f"{success_count}/"
                            f"{len(TEST_URLS)}"
                        )

                    else:

                        record["dead"] = (
                            record.get(
                                "dead",
                                0
                            ) + 1
                        )

                        record["last_status"] = (
                            "dead"
                        )

                        run_stats[
                            "dead"
                        ] += 1

                        if context_name == "source":

                            for src in sources:

                                ensure_source_bucket(
                                    src
                                )[
                                    "dead"
                                ] += 1

                        logger.info(
                            f"DEAD | "
                            f"{nid} | "
                            f"{success_count}/"
                            f"{len(TEST_URLS)}"
                        )

            except Exception as e:

                logger.error(
                    f"Ошибка проверки "
                    f"{nid}: {e}"
                )

                with lock:

                    run_stats[
                        "dead"
                    ] += 1

            finally:

                port_queue.put(
                    port
                )

                task_queue.task_done()

    with ThreadPoolExecutor(
        max_workers=MAX_THREADS
    ) as executor:

        futures = [
            executor.submit(
                worker
            )
            for _ in range(
                MAX_THREADS
            )
        ]

        for future in futures:

            try:

                future.result()

            except Exception as e:

                logger.error(
                    f"Worker error: {e}"
                )

    return list(
        dict.fromkeys(
            results
        )
    )


# =========================================================
# SOURCE MEMBERSHIP STATS
# =========================================================

def calculate_source_found(
    source_nodes
):

    for source, nodes in source_nodes.items():

        unique = set(nodes)

        bucket = ensure_source_bucket(
            source
        )

        bucket["found"] += len(
            unique
        )


# =========================================================
# UPDATE NODE SOURCE MEMBERSHIP
# =========================================================

def update_node_source_membership(
    source_nodes
):

    for source, nodes in source_nodes.items():

        for node in set(nodes):

            nid, record = (
                ensure_node_record(
                    node
                )
            )

            sources = record.setdefault(
                "sources",
                []
            )

            if source not in sources:

                sources.append(
                    source
                )

            record["last_seen"] = (
                datetime.now().isoformat(
                    timespec="seconds"
                )
            )

            if not record.get(
                "first_seen"
            ):

                record["first_seen"] = (
                    record["last_seen"]
                )


# =========================================================
# MAIN
# =========================================================

def main():

    started_at = datetime.now()

    logger.info(
        "=" * 70
    )

    logger.info(
        "=== SING-BOX VLESS MAIN ==="
    )

    logger.info(
        "=" * 70
    )

    logger.info(
        f"Sources: {len(SOURCES)}"
    )

    logger.info(
        f"Threads: {MAX_THREADS}"
    )

    logger.info(
        f"Services: {len(TEST_URLS)}"
    )

    logger.info(
        f"LIVE requirement: "
        f"{MIN_SUCCESS_SERVICES}/"
        f"{len(TEST_URLS)}"
    )

    logger.info(
        f"Subscription target: {MAX_NODES}"
    )

    logger.info(
        "SNI mutation: DISABLED"
    )

    logger.info(
        f"Archive: {ALIVE_ARCHIVE_FILE}"
    )

    # -----------------------------------------------------
    # SING-BOX CHECK
    # -----------------------------------------------------

    try:

        version = subprocess.run(
            [
                "sing-box",
                "version"
            ],
            capture_output=True,
            text=True,
            timeout=10
        )

        logger.info(
            version.stdout.strip()
        )

    except Exception as e:

        logger.error(
            f"sing-box не найден: {e}"
        )

        return

    # -----------------------------------------------------
    # RUN STATS
    # -----------------------------------------------------

    run_stats = {

        "started_at":
            started_at.isoformat(
                timespec="seconds"
            ),

        "sources":
            len(SOURCES),

        "source_fetch_success":
            0,

        "source_fetch_failed":
            0,

        "vless_found":
            0,

        "unique_nodes":
            0,

        "checked":
            0,

        "live":
            0,

        "dead":
            0,

        "archive_checked":
            0,

        "archive_live":
            0,

        "archive_dead":
            0
    }

    # -----------------------------------------------------
    # STEP 1 — DOWNLOAD
    # -----------------------------------------------------

    logger.info(
        "=" * 70
    )

    logger.info(
        "=== DOWNLOADING SOURCES ==="
    )

    source_nodes = defaultdict(list)

    all_nodes = []

    for source in SOURCES:

        bucket = ensure_source_bucket(
            source
        )

        bucket["runs"] += 1

        bucket["last_run"] = (
            run_stats["started_at"]
        )

        lines = fetch_source(
            source
        )

        if lines is None:

            bucket[
                "downloads_failed"
            ] += 1

            run_stats[
                "source_fetch_failed"
            ] += 1

            logger.error(
                f"DOWNLOAD FAILED: "
                f"{source}"
            )

            continue

        bucket[
            "downloads_ok"
        ] += 1

        run_stats[
            "source_fetch_success"
        ] += 1

        vless_nodes = []

        for line in lines:

            line = line.strip()

            if not is_valid_vless(
                line
            ):

                continue

            vless_nodes.append(
                line
            )

        unique_source_nodes = list(
            dict.fromkeys(
                vless_nodes
            )
        )

        bucket[
            "found"
        ] += len(
            unique_source_nodes
        )

        source_nodes[
            source
        ].extend(
            unique_source_nodes
        )

        all_nodes.extend(
            unique_source_nodes
        )

        logger.info(
            f"OK: {source}"
        )

        logger.info(
            f"VLESS found: "
            f"{len(unique_source_nodes)}"
        )

    # -----------------------------------------------------
    # STEP 2 — UNIQUE
    # -----------------------------------------------------

    unique_nodes = list(
        dict.fromkeys(
            all_nodes
        )
    )

    run_stats[
        "vless_found"
    ] = sum(
        len(
            set(nodes)
        )
        for nodes in source_nodes.values()
    )

    run_stats[
        "unique_nodes"
    ] = len(
        unique_nodes
    )

    logger.info(
        f"VLESS found across sources: "
        f"{run_stats['vless_found']}"
    )

    logger.info(
        f"Unique nodes for testing: "
        f"{len(unique_nodes)}"
    )

    # -----------------------------------------------------
    # NODE SOURCE MEMBERSHIP
    # -----------------------------------------------------

    node_sources_map = defaultdict(
        list
    )

    for source, nodes in source_nodes.items():

        for node in set(nodes):

            node_sources_map[
                node
            ].append(
                source
            )

    update_node_source_membership(
        source_nodes
    )

    # -----------------------------------------------------
    # STEP 3 — TEST CURRENT SOURCES
    # -----------------------------------------------------

    logger.info(
        "=" * 70
    )

    logger.info(
        f"=== TESTING CURRENT SOURCES: "
        f"{len(unique_nodes)} NODES ==="
    )

    stop_event.clear()

    test_start = time.time()

    fresh_live = test_nodes(
        unique_nodes,
        node_sources_map,
        run_stats,
        "source"
    )

    elapsed = (
        time.time()
        - test_start
    )

    logger.info(
        f"Current source testing time: "
        f"{elapsed:.2f}s"
    )

    fresh_live = list(
        dict.fromkeys(
            fresh_live
        )
    )

    fresh_live_ids = {
        node_id(node)
        for node in fresh_live
    }

    # -----------------------------------------------------
    # UPDATE LIVE ARCHIVE
    # -----------------------------------------------------

    archive_nodes = load_alive_archive()

    archive_by_id = {}

    for node in archive_nodes:

        archive_by_id[
            node_id(node)
        ] = node

    for node in fresh_live:

        archive_by_id[
            node_id(node)
        ] = node

    logger.info(
        "=" * 70
    )

    logger.info(
        "=== CURRENT SOURCE RESULTS ==="
    )

    for source in SOURCES:

        bucket = ensure_source_bucket(
            source
        )

        found = bucket.get(
            "found",
            0
        )

        # Это cumulative, поэтому здесь
        # показываем накопленные значения
        tested = bucket.get(
            "tested",
            0
        )

        live = bucket.get(
            "live",
            0
        )

        dead = bucket.get(
            "dead",
            0
        )

        logger.info(
            f"{source}"
        )

        logger.info(
            f"found: {found} | "
            f"tested cumulative: {tested} | "
            f"LIVE cumulative: {live} | "
            f"DEAD cumulative: {dead}"
        )

    # -----------------------------------------------------
    # ARCHIVE FILL
    # -----------------------------------------------------

    subscription_nodes = list(
        fresh_live
    )

    selected_ids = {
        node_id(node)
        for node in subscription_nodes
    }

    archive_candidates = []

    for node in archive_by_id.values():

        nid = node_id(
            node
        )

        if nid in selected_ids:

            continue

        if nid in fresh_live_ids:

            continue

        archive_candidates.append(
            node
        )

    archive_live = []

    archive_dead = []

    if len(subscription_nodes) < MAX_NODES:

        need = (
            MAX_NODES
            - len(subscription_nodes)
        )

        logger.info(
            "=" * 70
        )

        logger.info(
            f"=== ARCHIVE FILL: NEED "
            f"{need} NODES ==="
        )

        logger.info(
            f"Archive candidates: "
            f"{len(archive_candidates)}"
        )

        for node in archive_candidates:

            if len(subscription_nodes) >= MAX_NODES:

                break

            stop_event.clear()

            run_result = test_nodes(
                [node],
                {},
                run_stats,
                "archive"
            )

            run_stats[
                "archive_checked"
            ] += 1

            if run_result:

                archive_live.append(
                    node
                )

                run_stats[
                    "archive_live"
                ] += 1

                subscription_nodes.append(
                    node
                )

                selected_ids.add(
                    node_id(node)
                )

                archive_by_id[
                    node_id(node)
                ] = node

            else:

                archive_dead.append(
                    node
                )

                run_stats[
                    "archive_dead"
                ] += 1

                archive_by_id.pop(
                    node_id(node),
                    None
                )

    # -----------------------------------------------------
    # SAVE ARCHIVE
    # -----------------------------------------------------

    final_archive = list(
        archive_by_id.values()
    )

    # Fresh LIVE and successful archive
    # candidates are already present.
    # Dead archive candidates were removed.

    save_alive_archive(
        final_archive
    )

    # -----------------------------------------------------
    # SUBSCRIPTION
    # -----------------------------------------------------

    subscription_nodes = list(
        dict.fromkeys(
            subscription_nodes
        )
    )

    subscription_nodes = (
        subscription_nodes[:MAX_NODES]
    )

    if subscription_nodes:

        with open(
            SUBSCRIPTION_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            f.write(
                "\n".join(
                    subscription_nodes
                )
            )

            f.write("\n")

        logger.info(
            "=" * 70
        )

        logger.info(
            "=== SUBSCRIPTION ==="
        )

        logger.info(
            f"Subscription nodes: "
            f"{len(subscription_nodes)}"
        )

        logger.info(
            f"Subscription file: "
            f"{SUBSCRIPTION_FILE}"
        )

    else:

        logger.warning(
            "No LIVE nodes. "
            "Subscription was not replaced."
        )

    # -----------------------------------------------------
    # FINAL RUN
    # -----------------------------------------------------

    run_stats[
        "finished_at"
    ] = datetime.now().isoformat(
        timespec="seconds"
    )

    run_stats[
        "elapsed_seconds"
    ] = round(
        (
            datetime.fromisoformat(
                run_stats["finished_at"]
            )
            - datetime.fromisoformat(
                run_stats["started_at"]
            )
        ).total_seconds(),
        2
    )

    logger.info(
        "=" * 70
    )

    logger.info(
        "=== CURRENT RUN ==="
    )

    logger.info(
        f"VLESS found: "
        f"{run_stats['vless_found']}"
    )

    logger.info(
        f"Unique nodes: "
        f"{run_stats['unique_nodes']}"
    )

    logger.info(
        f"Checked: "
        f"{run_stats['checked']}"
    )

    logger.info(
        f"Fresh LIVE: "
        f"{len(fresh_live)}"
    )

    logger.info(
        f"Fresh DEAD: "
        f"{run_stats['dead']}"
    )

    logger.info(
        f"Archive checked: "
        f"{run_stats['archive_checked']}"
    )

    logger.info(
        f"Archive LIVE: "
        f"{run_stats['archive_live']}"
    )

    logger.info(
        f"Archive DEAD: "
        f"{run_stats['archive_dead']}"
    )

    logger.info(
        f"Subscription: "
        f"{len(subscription_nodes)}"
    )

    logger.info(
        f"Archive size: "
        f"{len(final_archive)}"
    )

    logger.info(
        f"Elapsed: "
        f"{run_stats['elapsed_seconds']}s"
    )

    # -----------------------------------------------------
    # SERVICES
    # -----------------------------------------------------

    logger.info(
        "=" * 70
    )

    logger.info(
        "=== SERVICES — CUMULATIVE ==="
    )

    total_attempts = 0
    total_success = 0
    total_failed = 0

    for url in TEST_URLS:

        data = SERVICE_STATS.get(
            url,
            {}
        )

        attempts = int(
            data.get(
                "attempts",
                0
            )
        )

        success = int(
            data.get(
                "success",
                0
            )
        )

        failed = int(
            data.get(
                "failed",
                0
            )
        )

        total_attempts += attempts
        total_success += success
        total_failed += failed

        rate = (
            success
            / attempts
            * 100
            if attempts
            else 0
        )

        logger.info(
            f"{url} "
            f"attempts={attempts} "
            f"success={success} "
            f"failed={failed} "
            f"{rate:.2f}%"
        )

    # -----------------------------------------------------
    # SOURCE STATS
    # -----------------------------------------------------

    logger.info(
        "=" * 70
    )

    logger.info(
        "=== SOURCES — CUMULATIVE ==="
    )

    for source in SOURCES:

        data = ensure_source_bucket(
            source
        )

        found = data.get(
            "found",
            0
        )

        tested = data.get(
            "tested",
            0
        )

        live = data.get(
            "live",
            0
        )

        dead = data.get(
            "dead",
            0
        )

        rate = (
            live
            / tested
            * 100
            if tested
            else 0
        )

        logger.info(
            f"{source} | "
            f"downloads OK={data.get('downloads_ok', 0)} | "
            f"ERR={data.get('downloads_failed', 0)} | "
            f"found={found} | "
            f"tested={tested} | "
            f"LIVE={live} | "
            f"DEAD={dead} | "
            f"rate={rate:.2f}%"
        )

    # -----------------------------------------------------
    # ALL TIME TOTALS
    # -----------------------------------------------------

    previous_runs = load_json_stats(
        RUN_STATS_FILE,
        lambda: {
            "runs": 0,
            "vless_found": 0,
            "unique_nodes": 0,
            "checked": 0,
            "live": 0,
            "dead": 0,
            "archive_checked": 0,
            "archive_live": 0,
            "archive_dead": 0,
            "service_attempts": 0,
            "service_success": 0,
            "service_failed": 0
        }
    )

    # Compatibility with old simple format
    if not isinstance(
        previous_runs.get("runs"),
        int
    ):

        previous_runs["runs"] = 0

    previous_runs["runs"] += 1

    for key in (
        "vless_found",
        "unique_nodes",
        "checked",
        "live",
        "dead",
        "archive_checked",
        "archive_live",
        "archive_dead"
    ):

        previous_runs[key] = (
            int(
                previous_runs.get(
                    key,
                    0
                )
            )
            + int(
                run_stats.get(
                    key,
                    0
                )
            )
        )

    previous_runs[
        "service_attempts"
    ] = (
        previous_runs.get(
            "service_attempts",
            0
        )
        + total_attempts
    )

    previous_runs[
        "service_success"
    ] = (
        previous_runs.get(
            "service_success",
            0
        )
        + total_success
    )

    previous_runs[
        "service_failed"
    ] = (
        previous_runs.get(
            "service_failed",
            0
        )
        + total_failed
    )

    previous_runs[
        "last_run"
    ] = run_stats

    # -----------------------------------------------------
    # SAVE
    # -----------------------------------------------------

    save_json_stats(
        SERVICE_STATS_FILE,
        SERVICE_STATS,
        "Статистика сервисов"
    )

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

    save_json_stats(
        RUN_STATS_FILE,
        previous_runs,
        "Статистика запусков"
    )

    logger.info(
        "=" * 70
    )

    logger.info(
        "=== ALL-TIME TOTALS ==="
    )

    logger.info(
        f"Runs: "
        f"{previous_runs['runs']}"
    )

    logger.info(
        f"VLESS found: "
        f"{previous_runs['vless_found']}"
    )

    logger.info(
        f"Unique nodes: "
        f"{previous_runs['unique_nodes']}"
    )

    logger.info(
        f"Checked: "
        f"{previous_runs['checked']}"
    )

    logger.info(
        f"LIVE: "
        f"{previous_runs['live']}"
    )

    logger.info(
        f"DEAD: "
        f"{previous_runs['dead']}"
    )

    logger.info(
        f"Archive checked: "
        f"{previous_runs['archive_checked']}"
    )

    logger.info(
        f"Archive LIVE: "
        f"{previous_runs['archive_live']}"
    )

    logger.info(
        f"Archive DEAD: "
        f"{previous_runs['archive_dead']}"
    )

    logger.info(
        f"Service attempts: "
        f"{previous_runs['service_attempts']}"
    )

    logger.info(
        f"Service success: "
        f"{previous_runs['service_success']}"
    )

    logger.info(
        f"Service failed: "
        f"{previous_runs['service_failed']}"
    )

    logger.info(
        "=" * 70
    )

    logger.info(
        "=== DONE ==="
    )


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    main()
