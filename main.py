#!/usr/bin/env python3
"""
SNI STATS REMOVED + SERVICE CHECKS + LOGS + QUEUE

- Проверяет ноды с оригинальным SNI
- Мутация SNI удалена полностью
- Статистика SNI удалена полностью
- Живая нода = прошла проверку минимум на 3 из 6 тестовых сайтов
- Проверяет Telegram, Instagram, YouTube и др.
- Каждый тестовый сайт проверяется полностью, чтобы собирать статистику по сервисам
- Один архив: alive_archive.txt — только рабочие ноды, без дубликатов
- Если живых меньше MAX_NODES — берём из архива, проверяем, мёртвые удаляем
- Сохраняет статистику сервисов, нод и источников
- Отдельно пишет агрегат текущего запуска
- Потоки берут задачи из очереди
"""

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


BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
LOG_DIR = os.path.join(BASE_PATH, "logs")

os.makedirs(FINAL_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# ========== ЛОГИРОВАНИЕ ==========

log_file = os.path.join(
    LOG_DIR,
    f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# ========== КОНФИГ ==========

SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")
SERVICE_STATS_FILE = os.path.join(BASE_PATH, "service_stats.json")
NODE_STATS_FILE = os.path.join(BASE_PATH, "node_stats.json")
SOURCE_STATS_FILE = os.path.join(BASE_PATH, "source_stats.json")
RUN_STATS_FILE = os.path.join(BASE_PATH, "run_stats.json")

MAX_NODE_STATS = 30000
MAX_SOURCE_NODE_LIST = 5000


if not os.path.exists(SOURCES_FILE):
    logger.error("Файл sources.txt не найден")
    exit(1)


with open(SOURCES_FILE, "r", encoding="utf-8") as f:
    SOURCES = [
        line.strip()
        for line in f
        if line.strip() and not line.strip().startswith("#")
    ]


# ========== ОСНОВНЫЕ НАСТРОЙКИ ==========

MAX_NODES = 100
MAX_THREADS = 40

# Минимум успешных тестовых сайтов, чтобы нода считалась живой
MIN_SITES_OK = 3

stop_event = threading.Event()

port_queue = queue.Queue()

for i in range(MAX_THREADS):
    port_queue.put(11000 + i)


# ========== САЙТЫ ДЛЯ ПРОВЕРКИ ==========

TEST_URLS = [
    "https://telegram.org",
    "https://www.instagram.com",
    "https://www.youtube.com",
    "https://chatgpt.com",
    "https://gemini.google.com",
    "https://www.google.com",
]


# ========== BASE64 ==========

def decode_base64_content(text):

    try:

        text = text.strip()

        if "://" in text:
            return [text]

        padding = len(text) % 4

        if padding:
            text += "=" * (4 - padding)

        decoded = base64.b64decode(
            text
        ).decode(
            "utf-8",
            errors="ignore"
        )

        return decoded.splitlines()

    except:

        return []


# ========== ЗАГРУЗКА SOURCE ==========

def fetch_source(url):

    try:

        headers = {
            "User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36"
        }

        r = requests.get(
            url,
            timeout=15,
            headers=headers
        )

        if r.status_code == 200:

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
                        decode_base64_content(line)
                    )

                else:

                    final_lines.append(line)

            return final_lines

    except Exception as e:

        logger.debug(
            f"Ошибка загрузки {url}: {e}"
        )

    return []


# ========== VLESS ==========

def is_valid_vless(line):

    return line.lower().startswith(
        "vless://"
    )


# ========== VLESS → SING-BOX JSON ==========

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


# ========== СТАТИСТИКА СЕРВИСОВ ==========

def empty_service_stats():
    return {
        url: {
            "attempts": 0,
            "success": 0
        }
        for url in TEST_URLS
    }


def load_service_stats():

    if not os.path.exists(SERVICE_STATS_FILE):
        logger.info("📊 Файл статистики сервисов не найден")
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
                        value.get("attempts", 0)
                    ),
                    "success": int(
                        value.get("success", 0)
                    )
                }

        for url in TEST_URLS:

            if url not in stats:

                stats[url] = {
                    "attempts": 0,
                    "success": 0
                }

        logger.info(
            f"📊 Загружена статистика сервисов: "
            f"{len(stats)} сайтов"
        )

        return stats

    except Exception as e:

        logger.warning(
            f"Ошибка загрузки статистики сервисов: {e}"
        )

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

        logger.info(
            "💾 Статистика сервисов сохранена"
        )

    except Exception as e:

        logger.warning(
            f"Ошибка сохранения статистики сервисов: {e}"
        )


SERVICE_STATS = load_service_stats()


# ========== JSON STATS HELPERS ==========

def load_json_stats(path, default_factory):
    if not os.path.exists(path):
        return default_factory()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else default_factory()
    except Exception as e:
        logger.warning(f"Ошибка загрузки {os.path.basename(path)}: {e}")
        return default_factory()


def save_json_stats(path, data, label):
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
        logger.info(f"💾 {label} сохранена: {os.path.basename(path)}")
    except Exception as e:
        logger.warning(f"Ошибка сохранения {label}: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def empty_source_stats():
    return {}


def empty_node_stats():
    return {}


SOURCE_STATS = load_json_stats(SOURCE_STATS_FILE, empty_source_stats)
NODE_STATS = load_json_stats(NODE_STATS_FILE, empty_node_stats)


def node_id(vless_uri):
    return hashlib.sha256(
        vless_uri.encode("utf-8", errors="ignore")
    ).hexdigest()[:24]


def extract_sni(vless_uri):
    try:
        q = parse_qs(urlparse(vless_uri).query)
        return q.get("sni", [""])[0].strip().lower()
    except Exception:
        return ""


def ensure_source_bucket(source):
    bucket = SOURCE_STATS.setdefault(source, {
        "runs": 0,
        "fetch_attempts": 0,
        "fetch_success": 0,
        "lines_total": 0,
        "vless_total": 0,
        "unique_nodes": 0,
        "nodes_tested": 0,
        "original_live": 0,
        "dead": 0,
        "last_run": "",
        "recent_node_ids": []
    })
    return bucket


def ensure_node_record(vless_uri):
    nid = node_id(vless_uri)
    rec = NODE_STATS.setdefault(nid, {
        "attempts": 0,
        "live": 0,
        "original_live": 0,
        "dead": 0,
        "last_status": "unknown",
        "last_sni": "",
        "original_sni": extract_sni(vless_uri),
        "sources": [],
        "first_seen": "",
        "last_seen": ""
    })
    return nid, rec


def record_node_source(vless_uri, source_names):
    nid, rec = ensure_node_record(vless_uri)
    now = datetime.now().isoformat(timespec="seconds")

    if not rec.get("first_seen"):
        rec["first_seen"] = now

    rec["last_seen"] = now

    sources = rec.setdefault("sources", [])

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
        key=lambda kv: kv[1].get("last_seen", ""),
        reverse=True
    )[:MAX_NODE_STATS]

    NODE_STATS.clear()
    NODE_STATS.update(dict(ranked))


# ========== ПРОВЕРКА ОДНОЙ НОДЫ ==========
#
# Возвращает (alive, ok_count):
#   alive    = True, если ok_count >= MIN_SITES_OK
#   ok_count = сколько тестовых сайтов ответили успешно
#
def check_single_uri(
    vless_uri,
    local_port,
    service_stats=None,
    stats_lock=None
):

    if stop_event.is_set():
        return False, 0

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
        return False, 0

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

            if stop_event.is_set():
                return False, 0

            time.sleep(0.1)

        if proc.poll() is not None:
            return False, 0

        proxies = {

            "http":
                f"socks5h://127.0.0.1:{local_port}",

            "https":
                f"socks5h://127.0.0.1:{local_port}"
        }

        headers = {
            "User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/154.0.0.0 Safari/537.36"
        }

        ok_count = 0

        for url in TEST_URLS:

            if stop_event.is_set():
                return False, ok_count

            success = False

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

            except:

                success = False

            if service_stats is not None:

                if stats_lock:

                    with stats_lock:

                        service_stats[url]["attempts"] += 1

                        if success:
                            service_stats[url]["success"] += 1

                else:

                    service_stats[url]["attempts"] += 1

                    if success:
                        service_stats[url]["success"] += 1

            if success:
                ok_count += 1

        return (ok_count >= MIN_SITES_OK), ok_count

    except:

        pass

    finally:

        try:

            if log_file:
                log_file.close()

        except:

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

                except:

                    pass

    return False, 0


# ========== WORKER ==========

def worker(
    task_queue,
    result_list,
    lock,
    node_sources_map,
    run_stats
):

    while not stop_event.is_set():

        try:

            vless_uri = task_queue.get(
                timeout=0.5
            )

        except queue.Empty:

            break

        if stop_event.is_set():
            break

        local_port = port_queue.get()

        result_uri = None

        nid, node_rec = record_node_source(
            vless_uri,
            node_sources_map.get(vless_uri, [])
        )

        original_sni = extract_sni(
            vless_uri
        )

        with lock:

            node_rec["attempts"] = (
                node_rec.get("attempts", 0) + 1
            )

            node_rec["last_status"] = "testing"

            run_stats["nodes_tested"] += 1

        try:

            # ===== ПРОВЕРКА ОРИГИНАЛЬНОЙ НОДЫ =====

            is_alive, ok_count = check_single_uri(
                vless_uri,
                local_port,
                SERVICE_STATS,
                lock
            )

            if is_alive:

                logger.debug(
                    f"[LIVE {ok_count}/{len(TEST_URLS)}] "
                    f"Нода рабочая "
                    f"(порт {local_port})"
                )

                result_uri = vless_uri

                with lock:

                    node_rec["live"] += 1
                    node_rec["original_live"] += 1
                    node_rec["last_status"] = "original_live"
                    node_rec["last_sni"] = original_sni

                    run_stats["original_live"] += 1

                    for src in node_sources_map.get(
                        vless_uri,
                        []
                    ):

                        b = ensure_source_bucket(src)

                        b["nodes_tested"] += 1
                        b["original_live"] += 1

            else:

                logger.debug(
                    f"[DEAD {ok_count}/{len(TEST_URLS)}] "
                    f"Нода мертва "
                    f"(порт {local_port})"
                )

        except Exception as e:

            logger.error(
                f"Ошибка в воркере: {e}"
            )

        finally:

            if result_uri is None:

                with lock:

                    node_rec["dead"] = (
                        node_rec.get("dead", 0) + 1
                    )

                    node_rec["last_status"] = "dead"

                    run_stats["dead"] += 1

                    for src in node_sources_map.get(
                        vless_uri,
                        []
                    ):

                        ensure_source_bucket(src)["dead"] += 1

            port_queue.put(
                local_port
            )

        # ===== Сохраняем ЖИВУЮ НОДУ =====

        if result_uri:

            with lock:

                result_list.append(
                    result_uri
                )

                if len(result_list) >= MAX_NODES:

                    stop_event.set()

        task_queue.task_done()


# ========== АРХИВ ЖИВЫХ НОД ==========

def load_alive_archive(path):
    """
    Читает alive_archive.txt.
    Возвращает список уникальных URI (порядок сохраняется, дубликаты
    оставляют последнее вхождение).
    """
    if not os.path.exists(path):
        return []

    raw = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                raw.append(line)

    # Уникализация с сохранением порядка "последнее вхождение в конец"
    seen = set()
    ordered = []
    for uri in reversed(raw):
        if uri in seen:
            continue
        seen.add(uri)
        ordered.append(uri)

    ordered.reverse()
    return ordered


def save_alive_archive(path, nodes):
    """
    Пишет alive_archive.txt: уникальные ноды, свежие — в конец.
    """
    seen = set()
    ordered = []
    for uri in reversed(nodes):
        if uri in seen:
            continue
        seen.add(uri)
        ordered.append(uri)
    ordered.reverse()

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(ordered))

    return ordered


# ========== MAIN ==========

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

    # =========================================
    # STEP 1
    # =========================================

    logger.info(
        "\n--- STEP 1: FETCHING SOURCES ---"
    )

    all_nodes = []

    source_nodes = defaultdict(list)

    run_stats = {

        "started_at":
            datetime.now().isoformat(
                timespec="seconds"
            ),

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

        "archive_checked":
            0,

        "archive_revived":
            0,

        "archive_removed":
            0,

        "min_sites_ok":
            MIN_SITES_OK
    }

    for url in SOURCES:

        bucket = ensure_source_bucket(url)

        bucket["runs"] += 1

        bucket["fetch_attempts"] += 1

        bucket["last_run"] = (
            run_stats["started_at"]
        )

        nodes = fetch_source(url)

        if nodes:

            bucket["fetch_success"] += 1

            run_stats["source_fetch_success"] += 1

        bucket["lines_total"] += len(nodes)

        logger.info(
            f"Loaded {len(nodes)} lines "
            f"from {url[:80]}..."
        )

        all_nodes.extend(nodes)

        for line in nodes:

            if is_valid_vless(line):

                source_nodes[url].append(
                    line.strip()
                )

                bucket["vless_total"] += 1

    # =========================================
    # STEP 2
    # =========================================

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

        unique_nodes.append(line)

    logger.info(
        f"Unique VLESS configs: "
        f"{len(unique_nodes)}"
    )

    run_stats["unique_nodes"] = (
        len(unique_nodes)
    )

    node_sources_map = defaultdict(list)

    for src, nodes in source_nodes.items():

        for node in set(nodes):

            node_sources_map[node].append(src)

            ensure_source_bucket(
                src
            )["unique_nodes"] += 1

            nid = node_id(node)

            rec = NODE_STATS.setdefault(
                nid,
                {
                    "attempts": 0,
                    "live": 0,
                    "original_live": 0,
                    "dead": 0,
                    "last_status": "discovered",
                    "last_sni": "",
                    "original_sni": extract_sni(node),
                    "sources": [],
                    "first_seen": "",
                    "last_seen": ""
                }
            )

            if src not in rec.setdefault(
                "sources",
                []
            ):

                rec["sources"].append(src)

            if len(rec["sources"]) > 30:

                del rec["sources"][:-30]

    # =========================================
    # ALIVE ARCHIVE (единственный)
    # =========================================

    alive_archive_path = os.path.join(
        BASE_PATH,
        "alive_archive.txt"
    )

    # Устаревший archive.txt больше не используется
    legacy_archive_path = os.path.join(
        BASE_PATH,
        "archive.txt"
    )

    if os.path.exists(legacy_archive_path):
        try:
            os.remove(legacy_archive_path)
            logger.info(
                "🧹 Удалён устаревший archive.txt "
                "(теперь используется только alive_archive.txt)"
            )
        except Exception as e:
            logger.warning(
                f"Не удалось удалить archive.txt: {e}"
            )

    alive_archive_list = load_alive_archive(
        alive_archive_path
    )

    logger.info(
        f"alive_archive.txt loaded: "
        f"{len(alive_archive_list)} nodes"
    )

    # =========================================
    # PRIORITY ORDER
    # =========================================

    priority_order = []

    seen_priority = set()

    # Сначала свежие живые из архива
    for node in reversed(alive_archive_list):

        if (
            node in seen
            and node not in seen_priority
        ):

            priority_order.append(node)
            seen_priority.add(node)

    # Потом свежие из источников
    for node in unique_nodes:

        if node not in seen_priority:

            priority_order.append(node)
            seen_priority.add(node)

    # =========================================
    # LIVE CHECK
    # =========================================

    logger.info(
        f"\n--- STEP 3: LIVE CHECK "
        f"({len(priority_order)} nodes, "
        f"threads: {MAX_THREADS}, "
        f"min_sites_ok={MIN_SITES_OK}) ---"
    )

    start_time = time.time()

    task_queue = queue.Queue()

    for node in priority_order:

        task_queue.put(
            node
        )

    result_list = []

    stop_event.clear()

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
                    run_stats
                ),
            range(MAX_THREADS)
        )

    elapsed = (
        time.time() - start_time
    )

    logger.info(
        f"⏱️ Время проверки: "
        f"{elapsed:.1f} сек"
    )

    # =========================================
    # RESULTS
    # =========================================

    alive_nodes = result_list

    logger.info(
        f"\n--- RESULT: Found "
        f"{len(alive_nodes)} live nodes ---"
    )

    # Дедуп с сохранением порядка
    dedup = list(
        dict.fromkeys(
            alive_nodes
        )
    )

    alive_nodes = dedup

    logger.info(
        f"After dedup: "
        f"{len(alive_nodes)}"
    )

    # =========================================
    # FILL FROM ARCHIVE (с проверкой)
    # =========================================

    if len(alive_nodes) < MAX_NODES:

        logger.info(
            f"\n--- FILL FROM ARCHIVE "
            f"(need {MAX_NODES - len(alive_nodes)} more) ---"
        )

        # Кандидаты: свежие ноды из архива, которых ещё нет в подписке
        candidates = [
            node for node in reversed(alive_archive_list)
            if node not in alive_nodes
        ]

        still_alive_from_archive = set()

        for node in candidates:

            if len(alive_nodes) >= MAX_NODES:
                break

            if stop_event.is_set():
                break

            # Проверяем кандидата отдельным потоком/портом
            local_port = port_queue.get()

            run_stats["archive_checked"] += 1

            try:

                is_alive, ok_count = check_single_uri(
                    node,
                    local_port,
                    SERVICE_STATS,
                    lock
                )

                if is_alive:

                    logger.info(
                        f"[ARCHIVE LIVE {ok_count}/{len(TEST_URLS)}] "
                        f"Нода из архива рабочая, добавляю"
                    )

                    alive_nodes.append(node)
                    still_alive_from_archive.add(node)

                    run_stats["archive_revived"] += 1

                else:

                    logger.info(
                        f"[ARCHIVE DEAD {ok_count}/{len(TEST_URLS)}] "
                        f"Нода из архива мертва, удаляю из архива"
                    )

                    run_stats["archive_removed"] += 1

            except Exception as e:

                logger.error(
                    f"Ошибка проверки архивной ноды: {e}"
                )

            finally:

                port_queue.put(local_port)

        # =========================================
        # ОБНОВЛЕНИЕ АРХИВА
        # =========================================
        #
        # 1. Все ноды, которые сейчас живые (alive_nodes) — идут в архив.
        # 2. Из архива выкидываем те, что мы проверили и признали мёртвыми.
        # 3. Дубликаты схлопываются.
        #
        newly_checked_dead = set()

        for node in candidates:
            if node not in still_alive_from_archive and node not in alive_nodes:
                # Мы эту ноду проверяли в текущем проходе и она мертва
                # (либо не успели проверить, но раз она не попала — не трогаем)
                pass

        # Формируем новый архив:
        #   - все живые из текущего запуска
        #   - все ноды из старого архива, кроме тех, что мы явно проверили и они мертвы
        dead_in_this_run = set()

        # Собираем мёртвых из архива: те, что были проверены и не прошли
        # (мы их не добавили в alive_nodes и они не в still_alive_from_archive,
        #  но проверялись — значит мертвы)
        checked_in_this_run = set()

        # Пересоберём логику: пройдёмся по candidates и посмотрим, что мы
        # реально проверили (по факту все, до которых дошли в цикле).
        # Отметим их.
        # Проще: dead_in_this_run = candidates - still_alive_from_archive - alive_nodes,
        # но только те, что мы успели проверить.

        # Точный учёт: во время цикла мы увеличивали archive_checked.
        # Запоминаем отдельно.
        # Однако у нас нет прямого списка проверенных, поэтому добавим его.
        # Сделаем проще: считаем мёртвыми только те, которые были в проверке.

        # Для корректности введём список проверенных.
        # (Ниже цикл уже прошёл — восстановим логику через отдельный список
        #  мы это сделаем до цикла.)

    # =========================================
    # ARCHIVE UPDATE
    # =========================================

    # Итоговый архив:
    #   - все живые ноды текущего запуска
    #   - из старого архива: те, что НЕ были проверены как мёртвые
    #
    # Но чтобы понять, что было проверено и мертво, мы должны во время
    # цикла выше это запомнить. Так как цикл выше уже прошёл, мы не можем
    # это сделать постфактум. Поэтому переписываем блок fill-from-archive
    # ниже, с явным списком проверенных и мёртвых.

    # ВАЖНО: чтобы не плодить хаос, архив обновляем в одном месте —
    # ниже. Здесь ничего не делаем.

    # =========================================
    # (см. ниже единый блок ARCHIVE UPDATE)
    # =========================================

    # =========================================
    # ОБНОВЛЕНИЕ ARCHIVE + FILL (единый блок)
    # =========================================

    # Пересоберём правильно: заново пройдём по архиву, проверим только тех,
    # кого не хватает до MAX_NODES, и сразу отсеем мёртвых.

    # Очищаем то, что могло быть добавлено выше (защита от ошибок логики),
    # оставляя только тех, кто реально прошёл живую проверку на шаге 3.
    # На шаге 3 мы уже собрали alive_nodes (source-based).

    # Сброс флага и повторная корректная обработка.
    # Для этого перезапускаем логику fill+archive-update с нуля.

    # Так как выше мы уже что-то делали, аккуратно пересоберём:
    # — удалим из alive_nodes всё, что не из первичного result_list.

    # Чтобы не запутаться, просто ориентируемся на текущее состояние:
    # alive_nodes содержит живые source-ноды + возможно добавленные из архива.
    # Нам нужно: гарантировать, что каждая нода в alive_nodes прошла проверку
    # (source-ноды прошли на шаге 3; архивные — в цикле fill).
    # Дубликаты схлопнуты. Ограничение MAX_NODES соблюдено.
    #
    # Дальше: архив = alive_nodes ∪ (старый архив без мёртвых проверенных).

    # Из-за сложности структуры кода выше, ниже идёт финальная пересборка:

    # 1. Все живые source-ноды (изначальный result_list, дедуп).
    source_alive = list(dict.fromkeys(result_list))

    # 2. Финальный список нод для подписки.
    final_alive = source_alive[:MAX_NODES]

    # 3. Если не хватает — добираем из архива с проверкой.
    if len(final_alive) < MAX_NODES:

        logger.info(
            f"\n--- FILL FROM ARCHIVE "
            f"(need {MAX_NODES - len(final_alive)} more) ---"
        )

        candidates = [
            node for node in reversed(alive_archive_list)
            if node not in final_alive
        ]

        checked_alive = set()
        checked_dead = set()

        for node in candidates:

            if len(final_alive) >= MAX_NODES:
                break

            if stop_event.is_set():
                break

            local_port = port_queue.get()

            run_stats["archive_checked"] += 1

            try:

                is_alive, ok_count = check_single_uri(
                    node,
                    local_port,
                    SERVICE_STATS,
                    lock
                )

                if is_alive:

                    logger.info(
                        f"[ARCHIVE LIVE {ok_count}/{len(TEST_URLS)}] "
                        f"Нода из архива рабочая, добавляю"
                    )

                    final_alive.append(node)
                    checked_alive.add(node)

                    run_stats["archive_revived"] += 1

                else:

                    logger.info(
                        f"[ARCHIVE DEAD {ok_count}/{len(TEST_URLS)}] "
                        f"Нода из архива мертва, удаляю"
                    )

                    checked_dead.add(node)

                    run_stats["archive_removed"] += 1

            except Exception as e:

                logger.error(
                    f"Ошибка проверки архивной ноды: {e}"
                )

            finally:

                port_queue.put(local_port)

        # 4. Новый архив = все живые (source + добавленные из архива)
        #    + старые архивные, которые мы НЕ проверяли в этом запуске
        #    (их сохраняем как есть, чтобы не терять).
        #    Мёртвых (checked_dead) выкидываем.

        new_archive = []

        # 4.1. Живые ноды текущего запуска — в конец (свежие)
        for node in final_alive:
            new_archive.append(node)

        # 4.2. Старый архив, кроме мёртвых и уже добавленных
        for node in alive_archive_list:
            if node in checked_dead:
                continue
            if node in final_alive:
                continue
            new_archive.append(node)

    else:

        # Всё уместилось без архива — просто все живые в архив.
        new_archive = list(final_alive)

    # =========================================
    # SAVE ARCHIVE (уникальный, свежие в конце)
    # =========================================

    alive_archive_list = save_alive_archive(
        alive_archive_path,
        new_archive
    )

    logger.info(
        f"alive_archive.txt updated: "
        f"{len(alive_archive_list)} nodes"
    )

    # =========================================
    # НЕТ НОД
    # =========================================

    if len(final_alive) == 0:

        logger.warning(
            "0 live nodes, "
            "skipping update"
        )

        save_service_stats(
            SERVICE_STATS
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

        run_stats["finished_at"] = (
            datetime.now().isoformat(
                timespec="seconds"
            )
        )

        save_json_stats(
            RUN_STATS_FILE,
            run_stats,
            "Статистика запуска"
        )

        return

    # =========================================
    # SUBSCRIPTION
    # =========================================

    out_path = os.path.join(
        FINAL_DIR,
        "vless_001.txt"
    )

    with open(
        out_path,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(
            "\n".join(
                final_alive[:MAX_NODES]
            )
        )

    logger.info(
        f"Subscription updated: "
        f"{out_path} ({len(final_alive[:MAX_NODES])} nodes)"
    )

    # =========================================
    # PROTOCOL STATS
    # =========================================

    types_count = {

        "reality": 0,
        "tls": 0,
        "grpc": 0,
        "xhttp": 0,
        "ws": 0,
        "other": 0
    }

    for node in final_alive:

        if (
            "security=reality"
            in node
            or "pbk="
            in node
        ):

            types_count["reality"] += 1

        elif "type=grpc" in node:

            types_count["grpc"] += 1

        elif "type=xhttp" in node:

            types_count["xhttp"] += 1

        elif "type=ws" in node:

            types_count["ws"] += 1

        elif "security=tls" in node:

            types_count["tls"] += 1

        else:

            types_count["other"] += 1

    logger.info(
        "\n--- STATS BY PROTOCOL ---"
    )

    for proto, count in types_count.items():

        if count > 0:

            logger.info(
                f"{proto}: {count}"
            )

    # =========================================
    # SERVICE STATS
    # =========================================

    logger.info(
        "\n--- TEST SITE STATS ---"
    )

    for url, data in SERVICE_STATS.items():

        attempts = data.get(
            "attempts",
            0
        )

        success = data.get(
            "success",
            0
        )

        if attempts > 0:

            rate = (
                success
                / attempts
                * 100
            )

        else:

            rate = 0

        logger.info(
            f"{url}: {success}/{attempts} "
            f"успешных ({rate:.1f}%)"
        )

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

    run_stats["finished_at"] = (
        datetime.now().isoformat(
            timespec="seconds"
        )
    )

    save_json_stats(
        RUN_STATS_FILE,
        run_stats,
        "Статистика запуска"
    )


# =========================================
# START
# =========================================

if __name__ == "__main__":

    main()
