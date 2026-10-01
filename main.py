#!/usr/bin/env python3
"""
SNI MUTATOR + SNI STATS + SERVICE CHECKS + LOGS + QUEUE

- Проверяет ноды с оригинальным SNI
- Если Reality-нода мертва — перебирает SNI из рейтинга
- Ведёт статистику SNI:
    attempts = сколько раз SNI проверялся
    success  = сколько раз SNI оживил ноду
- Показывает процент успешности SNI
- Выбирает SNI с учётом процента, объёма статистики и исследования новых SNI
- Проверяет Telegram, Instagram, YouTube
- Пока нода считается живой, если отвечает хотя бы один тестовый сайт
- Каждый тестовый сайт проверяется полностью, чтобы собирать статистику по сервисам
- TOP15 + RANDOM5 сохраняются как общий лимит 20 SNI
- Сохраняет статистику SNI, сервисов, нод, источников и SNI→сервисы
- Отдельно пишет агрегат текущего запуска
- Связь SNI→сервисы пока НЕ влияет на выбор SNI — только накапливается
- Без старения
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
SNI_STATS_FILE = os.path.join(BASE_PATH, "sni_stats.json")
SERVICE_STATS_FILE = os.path.join(BASE_PATH, "service_stats.json")
NODE_STATS_FILE = os.path.join(BASE_PATH, "node_stats.json")
SOURCE_STATS_FILE = os.path.join(BASE_PATH, "source_stats.json")
SNI_SERVICE_STATS_FILE = os.path.join(BASE_PATH, "sni_service_stats.json")
RUN_STATS_FILE = os.path.join(BASE_PATH, "run_stats.json")
MAX_NODE_STATS = 30000
MAX_SOURCE_NODE_LIST = 5000
# Ограничиваем только LIVE-CHECK: весь источник скачиваем и разбираем,
# но из каждого источника в конкретном запуске проверяем не более этого числа.


if not os.path.exists(SOURCES_FILE):
    logger.error("Файл sources.txt не найден")
    exit(1)


with open(SOURCES_FILE, "r", encoding="utf-8") as f:
    SOURCES = [
        line.strip()
        for line in f
        if line.strip() and not line.strip().startswith("#")
    ]


# ========== СПИСОК SNI ==========

DEFAULT_SNI = {
    "web.max.ru": {"attempts": 0, "success": 0},
    "vk.com": {"attempts": 0, "success": 0},
    "rutube.ru": {"attempts": 0, "success": 0},
    "mail.ru": {"attempts": 0, "success": 0},
    "ok.ru": {"attempts": 0, "success": 0},
    "yandex.ru": {"attempts": 0, "success": 0},
    "dzen.ru": {"attempts": 0, "success": 0},
    "gosuslugi.ru": {"attempts": 0, "success": 0},
    "ozon.ru": {"attempts": 0, "success": 0},
    "wildberries.ru": {"attempts": 0, "success": 0},
    "kinopoisk.ru": {"attempts": 0, "success": 0},
    "yandex.by": {"attempts": 0, "success": 0},
    "yandex.kz": {"attempts": 0, "success": 0},
    "telegram.org": {"attempts": 0, "success": 0},
    "cdn.x5.ru": {"attempts": 0, "success": 0},
    "storage.yandex.net": {"attempts": 0, "success": 0},
    "api-maps.yandex.ru": {"attempts": 0, "success": 0},
    "avatars.mds.yandex.net": {"attempts": 0, "success": 0},
    "sberbank.ru": {"attempts": 0, "success": 0},
    "tbank.ru": {"attempts": 0, "success": 0},
    "avito.ru": {"attempts": 0, "success": 0},
    "hh.ru": {"attempts": 0, "success": 0},
    "rambler.ru": {"attempts": 0, "success": 0},
    "lenta.ru": {"attempts": 0, "success": 0},
    "ria.ru": {"attempts": 0, "success": 0},
    "tass.ru": {"attempts": 0, "success": 0}
}


def empty_sni_stats():
    return {
        sni: {
            "attempts": 0,
            "success": 0
        }
        for sni in DEFAULT_SNI
    }


def load_sni_stats():

    if not os.path.exists(SNI_STATS_FILE):
        logger.info("📊 Файл статистики SNI не найден")
        logger.info("📊 Использую дефолтный список SNI")
        return empty_sni_stats()

    try:

        with open(SNI_STATS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        stats = {}

        # Поддержка старого формата:
        #
        # "api-maps.yandex.ru": 268
        #
        # преобразуется в:
        #
        # "api-maps.yandex.ru": {
        #     "attempts": 268,
        #     "success": 268
        # }

        old_format_found = False

        for sni, value in loaded.items():

            if isinstance(value, dict):

                stats[sni] = {
                    "attempts": int(value.get("attempts", 0)),
                    "success": int(value.get("success", 0))
                }

            elif isinstance(value, (int, float)):

                old_format_found = True

                old_value = int(value)

                stats[sni] = {
                    "attempts": old_value,
                    "success": old_value
                }

        # Добавляем новые SNI, которых ещё нет в файле

        for sni in DEFAULT_SNI:

            if sni not in stats:

                stats[sni] = {
                    "attempts": 0,
                    "success": 0
                }

        if old_format_found:

            logger.info(
                "📊 Обнаружен старый формат статистики SNI — "
                "выполнена совместимая конвертация"
            )

        logger.info(
            f"📊 Загружена статистика SNI: {len(stats)} доменов"
        )

        return stats

    except Exception as e:

        logger.warning(
            f"Ошибка загрузки статистики SNI: {e}"
        )

        logger.info(
            "📊 Использую дефолтный список SNI"
        )

        return empty_sni_stats()


def save_sni_stats(stats):

    try:

        with open(
            SNI_STATS_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                stats,
                f,
                indent=2,
                ensure_ascii=False
            )

        logger.info("💾 Статистика SNI сохранена")

    except Exception as e:

        logger.warning(
            f"Ошибка сохранения статистики: {e}"
        )


def get_sni_list(
    stats,
    top_count=15,
    random_count=5
):
    """
    Формирует максимум 20 SNI.

    Логика:
    1. SNI с attempts < MIN_SNI_ATTEMPTS считаются
       недостаточно исследованными и имеют приоритет.
    2. Из них и из уже исследованных формируем TOP15.
       Недостаточно исследованные выбираются случайно,
       чтобы один новый SNI не получал постоянный приоритет.
    3. Ещё RANDOM5 берутся случайно из остальных SNI.
       Это постоянное исследование менее проверенных вариантов.
    4. Для уже исследованных SNI используется сглаженная
       успешность (success + 2) / (attempts + 10), поэтому
       1/1 не обгоняет 268/589.
    """
    underexplored = [
        (sni, data)
        for sni, data in stats.items()
        if data.get("attempts", 0) < MIN_SNI_ATTEMPTS
    ]

    explored = [
        (sni, data)
        for sni, data in stats.items()
        if data.get("attempts", 0) >= MIN_SNI_ATTEMPTS
    ]

    random.shuffle(underexplored)

    def score(item):
        _, data = item
        attempts = data.get("attempts", 0)
        success = data.get("success", 0)

        # Сглаживание: маленькая выборка не может сразу стать №1.
        return (success + 2.0) / (attempts + 10.0)

    explored.sort(key=score, reverse=True)

    # Сначала недостаточно исследованные SNI.
    priority_pool = [sni for sni, _ in underexplored]

    # Затем лучшие уже исследованные SNI.
    priority_pool.extend(
        sni for sni, _ in explored
        if sni not in priority_pool
    )

    top = priority_pool[:top_count]

    # Остальные 5 — случайная разведка.
    remaining = [
        sni for sni in stats
        if sni not in top
    ]

    if remaining and random_count > 0:
        random_part = random.sample(
            remaining,
            min(random_count, len(remaining))
        )
    else:
        random_part = []

    selected = top + random_part

    random.shuffle(selected)

    return selected



SNI_STATS = load_sni_stats()


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


def empty_sni_service_stats():
    return {}


def source_bucket_template():
    return {
        "runs": 0,
        "fetch_success": 0,
        "fetch_errors": 0,
        "empty_responses": 0,
        "lines_total": 0,
        "vless_total": 0,
        "nodes_tested": 0,
        "live": 0,
        "dead": 0,
        "last_run": "",
        "last_http_status": 0,
        "last_error": "",
        "last_lines": 0,
        "last_vless": 0,
        "last_tested": 0,
        "last_live": 0,
        "last_dead": 0
    }


def ensure_all_source_buckets():
    for src in SOURCES:
        if src not in SOURCE_STATS:
            SOURCE_STATS[src] = source_bucket_template()
        else:
            base = source_bucket_template()
            base.update(SOURCE_STATS[src])
            SOURCE_STATS[src] = base


def ensure_service_bucket(stats, url):
    bucket = stats.setdefault(url, {
        "attempts": 0,
        "success": 0,
        "timeouts": 0,
        "connection_errors": 0,
        "http_errors": 0,
        "other_errors": 0,
        "status_codes": {}
    })
    bucket.setdefault("attempts", 0)
    bucket.setdefault("success", 0)
    bucket.setdefault("timeouts", 0)
    bucket.setdefault("connection_errors", 0)
    bucket.setdefault("http_errors", 0)
    bucket.setdefault("other_errors", 0)
    bucket.setdefault("status_codes", {})
    return bucket


SOURCE_STATS = load_json_stats(SOURCE_STATS_FILE, empty_source_stats)
NODE_STATS = load_json_stats(NODE_STATS_FILE, empty_node_stats)
SNI_SERVICE_STATS = load_json_stats(SNI_SERVICE_STATS_FILE, empty_sni_service_stats)
ensure_all_source_buckets()


def node_id(vless_uri):
    """Стабильный ID ноды без хранения UUID/полной строки как ключа."""
    return hashlib.sha256(vless_uri.encode("utf-8", errors="ignore")).hexdigest()[:24]


def extract_sni(vless_uri):
    try:
        q = parse_qs(urlparse(vless_uri).query)
        return q.get("sni", [""])[0].strip().lower()
    except Exception:
        return ""


def ensure_source_bucket(source):
    bucket = SOURCE_STATS.setdefault(source, source_bucket_template())
    base = source_bucket_template()
    base.update(bucket)
    SOURCE_STATS[source] = base
    return base


def ensure_node_record(vless_uri):
    nid = node_id(vless_uri)
    rec = NODE_STATS.setdefault(nid, {
        "attempts": 0, "live": 0, "original_live": 0,
        "mutated_live": 0, "dead": 0, "mutation_attempts": 0,
        "mutation_success": 0, "last_status": "unknown",
        "last_sni": "", "original_sni": extract_sni(vless_uri),
        "sources": [], "first_seen": "", "last_seen": "",
        "server": "", "server_port": 0, "transport": "", "protocol": "vless",
        "service_stats": {}
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


def record_sni_service(sni, service_url, result, lock):
    if not sni:
        sni = "<empty>"
    with lock:
        sni_bucket = SNI_SERVICE_STATS.setdefault(sni, {})
        item = sni_bucket.setdefault(service_url, {
            "attempts": 0,
            "success": 0,
            "timeouts": 0,
            "connection_errors": 0,
            "http_errors": 0,
            "other_errors": 0,
            "status_codes": {}
        })
        for key in ("attempts", "success", "timeouts", "connection_errors", "http_errors", "other_errors"):
            item.setdefault(key, 0)
        item.setdefault("status_codes", {})
        item["attempts"] += 1
        kind = result.get("kind", "other_error")
        if result.get("success"):
            item["success"] += 1
        elif kind == "timeout":
            item["timeouts"] += 1
        elif kind == "connection_error":
            item["connection_errors"] += 1
        elif kind == "http_error":
            item["http_errors"] += 1
        else:
            item["other_errors"] += 1
        status = result.get("status_code")
        if status is not None:
            key = str(status)
            item["status_codes"][key] = item["status_codes"].get(key, 0) + 1


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


# ========== ОСНОВНЫЕ НАСТРОЙКИ ==========

MAX_NODES = 100
MAX_THREADS = 40

TOP_SNI_COUNT = 15
RANDOM_SNI_COUNT = 5

SNI_SUCCESS_WEIGHT = 1

# SNI считается ещё недостаточно исследованным, пока не было
# MIN_SNI_ATTEMPTS реальных проверок.
MIN_SNI_ATTEMPTS = 10


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
    """Fetch one source and return (lines, metadata)."""
    meta = {
        "http_status": 0,
        "fetch_success": False,
        "empty": False,
        "error_type": "",
        "error": ""
    }
    try:
        headers = {
            "User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36"
        }
        r = requests.get(url, timeout=15, headers=headers)
        meta["http_status"] = r.status_code
        if r.status_code != 200:
            meta["error_type"] = "http_error"
            meta["error"] = f"HTTP {r.status_code}"
            return [], meta

        final_lines = []
        for line in r.text.splitlines():
            line = line.strip()
            if not line:
                continue
            if "://" not in line and len(line) > 50:
                final_lines.extend(decode_base64_content(line))
            else:
                final_lines.append(line)

        meta["fetch_success"] = True
        meta["empty"] = len(final_lines) == 0
        return final_lines, meta

    except requests.exceptions.Timeout as e:
        meta["error_type"] = "timeout"
        meta["error"] = str(e)[:300]
    except requests.exceptions.RequestException as e:
        meta["error_type"] = "request_error"
        meta["error"] = str(e)[:300]
    except Exception as e:
        meta["error_type"] = "fetch_error"
        meta["error"] = str(e)[:300]
    return [], meta


# ========== VLESS ==========

def is_valid_vless(line):

    return line.lower().startswith(
        "vless://"
    )


# ========== SNI MUTATION ==========

def mutate_node_sni(
    vless_uri,
    target_sni
):

    try:

        parsed = urlparse(vless_uri)

        query_params = parse_qs(
            parsed.query
        )

        mutated_params = {
            k: v[:]
            for k, v in query_params.items()
        }

        mutated_params["sni"] = [
            target_sni
        ]

        new_query = urlencode(
            mutated_params,
            doseq=True
        )

        orig_fragment = (
            unquote(parsed.fragment)
            if parsed.fragment
            else "node"
        )

        new_fragment = (
            f"{orig_fragment}-fixed-{target_sni}"
        )

        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            new_fragment
        ))

    except:

        return vless_uri


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
            "success": 0,
            "timeouts": 0,
            "connection_errors": 0,
            "http_errors": 0,
            "other_errors": 0,
            "status_codes": {}
        }
        for url in TEST_URLS
    }


def load_service_stats():
    if not os.path.exists(SERVICE_STATS_FILE):
        logger.info("📊 Файл статистики сервисов не найден")
        return empty_service_stats()
    try:
        with open(SERVICE_STATS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        stats = {}
        for url, value in loaded.items():
            if isinstance(value, dict):
                stats[url] = dict(value)
                ensure_service_bucket(stats, url)
        for url in TEST_URLS:
            ensure_service_bucket(stats, url)
        logger.info(f"📊 Загружена статистика сервисов: {len(stats)} сайтов")
        return stats
    except Exception as e:
        logger.warning(f"Ошибка загрузки статистики сервисов: {e}")
        return empty_service_stats()


def save_service_stats(stats):
    try:
        for url in TEST_URLS:
            ensure_service_bucket(stats, url)
        with open(SERVICE_STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        logger.info("💾 Статистика сервисов сохранена")
    except Exception as e:
        logger.warning(f"Ошибка сохранения статистики сервисов: {e}")


SERVICE_STATS = load_service_stats()


# ========== ПРОВЕРКА ОДНОЙ НОДЫ ==========

def check_single_uri(
    vless_uri,
    local_port,
    service_stats=None,
    stats_lock=None,
    tested_sni=None,
    node_id_value=None
):

    if stop_event.is_set():
        return False

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

        # Даём sing-box поднять локальный SOCKS
        for _ in range(15):

            if stop_event.is_set():
                return False

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
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/154.0.0.0 Safari/537.36"
        }

        # Проверяем все сервисы, чтобы одновременно собирать
        # статистику по каждому из них.
        # Нода считается живой, если хотя бы один сервис успешен.

        is_alive = False

        for url in TEST_URLS:
            if stop_event.is_set():
                return False

            result = {"success": False, "kind": "other_error", "status_code": None}
            try:
                response = requests.get(
                    url, proxies=proxies, timeout=2, headers=headers, allow_redirects=True
                )
                result["status_code"] = response.status_code
                result["success"] = response.status_code in [200, 204, 301, 302]
                result["kind"] = "success" if result["success"] else "http_error"
            except requests.exceptions.Timeout:
                result["kind"] = "timeout"
            except requests.exceptions.ConnectionError:
                result["kind"] = "connection_error"
            except requests.exceptions.RequestException:
                result["kind"] = "other_error"

            if service_stats is not None:
                if stats_lock:
                    with stats_lock:
                        bucket = ensure_service_bucket(service_stats, url)
                        bucket["attempts"] += 1
                        if result["success"]:
                            bucket["success"] += 1
                        elif result["kind"] == "timeout":
                            bucket["timeouts"] += 1
                        elif result["kind"] == "connection_error":
                            bucket["connection_errors"] += 1
                        elif result["kind"] == "http_error":
                            bucket["http_errors"] += 1
                        else:
                            bucket["other_errors"] += 1
                        if result["status_code"] is not None:
                            code = str(result["status_code"])
                            bucket["status_codes"][code] = bucket["status_codes"].get(code, 0) + 1
                else:
                    bucket = ensure_service_bucket(service_stats, url)
                    bucket["attempts"] += 1
                    if result["success"]:
                        bucket["success"] += 1

            if tested_sni is not None and stats_lock is not None:
                record_sni_service(tested_sni, url, result, stats_lock)

            if node_id_value is not None:
                with (stats_lock if stats_lock is not None else threading.Lock()):
                    node_rec = NODE_STATS.get(node_id_value)
                    if node_rec is not None:
                        service_bucket = node_rec.setdefault("service_stats", {}).setdefault(
                            url, {"attempts": 0, "success": 0}
                        )
                        service_bucket["attempts"] += 1
                        if result["success"]:
                            service_bucket["success"] += 1

            if result["success"]:
                is_alive = True

        return is_alive

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

    return False


# ========== WORKER ==========

def worker(
    task_queue,
    result_list,
    stats,
    lock,
    node_sources_map,
    run_stats
):

    """
    Воркер берёт задачи из очереди.

    1. Проверяем оригинальную ноду.
    2. Если живая — сохраняем.
    3. Если мёртвая Reality —
       перебираем SNI.
    4. Каждый SNI получает:
       attempts +1
    5. Если SNI оживил ноду:
       success +1
    """

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
        nid, node_rec = record_node_source(vless_uri, node_sources_map.get(vless_uri, []))
        original_sni = extract_sni(vless_uri)

        with lock:
            node_rec["attempts"] = node_rec.get("attempts", 0) + 1
            node_rec["last_status"] = "testing"
            run_stats["nodes_tested"] += 1

        try:

            # ===== СНАЧАЛА ОРИГИНАЛЬНЫЙ SNI =====

            is_alive = check_single_uri(
                vless_uri,
                local_port,
                SERVICE_STATS,
                lock,
                tested_sni=original_sni,
                node_id_value=nid
            )

            if is_alive:

                logger.debug(
                    f"[LIVE] Исходная нода рабочая "
                    f"(порт {local_port})"
                )

                result_uri = vless_uri
                with lock:
                    node_rec["live"] += 1
                    node_rec["original_live"] += 1
                    node_rec["last_status"] = "original_live"
                    node_rec["last_sni"] = original_sni
                    run_stats["original_live"] += 1
                    for src in node_sources_map.get(vless_uri, []):
                        b = ensure_source_bucket(src)
                        b["nodes_tested"] += 1
                        b["live"] += 1

            # ===== ЕСЛИ МЕРТВАЯ REALITY =====

            elif (
                "security=reality"
                in vless_uri.lower()
                or "pbk="
                in vless_uri.lower()
            ) and stats:

                sni_list = get_sni_list(
                    stats,
                    TOP_SNI_COUNT,
                    RANDOM_SNI_COUNT
                )

                for sni in sni_list:

                    if stop_event.is_set():
                        break

                    mutated_uri = mutate_node_sni(
                        vless_uri,
                        sni
                    )

                    # СНАЧАЛА фиксируем попытку.
                    # Теперь это настоящая статистика:
                    # каждый реально проверенный SNI
                    # получает attempts +1.

                    with lock:

                        if sni not in stats:
                            stats[sni] = {"attempts": 0, "success": 0}
                        stats[sni]["attempts"] += 1
                        node_rec["mutation_attempts"] += 1
                        run_stats["mutation_attempts"] += 1
                        for src in node_sources_map.get(vless_uri, []):
                            b = ensure_source_bucket(src)

                    # ===== ПРОВЕРЯЕМ SNI =====

                    if check_single_uri(
                        mutated_uri,
                        local_port,
                        SERVICE_STATS,
                        lock,
                        tested_sni=sni,
                        node_id_value=nid
                    ):

                        logger.info(
                            f"[🎉 SNI WORKED] "
                            f"Нода ожила с SNI: {sni} "
                            f"(порт {local_port})"
                        )

                        with lock:

                            stats[sni]["success"] += SNI_SUCCESS_WEIGHT
                            node_rec["live"] += 1
                            node_rec["mutated_live"] += 1
                            node_rec["mutation_success"] += 1
                            node_rec["last_status"] = "mutated_live"
                            node_rec["last_sni"] = sni
                            run_stats["mutated_live"] += 1
                            run_stats["mutation_success"] += 1
                            for src in node_sources_map.get(vless_uri, []):
                                b = ensure_source_bucket(src)
                                b["live"] += 1

                        result_uri = mutated_uri

                        break

            else:

                logger.debug(
                    "[SKIP] Не Reality, "
                    "мутация не поддерживается"
                )

        except Exception as e:

            logger.error(
                f"Ошибка в воркере: {e}"
            )

        finally:

            if result_uri is None:
                with lock:
                    node_rec["dead"] = node_rec.get("dead", 0) + 1
                    node_rec["last_status"] = "dead"
                    run_stats["dead"] += 1
                    for src in node_sources_map.get(vless_uri, []):
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
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "sources": len(SOURCES), "source_fetch_success": 0,
        "lines_total": 0, "vless_total": 0, "unique_nodes": 0,
        "nodes_tested": 0, "original_live": 0, "mutated_live": 0,
        "dead": 0, "mutation_attempts": 0, "mutation_success": 0,
        "candidates_after_source_sampling": 0
    }

    for url in SOURCES:
        bucket = ensure_source_bucket(url)
        bucket["runs"] += 1
        bucket["fetch_attempts"] += 1
        bucket["last_run"] = run_stats["started_at"]
        bucket["last_error"] = ""
        nodes, meta = fetch_source(url)
        bucket["last_http_status"] = meta.get("http_status", 0)
        if meta.get("fetch_success"):
            bucket["fetch_success"] += 1
            run_stats["source_fetch_success"] += 1
        elif meta.get("error_type") == "http_error":
            bucket["http_errors"] += 1
            bucket["last_error"] = meta.get("error", "")
        elif meta.get("error_type"):
            bucket["fetch_errors"] += 1
            bucket["last_error"] = meta.get("error", "")
        if meta.get("empty"):
            bucket["empty_responses"] += 1
        bucket["lines_total"] += len(nodes)
        bucket["last_lines"] = len(nodes)
        run_stats["lines_total"] += len(nodes)
        logger.info(f"Loaded {len(nodes)} lines from {url[:80]}... [HTTP {meta.get('http_status', 0)}]")
        all_nodes.extend(nodes)
        source_unique = set()
        for line in nodes:
            if is_valid_vless(line):
                clean = line.strip()
                source_nodes[url].append(clean)
                source_unique.add(clean)
                bucket["vless_total"] += 1
                run_stats["vless_total"] += 1
        bucket["last_vless"] = len(source_unique)


    # =========================================
    # STEP 2
    # =========================================

    ensure_all_source_buckets()

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

    logger.info(f"Unique VLESS configs: {len(unique_nodes)}")
    run_stats["unique_nodes"] = len(unique_nodes)

    node_sources_map = defaultdict(list)
    for src, nodes in source_nodes.items():
        for node in set(nodes):
            node_sources_map[node].append(src)


    # =========================================
    # ARCHIVES
    # =========================================

    archive_path = os.path.join(
        BASE_PATH,
        "archive.txt"
    )

    alive_archive_path = os.path.join(
        BASE_PATH,
        "alive_archive.txt"
    )

    archive_list = []
    alive_archive_list = []


    if os.path.exists(archive_path):

        with open(
            archive_path,
            "r",
            encoding="utf-8"
        ) as f:

            archive_list = [
                x.strip()
                for x in f
                if x.strip()
            ]


    if os.path.exists(
        alive_archive_path
    ):

        with open(
            alive_archive_path,
            "r",
            encoding="utf-8"
        ) as f:

            alive_archive_list = [
                x.strip()
                for x in f
                if x.strip()
            ]


    archive_seen = set(
        archive_list
    )


    for node in unique_nodes:

        if node not in archive_seen:

            archive_list.append(node)

            archive_seen.add(node)


    if len(archive_list) > 10000:

        archive_list = archive_list[-10000:]


    with open(
        archive_path,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(
            "\n".join(archive_list)
        )


    logger.info(
        f"Archive updated: "
        f"{len(archive_list)} nodes "
        f"(limit 10000)"
    )


    # =========================================
    # PRIORITY ORDER / SOURCE SAMPLING
    # =========================================

    # Все уникальные ноды из всех загруженных источников идут в LIVE-CHECK.
    # Ограничений 30 нод/источник и 350 нод суммарно больше нет.
    # Дедупликация выполняется выше, поэтому одна и та же нода проверяется один раз,
    # но сохраняется связь ноды со всеми источниками через node_sources_map.
    priority_order = []
    priority_seen = set()

    # Сначала недавно живые ноды, если они есть среди текущих уникальных нод.
    for node in reversed(alive_archive_list):
        if node in seen and node not in priority_seen:
            priority_order.append(node)
            priority_seen.add(node)

    # Затем все остальные уникальные ноды текущего запуска.
    for node in unique_nodes:
        if node not in priority_seen:
            priority_order.append(node)
            priority_seen.add(node)

    run_stats["candidates_after_source_sampling"] = len(priority_order)

    logger.info(
        f"Source sampling disabled: checking all {len(priority_order)} unique VLESS nodes"
    )


    # =========================================
    # LIVE CHECK
    # =========================================

    logger.info(
        f"\n--- STEP 3: LIVE CHECK "
        f"({len(priority_order)} nodes, "
        f"threads: {MAX_THREADS}) ---"
    )

    start_time = time.time()

    task_queue = queue.Queue()

    for node in priority_order:

        task_queue.put(node)


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
                    SNI_STATS,
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
    # SOURCE LAST-RUN STATS
    # =========================================

    for src in SOURCES:
        b = ensure_source_bucket(src)
        tested = b.get("nodes_tested", 0)
        # Cumulative counters above remain useful, while these last-run
        # fields make it immediately visible which source gave a result.
        # The current run contribution is calculated from nodes selected
        # for this source below.
        source_pool = set(source_nodes.get(src, []))
        tested_here = len(source_pool & set(priority_order))
        live_here = len(source_pool & set(result_list))
        b["last_tested"] = tested_here
        b["last_live"] = live_here
        b["last_dead"] = max(0, tested_here - live_here)

    # =========================================
    # RESULTS
    # =========================================

    alive_nodes = result_list

    logger.info(
        f"\n--- RESULT: Found "
        f"{len(alive_nodes)} live nodes ---"
    )


    alive_nodes = list(
        dict.fromkeys(
            alive_nodes
        )
    )


    logger.info(
        f"After dedup: "
        f"{len(alive_nodes)}"
    )


    # =========================================
    # FILL FROM ARCHIVE
    # =========================================

    if len(alive_nodes) < MAX_NODES:

        added = 0

        for node in reversed(
            alive_archive_list
        ):

            if node not in alive_nodes:

                alive_nodes.append(
                    node
                )

                added += 1

                if len(alive_nodes) >= MAX_NODES:
                    break

        logger.info(
            f"Filled from archive "
            f"(+{added}), total: "
            f"{len(alive_nodes)}"
        )


    # =========================================
    # UPDATE ALIVE ARCHIVE
    # =========================================

    for node in alive_nodes:

        if node in alive_archive_list:

            alive_archive_list.remove(
                node
            )

        alive_archive_list.append(
            node
        )


    if len(alive_archive_list) > 5000:

        alive_archive_list = (
            alive_archive_list[-5000:]
        )


    with open(
        alive_archive_path,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(
            "\n".join(
                alive_archive_list
            )
        )


    logger.info(
        f"alive_archive.txt updated: "
        f"{len(alive_archive_list)} nodes "
        f"(limit 5000)"
    )


    # =========================================
    # НЕТ НОД
    # =========================================

    if len(alive_nodes) == 0:

        logger.warning(
            "0 live nodes, "
            "skipping update"
        )

        save_sni_stats(
            SNI_STATS
        )

        save_service_stats(SERVICE_STATS)
        save_json_stats(SOURCE_STATS_FILE, SOURCE_STATS, "Статистика источников")
        save_json_stats(NODE_STATS_FILE, NODE_STATS, "Статистика нод")
        save_json_stats(SNI_SERVICE_STATS_FILE, SNI_SERVICE_STATS, "Связь SNI→сервисы")
        run_stats["finished_at"] = datetime.now().isoformat(timespec="seconds")
        save_json_stats(RUN_STATS_FILE, run_stats, "Статистика запуска")
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
                alive_nodes[:MAX_NODES]
            )
        )


    logger.info(
        f"Subscription updated: "
        f"{out_path}"
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


    for node in alive_nodes:

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

        attempts = data.get("attempts", 0)
        success = data.get("success", 0)

        if attempts > 0:
            rate = success / attempts * 100
        else:
            rate = 0

        logger.info(
            f"{url}: {success}/{attempts} успешных ({rate:.1f}%) | "
            f"timeout={data.get('timeouts', 0)} "
            f"conn={data.get('connection_errors', 0)} "
            f"http={data.get('http_errors', 0)} "
            f"other={data.get('other_errors', 0)}"
        )

    save_service_stats(SERVICE_STATS)

    prune_node_stats()
    save_json_stats(SOURCE_STATS_FILE, SOURCE_STATS, "Статистика источников")
    save_json_stats(NODE_STATS_FILE, NODE_STATS, "Статистика нод")
    save_json_stats(SNI_SERVICE_STATS_FILE, SNI_SERVICE_STATS, "Связь SNI→сервисы")


    ensure_all_source_buckets()

    # =========================================
    # SNI STATS
    # =========================================

    logger.info(
        "\n--- TOP SNI STATS ---"
    )


    sorted_sni = sorted(
        SNI_STATS.items(),
        key=lambda x: (
            (x[1].get("success", 0) / x[1].get("attempts", 1)) if x[1].get("attempts", 0) else 0,
            x[1].get("attempts", 0)
        ),
        reverse=True
    )


    for sni, data in sorted_sni[:15]:

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
            f"{sni}: "
            f"{success}/{attempts} "
            f"успешных "
            f"({rate:.1f}%)"
        )


    # =========================================
    # SAVE SNI STATS
    # =========================================

    save_sni_stats(SNI_STATS)
    run_stats["finished_at"] = datetime.now().isoformat(timespec="seconds")
    save_json_stats(RUN_STATS_FILE, run_stats, "Статистика запуска")


# =========================================
# START
# =========================================

if __name__ == "__main__":

    main()
