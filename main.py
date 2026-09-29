import os
import re
import json
import time
import base64
import random
import queue
import socket
import shutil
import tempfile
import threading
import subprocess
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import requests
from concurrent.futures import ThreadPoolExecutor


# ============================================================
# CONFIG
# ============================================================

SOURCES_FILE = "sources.txt"

ARCHIVE_FILE = "archive.txt"
ALIVE_ARCHIVE_FILE = "alive_archive.txt"

OUTPUT_DIR = "subs"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "vless_001.txt")

SNI_STATS_FILE = "sni_stats.json"

MAX_NODES = 100
MAX_THREADS = 20

BASE_PORT = 11000

ARCHIVE_LIMIT = 10000
ALIVE_ARCHIVE_LIMIT = 5000

# Для первого теста не даём старому архиву разогнать проверку
# до десятков тысяч узлов.
#
# 0 = проверять всё.
MAX_CANDIDATES = 1000

HTTP_TIMEOUT = 4.0

SOCKS_START_TIMEOUT = 3.0
SOCKS_POLL_INTERVAL = 0.1

SOURCE_TIMEOUT = 15.0

SNI_WARMUP_ATTEMPTS = 100

TOP_SNI_COUNT = 8
RANDOM_SNI_COUNT = 5

SNI_DISABLE_AFTER = 50


# ============================================================
# SNI
# ============================================================

DEFAULT_SNI = [
    "web.max.ru",
    "vk.com",
    "rutube.ru",
    "mail.ru",
    "ok.ru",
    "yandex.ru",
    "dzen.ru",
    "gosuslugi.ru",
    "ozon.ru",
    "wildberries.ru",
    "kinopoisk.ru",
    "yandex.by",
    "yandex.kz",
    "telegram.org",
    "cdn.x5.ru",
    "storage.yandex.net",
    "api-maps.yandex.ru",
    "avatars.mds.yandex.net",
    "sberbank.ru",
    "tbank.ru",
    "avito.ru",
    "hh.ru",
    "rambler.ru",
    "lenta.ru",
    "ria.ru",
    "tass.ru",
]


# ============================================================
# TEST TARGETS
# ============================================================

QUICK_TEST_URL = "https://www.youtube.com/generate_204"

VALIDATION_TARGETS = [
    {
        "name": "YouTube",
        "url": "https://www.youtube.com/generate_204",
        "expected_codes": {200, 204, 301, 302},
    },
    {
        "name": "Instagram",
        "url": "https://www.instagram.com/",
        "expected_codes": {200, 301, 302},
    },
    {
        "name": "Telegram",
        "url": "https://api.telegram.org",
        "expected_codes": {200, 400, 401, 404},
    },
]


# ============================================================
# GLOBAL STATE
# ============================================================

stats_lock = threading.Lock()
results_lock = threading.Lock()

stop_event = threading.Event()

SNI_STATS = {}

ALIVE_RESULTS = []

CHECKED_COUNT = 0
LIVE_COUNT = 0

PROTOCOL_STATS = {
    "total": 0,
    "reality": 0,
    "tcp": 0,
    "ws": 0,
    "grpc": 0,
    "xhttp": 0,
    "other": 0,
}


# ============================================================
# UTILS
# ============================================================

def now_string():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_print(message):
    print(message, flush=True)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def is_vless(line):
    return line.strip().lower().startswith("vless://")


def normalize_line(line):
    return line.strip().replace("\r", "").replace("\n", "")


# ============================================================
# FILE IO
# ============================================================

def read_node_file(path):
    if not os.path.exists(path):
        return []

    result = []

    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = normalize_line(line)

                if not line:
                    continue

                if is_vless(line):
                    result.append(line)

    except Exception as e:
        safe_print(f"[WARN] Не удалось прочитать {path}: {e}")

    return result


def write_node_file(path, nodes):
    directory = os.path.dirname(path)

    if directory:
        ensure_dir(directory)

    with open(path, "w", encoding="utf-8") as f:
        for node in nodes:
            f.write(node + "\n")


# ============================================================
# BASE64
# ============================================================

def decode_base64_content(content):
    """
    Поддерживает:
    - обычные vless:// строки;
    - base64;
    - base64url;
    - несколько строк.
    """

    if not content:
        return []

    content = content.strip()

    # Сначала проверяем обычный текст
    direct = []

    for line in content.splitlines():
        line = normalize_line(line)

        if is_vless(line):
            direct.append(line)

    if direct:
        return direct

    # Убираем пробелы/переносы
    compact = re.sub(r"\s+", "", content)

    # padding
    compact += "=" * (-len(compact) % 4)

    decoded = None

    try:
        decoded = base64.b64decode(compact, validate=False).decode(
            "utf-8",
            errors="ignore"
        )
    except Exception:
        pass

    if not decoded:
        try:
            decoded = base64.urlsafe_b64decode(compact).decode(
                "utf-8",
                errors="ignore"
            )
        except Exception:
            pass

    if not decoded:
        return []

    result = []

    for line in decoded.splitlines():
        line = normalize_line(line)

        if is_vless(line):
            result.append(line)

    return result


# ============================================================
# SOURCES
# ============================================================

def fetch_source(source):
    source = source.strip()

    if not source:
        return []

    if is_vless(source):
        return [source]

    if not source.startswith(("http://", "https://")):
        return []

    try:
        response = requests.get(
            source,
            timeout=SOURCE_TIMEOUT,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 "
                    "(Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 "
                    "Chrome/154 Safari/537.36"
                )
            },
        )

        response.raise_for_status()

        nodes = decode_base64_content(response.text)

        safe_print(
            f"[SOURCE] {source} -> {len(nodes)} VLESS"
        )

        return nodes

    except Exception as e:
        safe_print(
            f"[SOURCE-ERROR] {source} -> {e}"
        )

        return []


def load_sources():
    if not os.path.exists(SOURCES_FILE):
        safe_print(
            f"[ERROR] Нет файла {SOURCES_FILE}"
        )
        return []

    sources = []

    with open(
        SOURCES_FILE,
        "r",
        encoding="utf-8",
        errors="ignore"
    ) as f:
        for line in f:
            line = line.strip()

            if line and not line.startswith("#"):
                sources.append(line)

    safe_print(
        f"[SOURCE] Источников найдено: {len(sources)}"
    )

    all_nodes = []

    for source in sources:
        nodes = fetch_source(source)
        all_nodes.extend(nodes)

    return deduplicate_nodes(all_nodes)


# ============================================================
# VLESS PARSER
# ============================================================

def parse_vless_uri(uri):
    try:
        parsed = urlsplit(uri)

        if parsed.scheme.lower() != "vless":
            return None

        uuid = parsed.username

        if not uuid:
            return None

        server = parsed.hostname

        if not server:
            return None

        port = parsed.port

        if not port:
            return None

        params = dict(parse_qsl(
            parsed.query,
            keep_blank_values=True
        ))

        fragment = parsed.fragment

        return {
            "uuid": uuid,
            "server": server,
            "port": port,
            "params": params,
            "fragment": fragment,
        }

    except Exception:
        return None


def canonical_vless_key(uri):
    parsed = parse_vless_uri(uri)

    if not parsed:
        return None

    params = parsed["params"]

    normalized_params = urlencode(
        sorted(params.items()),
        doseq=True
    )

    return (
        f"{parsed['uuid']}@"
        f"{parsed['server']}:{parsed['port']}?"
        f"{normalized_params}"
    )


def deduplicate_nodes(nodes):
    result = []
    seen = set()

    for node in nodes:
        node = normalize_line(node)

        if not is_vless(node):
            continue

        key = canonical_vless_key(node)

        if not key:
            continue

        if key in seen:
            continue

        seen.add(key)
        result.append(node)

    return result


# ============================================================
# SNI STATS
# ============================================================

def default_sni_record():
    return {
        "success": 0,
        "attempts": 0,
        "last_success": None,
    }


def load_sni_stats():
    stats = {}

    if os.path.exists(SNI_STATS_FILE):
        try:
            with open(
                SNI_STATS_FILE,
                "r",
                encoding="utf-8"
            ) as f:
                raw = json.load(f)

            if isinstance(raw, dict):
                for sni, value in raw.items():

                    # Старый формат:
                    # "vk.com": 15
                    if isinstance(value, int):
                        stats[sni] = {
                            "success": value,
                            "attempts": value,
                            "last_success": None,
                        }

                    # Новый формат
                    elif isinstance(value, dict):
                        stats[sni] = {
                            "success": int(
                                value.get("success", 0)
                            ),
                            "attempts": int(
                                value.get("attempts", 0)
                            ),
                            "last_success": value.get(
                                "last_success"
                            ),
                        }

        except Exception as e:
            safe_print(
                f"[SNI] Ошибка загрузки статистики: {e}"
            )

    # Добавляем отсутствующие SNI
    for sni in DEFAULT_SNI:
        if sni not in stats:
            stats[sni] = default_sni_record()

    return stats


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
                ensure_ascii=False,
                indent=2
            )

    except Exception as e:
        safe_print(
            f"[SNI] Ошибка сохранения статистики: {e}"
        )


def total_sni_attempts(stats):
    total = 0

    for value in stats.values():
        try:
            total += int(value.get("attempts", 0))
        except Exception:
            pass

    return total


def get_sni_list(stats):
    """
    Пока статистики мало — используем ВСЕ SNI.

    После накопления статистики:
      TOP 8
      + до 5 неисследованных
      + случайные из оставшихся.

    SNI с >=50 попытками и 0 успехов временно отключаются.
    """

    attempts_total = total_sni_attempts(stats)

    # ========================================================
    # WARMUP
    # ========================================================

    if attempts_total < SNI_WARMUP_ATTEMPTS:
        result = DEFAULT_SNI.copy()
        random.shuffle(result)

        return result

    # ========================================================
    # ADAPTIVE MODE
    # ========================================================

    usable = []

    for sni in DEFAULT_SNI:

        item = stats.get(
            sni,
            default_sni_record()
        )

        attempts = int(
            item.get("attempts", 0)
        )

        success = int(
            item.get("success", 0)
        )

        # Пока SNI исследован недостаточно — оставляем.
        # Полностью неудачные после 50 попыток временно убираем.
        if (
            attempts >= SNI_DISABLE_AFTER
            and success == 0
        ):
            continue

        rate = (
            success / attempts
            if attempts > 0
            else 0
        )

        usable.append(
            (
                sni,
                rate,
                success,
                attempts
            )
        )

    # TOP
    usable.sort(
        key=lambda x: (
            x[1],
            x[2],
            x[3],
        ),
        reverse=True
    )

    selected = []

    for item in usable:
        sni = item[0]

        if sni not in selected:
            selected.append(sni)

        if len(selected) >= TOP_SNI_COUNT:
            break

    # Неисследованные
    unexplored = [
        item[0]
        for item in usable
        if item[3] == 0
        and item[0] not in selected
    ]

    random.shuffle(unexplored)

    for sni in unexplored[:RANDOM_SNI_COUNT]:
        selected.append(sni)

    # Заполняем случайными
    remaining = [
        item[0]
        for item in usable
        if item[0] not in selected
    ]

    random.shuffle(remaining)

    needed = TOP_SNI_COUNT + RANDOM_SNI_COUNT

    if len(selected) < needed:
        selected.extend(
            remaining[:needed - len(selected)]
        )

    return selected


def record_sni_attempt(
    stats,
    sni,
    success
):
    with stats_lock:

        if sni not in stats:
            stats[sni] = default_sni_record()

        stats[sni]["attempts"] = (
            int(stats[sni].get("attempts", 0)) + 1
        )

        if success:
            stats[sni]["success"] = (
                int(stats[sni].get("success", 0)) + 1
            )

            stats[sni]["last_success"] = now_string()


# ============================================================
# SNI MUTATION
# ============================================================

def mutate_node_sni(uri, new_sni):
    parsed = parse_vless_uri(uri)

    if not parsed:
        return None

    params = parsed["params"].copy()

    params["sni"] = new_sni

    query = urlencode(
        params,
        doseq=True
    )

    username = parsed["uuid"]

    server = parsed["server"]

    port = parsed["port"]

    fragment = parsed["fragment"]

    result = urlunsplit(
        (
            "vless",
            f"{username}@{server}:{port}",
            "",
            query,
            fragment,
        )
    )

    return result


# ============================================================
# SING-BOX CONFIG
# ============================================================

def parse_vless_to_json(uri, socks_port):
    parsed = parse_vless_uri(uri)

    if not parsed:
        return None

    params = parsed["params"]

    server = parsed["server"]
    server_port = parsed["port"]
    uuid = parsed["uuid"]

    network = (
        params.get("type")
        or params.get("network")
        or "tcp"
    ).lower()

    security = (
        params.get("security")
        or ""
    ).lower()

    outbound = {
        "type": "vless",
        "tag": "proxy",

        "server": server,
        "server_port": server_port,
        "uuid": uuid,

        "network": "tcp",
    }

    # ========================================================
    # FLOW
    # ========================================================

    flow = params.get("flow")

    if flow:
        outbound["flow"] = flow

    # ========================================================
    # PACKET ENCODING
    # ========================================================

    packet_encoding = params.get(
        "packetEncoding"
    )

    if packet_encoding:
        outbound["packet_encoding"] = packet_encoding

    # ========================================================
    # TLS
    # ========================================================

    if security in ("tls", "reality"):

        tls = {
            "enabled": True
        }

        sni = (
            params.get("sni")
            or params.get("host")
        )

        if sni:
            tls["server_name"] = sni

        fp = params.get("fp")

        if fp:
            tls["utls"] = {
                "enabled": True,
                "fingerprint": fp,
            }

        alpn = params.get("alpn")

        if alpn:
            tls["alpn"] = [
                x.strip()
                for x in alpn.split(",")
                if x.strip()
            ]

        if security == "reality":

            pbk = (
                params.get("pbk")
                or params.get("publicKey")
            )

            sid = (
                params.get("sid")
                or params.get("shortId")
            )

            if not pbk:
                return None

            reality = {
                "enabled": True,
                "public_key": pbk,
            }

            if sid:
                reality["short_id"] = sid

            tls["reality"] = reality

        outbound["tls"] = tls

    # ========================================================
    # TRANSPORT
    # ========================================================

    path = params.get("path", "")
    host = params.get("host", "")

    if network == "ws":

        transport = {
            "type": "ws"
        }

        if path:
            transport["path"] = path

        if host:
            transport["headers"] = {
                "Host": host
            }

        outbound["transport"] = transport

    elif network == "grpc":

        service_name = (
            params.get("serviceName")
            or params.get("service_name")
            or ""
        )

        transport = {
            "type": "grpc"
        }

        if service_name:
            transport["service_name"] = service_name

        outbound["transport"] = transport

    elif network == "httpupgrade":

        transport = {
            "type": "httpupgrade"
        }

        if path:
            transport["path"] = path

        if host:
            transport["host"] = host

        outbound["transport"] = transport

    elif network in ("http", "h2", "xhttp", "splithttp"):

        # ВАЖНО:
        # sing-box 1.14.2 документирует V2Ray HTTP transport.
        # Отдельного XHTTP transport в документации sing-box
        # нет. Поэтому xhttp/splithttp здесь НЕ объявляется
        # как несуществующий type "xhttp".
        #
        # Такой узел будет проверяться через HTTP transport.
        # Это лучше, чем отправлять sing-box заведомо
        # невалидный конфиг.

        transport = {
            "type": "http"
        }

        if path:
            transport["path"] = path

        if host:
            transport["host"] = [host]

        outbound["transport"] = transport

    # ========================================================
    # SOCKS
    # ========================================================

    config = {
        "log": {
            "level": "error"
        },

        "inbounds": [
            {
                "type": "socks",
                "tag": "socks-in",
                "listen": "127.0.0.1",
                "listen_port": socks_port,
            }
        ],

        "outbounds": [
            outbound
        ]
    }

    return config


# ============================================================
# SOCKS READY
# ============================================================

def wait_for_socks(port):
    deadline = (
        time.time()
        + SOCKS_START_TIMEOUT
    )

    while time.time() < deadline:

        try:
            with socket.create_connection(
                ("127.0.0.1", port),
                timeout=0.3
            ):
                return True

        except Exception:
            time.sleep(
                SOCKS_POLL_INTERVAL
            )

    return False


# ============================================================
# REQUEST SESSION
# ============================================================

def create_socks_session(port):
    session = requests.Session()

    proxy = (
        f"socks5h://127.0.0.1:{port}"
    )

    session.proxies.update({
        "http": proxy,
        "https": proxy,
    })

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/154.0.0.0 "
            "Safari/537.36"
        )
    })

    return session


# ============================================================
# QUICK TEST
# ============================================================

def quick_test(session):
    try:

        response = session.get(
            QUICK_TEST_URL,
            timeout=HTTP_TIMEOUT,
            allow_redirects=False,
        )

        return response.status_code in {
            200,
            204,
            301,
            302,
        }

    except Exception:
        return False


# ============================================================
# DEEP TEST
# ============================================================

def deep_test(session):
    statuses = {}

    all_ok = True

    for target in VALIDATION_TARGETS:

        name = target["name"]

        try:

            response = session.get(
                target["url"],
                timeout=HTTP_TIMEOUT,
                allow_redirects=False,
            )

            code = response.status_code

            ok = (
                code in target["expected_codes"]
            )

            statuses[name] = code

            if not ok:
                all_ok = False

        except Exception as e:

            statuses[name] = str(e)

            all_ok = False

    return all_ok, statuses


# ============================================================
# SINGLE NODE CHECK
# ============================================================

def check_single_uri(
    uri,
    socks_port,
    deep=False
):
    config = parse_vless_to_json(
        uri,
        socks_port
    )

    if not config:
        return False, {}

    temp_path = None
    process = None

    try:

        fd, temp_path = tempfile.mkstemp(
            suffix=".json"
        )

        os.close(fd)

        with open(
            temp_path,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                config,
                f,
                ensure_ascii=False
            )

        process = subprocess.Popen(
            [
                "sing-box",
                "run",
                "-c",
                temp_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        if not wait_for_socks(socks_port):
            return False, {}

        session = create_socks_session(
            socks_port
        )

        # ====================================================
        # QUICK
        # ====================================================

        if not quick_test(session):
            return False, {}

        # ====================================================
        # QUICK ONLY
        # ====================================================

        if not deep:
            return True, {}

        # ====================================================
        # DEEP
        # ====================================================

        ok, statuses = deep_test(session)

        return ok, statuses

    except Exception:
        return False, {}

    finally:

        if process:

            try:
                process.terminate()

                try:
                    process.wait(
                        timeout=1.5
                    )
                except subprocess.TimeoutExpired:

                    process.kill()

                    try:
                        process.wait(
                            timeout=1
                        )
                    except Exception:
                        pass

            except Exception:
                pass

        if temp_path:

            try:
                os.remove(temp_path)
            except Exception:
                pass


# ============================================================
# REALITY DETECTION
# ============================================================

def is_reality_node(uri):
    parsed = parse_vless_uri(uri)

    if not parsed:
        return False

    params = parsed["params"]

    security = (
        params.get("security")
        or ""
    ).lower()

    if security == "reality":
        return True

    if params.get("pbk"):
        return True

    if params.get("publicKey"):
        return True

    return False


# ============================================================
# PROTOCOL STATS
# ============================================================

def register_protocol(uri):
    parsed = parse_vless_uri(uri)

    if not parsed:
        return

    params = parsed["params"]

    network = (
        params.get("type")
        or params.get("network")
        or "tcp"
    ).lower()

    security = (
        params.get("security")
        or ""
    ).lower()

    with results_lock:

        PROTOCOL_STATS["total"] += 1

        if security == "reality" or params.get("pbk"):
            PROTOCOL_STATS["reality"] += 1

        if network == "tcp":
            PROTOCOL_STATS["tcp"] += 1

        elif network == "ws":
            PROTOCOL_STATS["ws"] += 1

        elif network == "grpc":
            PROTOCOL_STATS["grpc"] += 1

        elif network in (
            "xhttp",
            "splithttp",
        ):
            PROTOCOL_STATS["xhttp"] += 1

        else:
            PROTOCOL_STATS["other"] += 1


# ============================================================
# WORKER
# ============================================================

def worker(
    task_queue,
    worker_id,
    total_candidates
):
    global CHECKED_COUNT
    global LIVE_COUNT

    socks_port = (
        BASE_PORT + worker_id
    )

    while not stop_event.is_set():

        try:
            node = task_queue.get(
                timeout=0.5
            )

        except queue.Empty:
            return

        try:

            if stop_event.is_set():
                continue

            # =================================================
            # ORIGINAL NODE
            # =================================================

            ok, statuses = check_single_uri(
                node,
                socks_port,
                deep=True
            )

            if ok:

                with results_lock:

                    if len(ALIVE_RESULTS) < MAX_NODES:

                        ALIVE_RESULTS.append(node)

                        LIVE_COUNT = len(
                            ALIVE_RESULTS
                        )

                        CHECKED_COUNT += 1

                        current_checked = (
                            CHECKED_COUNT
                        )

                        current_live = (
                            LIVE_COUNT
                        )

                        register_protocol(node)

                        safe_print(
                            f"[LIVE] "
                            f"{current_checked}/{total_candidates} "
                            f"| LIVE "
                            f"{current_live}/{MAX_NODES} "
                            f"| original"
                        )

                        if current_live >= MAX_NODES:
                            stop_event.set()

                continue

            # =================================================
            # REALITY SNI FALLBACK
            # =================================================

            if is_reality_node(node):

                with stats_lock:
                    sni_list = get_sni_list(
                        SNI_STATS
                    )

                for sni in sni_list:

                    if stop_event.is_set():
                        break

                    mutated = mutate_node_sni(
                        node,
                        sni
                    )

                    if not mutated:
                        continue

                    # -----------------------------------------
                    # QUICK SNI TEST
                    # -----------------------------------------

                    quick_ok, _ = check_single_uri(
                        mutated,
                        socks_port,
                        deep=False
                    )

                    record_sni_attempt(
                        SNI_STATS,
                        sni,
                        quick_ok
                    )

                    if not quick_ok:
                        continue

                    # -----------------------------------------
                    # DEEP TEST
                    # -----------------------------------------

                    deep_ok, deep_statuses = (
                        check_single_uri(
                            mutated,
                            socks_port,
                            deep=True
                        )
                    )

                    if not deep_ok:
                        continue

                    with results_lock:

                        if len(ALIVE_RESULTS) < MAX_NODES:

                            ALIVE_RESULTS.append(
                                mutated
                            )

                            LIVE_COUNT = len(
                                ALIVE_RESULTS
                            )

                            CHECKED_COUNT += 1

                            current_checked = (
                                CHECKED_COUNT
                            )

                            current_live = (
                                LIVE_COUNT
                            )

                            register_protocol(
                                mutated
                            )

                            safe_print(
                                f"[LIVE] "
                                f"{current_checked}/"
                                f"{total_candidates} "
                                f"| LIVE "
                                f"{current_live}/"
                                f"{MAX_NODES} "
                                f"| SNI={sni} "
                                f"| "
                                f"YT={deep_statuses.get('YouTube')} "
                                f"IG={deep_statuses.get('Instagram')} "
                                f"TG={deep_statuses.get('Telegram')}"
                            )

                            if current_live >= MAX_NODES:
                                stop_event.set()

                    break

            # =================================================
            # DEAD
            # =================================================

            with results_lock:

                CHECKED_COUNT += 1

                current_checked = (
                    CHECKED_COUNT
                )

                current_live = len(
                    ALIVE_RESULTS
                )

                LIVE_COUNT = current_live

                # Не спамим логом абсолютно каждой проверки,
                # если узел мёртвый.
                if (
                    current_checked <= 20
                    or current_checked % 10 == 0
                ):
                    safe_print(
                        f"[CHECK] "
                        f"{current_checked}/"
                        f"{total_candidates} "
                        f"| LIVE "
                        f"{current_live}/"
                        f"{MAX_NODES}"
                    )

        except Exception as e:

            with results_lock:

                CHECKED_COUNT += 1

                current_checked = (
                    CHECKED_COUNT
                )

                current_live = len(
                    ALIVE_RESULTS
                )

                LIVE_COUNT = current_live

                safe_print(
                    f"[ERROR] "
                    f"{current_checked}/"
                    f"{total_candidates} "
                    f"| {type(e).__name__}: {e}"
                )

        finally:

            task_queue.task_done()


# ============================================================
# ARCHIVE
# ============================================================

def update_archive(current_nodes):
    old_archive = read_node_file(
        ARCHIVE_FILE
    )

    combined = (
        current_nodes
        + old_archive
    )

    combined = deduplicate_nodes(
        combined
    )

    combined = combined[:ARCHIVE_LIMIT]

    write_node_file(
        ARCHIVE_FILE,
        combined
    )

    safe_print(
        f"[ARCHIVE] "
        f"archive.txt: {len(combined)}"
    )


def update_alive_archive(validated_nodes):
    old_alive = read_node_file(
        ALIVE_ARCHIVE_FILE
    )

    combined = (
        validated_nodes
        + old_alive
    )

    combined = deduplicate_nodes(
        combined
    )

    combined = combined[
        :ALIVE_ARCHIVE_LIMIT
    ]

    write_node_file(
        ALIVE_ARCHIVE_FILE,
        combined
    )

    safe_print(
        f"[ALIVE-ARCHIVE] "
        f"alive_archive.txt: {len(combined)}"
    )


def build_candidates(current_nodes):
    alive_archive = read_node_file(
        ALIVE_ARCHIVE_FILE
    )

    archive = read_node_file(
        ARCHIVE_FILE
    )

    # Сначала свежие источники,
    # затем ранее подтверждённые,
    # затем старый архив.
    combined = (
        current_nodes
        + alive_archive
        + archive
    )

    combined = deduplicate_nodes(
        combined
    )

    if MAX_CANDIDATES > 0:
        combined = combined[
            :MAX_CANDIDATES
        ]

    return combined


# ============================================================
# WRITE RESULTS
# ============================================================

def write_results():
    ensure_dir(OUTPUT_DIR)

    nodes = deduplicate_nodes(
        ALIVE_RESULTS
    )

    nodes = nodes[:MAX_NODES]

    write_node_file(
        OUTPUT_FILE,
        nodes
    )

    safe_print(
        f"[OUTPUT] "
        f"{OUTPUT_FILE}: {len(nodes)}"
    )


# ============================================================
# PRINT STATS
# ============================================================

def print_final_stats():
    safe_print("")
    safe_print("=" * 60)
    safe_print("FINAL STATS")
    safe_print("=" * 60)

    safe_print(
        f"Проверено: {CHECKED_COUNT}"
    )

    safe_print(
        f"Рабочих: {len(ALIVE_RESULTS)}"
    )

    safe_print(
        f"Цель: {MAX_NODES}"
    )

    safe_print("")

    safe_print(
        f"Всего VLESS: "
        f"{PROTOCOL_STATS['total']}"
    )

    safe_print(
        f"Reality: "
        f"{PROTOCOL_STATS['reality']}"
    )

    safe_print(
        f"TCP: "
        f"{PROTOCOL_STATS['tcp']}"
    )

    safe_print(
        f"WS: "
        f"{PROTOCOL_STATS['ws']}"
    )

    safe_print(
        f"gRPC: "
        f"{PROTOCOL_STATS['grpc']}"
    )

    safe_print(
        f"XHTTP/splithttp: "
        f"{PROTOCOL_STATS['xhttp']}"
    )

    safe_print(
        f"Other: "
        f"{PROTOCOL_STATS['other']}"
    )

    safe_print("")

    total_attempts = total_sni_attempts(
        SNI_STATS
    )

    safe_print(
        f"SNI attempts: {total_attempts}"
    )

    if total_attempts > 0:

        sorted_sni = []

        for sni, item in SNI_STATS.items():

            attempts = int(
                item.get("attempts", 0)
            )

            success = int(
                item.get("success", 0)
            )

            if attempts > 0:

                rate = (
                    success / attempts
                )

                sorted_sni.append(
                    (
                        sni,
                        success,
                        attempts,
                        rate
                    )
                )

        sorted_sni.sort(
            key=lambda x: (
                x[3],
                x[1],
                x[2]
            ),
            reverse=True
        )

        safe_print(
            "TOP SNI:"
        )

        for (
            sni,
            success,
            attempts,
            rate
        ) in sorted_sni[:10]:

            safe_print(
                f"  {sni}: "
                f"{success}/{attempts} "
                f"({rate * 100:.1f}%)"
            )

    safe_print("=" * 60)


# ============================================================
# MAIN
# ============================================================

def main():

    global SNI_STATS

    start_time = time.time()

    safe_print("")
    safe_print("=" * 60)
    safe_print("SBORNIK START")
    safe_print("=" * 60)

    safe_print(
        f"[TIME] {now_string()}"
    )

    safe_print(
        f"[CONFIG] "
        f"threads={MAX_THREADS}, "
        f"target={MAX_NODES}, "
        f"candidate_limit={MAX_CANDIDATES}"
    )

    # ========================================================
    # CHECK SING-BOX
    # ========================================================

    if not shutil.which("sing-box"):

        safe_print(
            "[ERROR] sing-box не найден"
        )

        return 1

    try:

        version_result = subprocess.run(
            [
                "sing-box",
                "version"
            ],
            capture_output=True,
            text=True,
            timeout=10
        )

        safe_print(
            version_result.stdout.strip()
        )

    except Exception as e:

        safe_print(
            f"[ERROR] sing-box version: {e}"
        )

        return 1

    # ========================================================
    # LOAD SNI STATS
    # ========================================================

    SNI_STATS = load_sni_stats()

    safe_print(
        f"[SNI] "
        f"Всего SNI: {len(DEFAULT_SNI)}"
    )

    safe_print(
        f"[SNI] "
        f"Всего попыток в статистике: "
        f"{total_sni_attempts(SNI_STATS)}"
    )

    if (
        total_sni_attempts(SNI_STATS)
        < SNI_WARMUP_ATTEMPTS
    ):

        safe_print(
            "[SNI] Статистики пока мало -> "
            "используем ВСЕ 26 SNI"
        )

    else:

        safe_print(
            "[SNI] Статистика накоплена -> "
            "используем adaptive TOP/RANDOM"
        )

    # ========================================================
    # LOAD SOURCES
    # ========================================================

    current_nodes = load_sources()

    safe_print(
        f"[SOURCE] "
        f"Уникальных свежих VLESS: "
        f"{len(current_nodes)}"
    )

    # ========================================================
    # ARCHIVE
    # ========================================================

    update_archive(
        current_nodes
    )

    # ========================================================
    # BUILD CANDIDATES
    # ========================================================

    candidates = build_candidates(
        current_nodes
    )

    safe_print(
        f"[QUEUE] "
        f"Кандидатов на проверку: "
        f"{len(candidates)}"
    )

    if not candidates:

        safe_print(
            "[ERROR] Нет кандидатов"
        )

        save_sni_stats(
            SNI_STATS
        )

        return 1

    # ========================================================
    # TASK QUEUE
    # ========================================================

    task_queue = queue.Queue()

    for node in candidates:
        task_queue.put(node)

    total_candidates = len(
        candidates
    )

    # ========================================================
    # WORKERS
    # ========================================================

    safe_print(
        f"[WORKERS] "
        f"Запускаем {MAX_THREADS} workers"
    )

    with ThreadPoolExecutor(
        max_workers=MAX_THREADS
    ) as executor:

        futures = []

        for worker_id in range(
            MAX_THREADS
        ):

            future = executor.submit(
                worker,
                task_queue,
                worker_id,
                total_candidates
            )

            futures.append(future)

        # Ждём завершения worker'ов.
        for future in futures:

            try:
                future.result()

            except Exception as e:

                safe_print(
                    f"[WORKER-ERROR] {e}"
                )

    # ========================================================
    # SAVE
    # ========================================================

    save_sni_stats(
        SNI_STATS
    )

    if ALIVE_RESULTS:

        update_alive_archive(
            ALIVE_RESULTS
        )

        write_results()

    else:

        safe_print(
            "[OUTPUT] Рабочих узлов не найдено"
        )

    # ========================================================
    # FINAL
    # ========================================================

    elapsed = (
        time.time()
        - start_time
    )

    print_final_stats()

    safe_print(
        f"[TIME] "
        f"Время выполнения: "
        f"{elapsed:.1f} сек."
    )

    safe_print(
        "[DONE]"
    )

    return 0


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    raise SystemExit(
        main()
    )
