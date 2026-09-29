import base64
import json
import os
import queue
import random
import subprocess
import tempfile
import time
import urllib.parse
import requests
import threading

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


# ============================================================
# FILES
# ============================================================

SOURCES_FILE = "sources.txt"
ARCHIVE_FILE = "archive.txt"
ALIVE_ARCHIVE_FILE = "alive_archive.txt"

OUTPUT_DIR = "subs"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "vless_001.txt")

SNI_STATS_FILE = "sni_stats.json"


# ============================================================
# LIMITS
# ============================================================

MAX_NODES = 100

# Не 40: 20 — более безопасный компромисс для GitHub Runner.
MAX_THREADS = 20

BASE_PORT = 11000

ARCHIVE_LIMIT = 10000
ALIVE_ARCHIVE_LIMIT = 5000

# Время ожидания ответа целевых сайтов.
HTTP_TIMEOUT = 4.0

# Максимальное время ожидания появления SOCKS.
SOCKS_START_TIMEOUT = 3.0

# Небольшая пауза между попытками проверки SOCKS.
SOCKS_POLL_INTERVAL = 0.1


# ============================================================
# SNI
# ============================================================
#
# НЕ СОКРАЩАЕМ СПИСОК ПОКА НЕТ СТАТИСТИКИ.
#
# После накопления статистики программа сама начнёт
# отдавать приоритет успешным SNI.
#

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

# Пока статистики мало — используем весь пул.
TOP_SNI_COUNT = 8
RANDOM_SNI_COUNT = 5

# После такого количества неудачных попыток SNI
# перестаёт получать высокий приоритет.
SNI_DISABLE_AFTER = 50

# До этого количества попыток SNI считается
# недостаточно исследованным.
SNI_MIN_ATTEMPTS = 10


# ============================================================
# TARGETS
# ============================================================

# Быстрый предварительный тест.
#
# Он нужен только для экономии времени.
# Если нода вообще не может установить нормальное
# HTTPS-соединение — не тратим время на глубокую проверку.
#
# После него всё равно обязательно идут все 3 сервиса.

QUICK_TEST_URL = "https://www.youtube.com/generate_204"


# Обязательная глубокая проверка.
#
# Все три должны пройти.
#
# Telegram может возвращать 200/400/401/404:
# это всё равно означает, что HTTP-соединение до API
# состоялось.
#
# Instagram может отдавать редиректы.
#

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

stop_event = threading.Event()

results_lock = threading.Lock()
sni_lock = threading.Lock()

alive_results = []

SNI_STATS = {}


# ============================================================
# SNI STATISTICS
# ============================================================

def new_sni_record():
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
                loaded = json.load(f)

            if isinstance(loaded, dict):

                for sni, value in loaded.items():

                    # Новый формат.
                    if isinstance(value, dict):

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

                    # Поддержка старого формата:
                    #
                    # "vk.com": 37
                    #
                    elif isinstance(value, (int, float)):

                        success = int(value)

                        stats[sni] = {
                            "success": success,
                            "attempts": max(success, 1),
                            "last_success": None,
                        }

        except Exception as e:

            print(
                f"[WARN] Ошибка загрузки "
                f"SNI statistics: {e}"
            )

    # Добавляем отсутствующие SNI.
    for sni in DEFAULT_SNI:

        if sni not in stats:
            stats[sni] = new_sni_record()

    return stats


def save_sni_stats(stats):

    try:

        tmp_file = SNI_STATS_FILE + ".tmp"

        with open(
            tmp_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                stats,
                f,
                indent=2,
                ensure_ascii=False
            )

        os.replace(
            tmp_file,
            SNI_STATS_FILE
        )

    except Exception as e:

        print(
            f"[WARN] Ошибка сохранения "
            f"SNI statistics: {e}"
        )


def sni_success_rate(record):

    attempts = int(
        record.get("attempts", 0)
    )

    success = int(
        record.get("success", 0)
    )

    if attempts <= 0:
        return 0.0

    return success / attempts


def record_sni_attempt(
    stats,
    sni,
    success
):

    record = stats.setdefault(
        sni,
        new_sni_record()
    )

    record["attempts"] = (
        int(record.get("attempts", 0)) + 1
    )

    if success:

        record["success"] = (
            int(record.get("success", 0)) + 1
        )

        record["last_success"] = (
            datetime.now().isoformat(
                timespec="seconds"
            )
        )


def get_sni_list(stats):

    candidates = []

    for sni, record in stats.items():

        attempts = int(
            record.get("attempts", 0)
        )

        success = int(
            record.get("success", 0)
        )

        rate = sni_success_rate(record)

        # Полностью бесполезный SNI после большого
        # количества попыток убираем из приоритета.
        if (
            attempts >= SNI_DISABLE_AFTER
            and success == 0
        ):
            continue

        candidates.append(
            (
                sni,
                success,
                attempts,
                rate
            )
        )

    # Сначала лучшие по проценту успеха,
    # затем по количеству успехов.
    candidates.sort(
        key=lambda x: (
            x[3],
            x[1],
            x[2]
        ),
        reverse=True
    )

    top = [
        item[0]
        for item in candidates[:TOP_SNI_COUNT]
    ]

    # Exploration:
    # пробуем SNI, у которых ещё мало статистики.
    unexplored = [
        item
        for item in candidates
        if (
            item[0] not in top
            and item[2] < SNI_MIN_ATTEMPTS
        )
    ]

    random.shuffle(unexplored)

    exploration = [
        item[0]
        for item in unexplored[
            :RANDOM_SNI_COUNT
        ]
    ]

    result = top + exploration

    # Если после фильтрации получилось меньше,
    # добираем остальные случайно.
    if len(result) < TOP_SNI_COUNT + RANDOM_SNI_COUNT:

        remaining = [
            item[0]
            for item in candidates
            if item[0] not in result
        ]

        random.shuffle(remaining)

        need = (
            TOP_SNI_COUNT
            + RANDOM_SNI_COUNT
            - len(result)
        )

        result.extend(
            remaining[:need]
        )

    # Если статистики ещё практически нет,
    # result фактически будет представлять весь пул.
    random.shuffle(result)

    return result


# ============================================================
# VLESS PARSING
# ============================================================

def parse_vless_uri(uri):

    if not uri:
        return None

    if not uri.lower().startswith("vless://"):
        return None

    try:

        parts = urllib.parse.urlsplit(uri)

        uuid = parts.username
        host = parts.hostname

        if not uuid or not host:
            return None

        try:
            port = parts.port
        except ValueError:
            return None

        if not port:
            port = 443

        params = urllib.parse.parse_qs(
            parts.query,
            keep_blank_values=True
        )

        # parse_qs выдаёт списки.
        params = {
            key: value[0]
            for key, value in params.items()
        }

        return {
            "uuid": uuid,
            "host": host,
            "port": port,
            "params": params,
            "fragment": parts.fragment,
        }

    except Exception:

        return None


# ============================================================
# VLESS DEDUPLICATION
# ============================================================

def canonical_vless_key(uri):

    parsed = parse_vless_uri(uri)

    if not parsed:
        return uri.strip()

    params = parsed["params"]

    # Фрагмент #название не учитываем.
    #
    # Важные параметры сохраняем.
    normalized_params = tuple(
        sorted(
            (
                str(k).lower(),
                str(v)
            )
            for k, v in params.items()
        )
    )

    return (
        parsed["uuid"].lower(),
        parsed["host"].lower(),
        int(parsed["port"]),
        normalized_params,
    )


def deduplicate_nodes(nodes):

    result = []
    seen = set()

    for node in nodes:

        node = node.strip()

        if not node:
            continue

        if not node.lower().startswith(
            "vless://"
        ):
            continue

        key = canonical_vless_key(node)

        if key in seen:
            continue

        seen.add(key)
        result.append(node)

    return result


# ============================================================
# VLESS SNI MUTATION
# ============================================================

def mutate_node_sni(
    vless_uri,
    new_sni
):

    parsed = parse_vless_uri(
        vless_uri
    )

    if not parsed:
        return None

    params = parsed["params"].copy()

    params["sni"] = new_sni

    # ВАЖНО:
    #
    # fragment НЕ меняем.
    #
    # Это безопаснее для клиентов.
    #

    query = urllib.parse.urlencode(
        params,
        doseq=False
    )

    fragment = parsed["fragment"]

    result = (
        f"vless://"
        f"{parsed['uuid']}"
        f"@"
        f"{parsed['host']}"
        f":"
        f"{parsed['port']}"
        f"?{query}"
    )

    if fragment:
        result += "#" + fragment

    return result


# ============================================================
# SING-BOX CONFIG
# ============================================================

def parse_vless_to_json(
    vless_uri,
    local_port
):

    parsed = parse_vless_uri(
        vless_uri
    )

    if not parsed:
        return None

    p = parsed["params"]

    security = p.get(
        "security",
        ""
    ).lower()

    network = p.get(
        "type",
        "tcp"
    ).lower()

    outbound = {
        "type": "vless",
        "tag": "proxy",

        "server": parsed["host"],
        "server_port": parsed["port"],

        "uuid": parsed["uuid"],

        "network": network,
    }

    # --------------------------------------------------------
    # FLOW
    # --------------------------------------------------------

    if p.get("flow"):
        outbound["flow"] = p["flow"]

    # --------------------------------------------------------
    # TLS / REALITY
    # --------------------------------------------------------

    if security in {
        "tls",
        "reality"
    }:

        tls = {
            "enabled": True,
            "server_name": p.get(
                "sni",
                parsed["host"]
            ),

            "utls": {
                "enabled": True,
                "fingerprint": p.get(
                    "fp",
                    "chrome"
                ),
            },
        }

        if security == "reality":

            public_key = p.get(
                "pbk",
                ""
            )

            short_id = p.get(
                "sid",
                ""
            )

            if not public_key:
                return None

            tls["reality"] = {
                "enabled": True,
                "public_key": public_key,
                "short_id": short_id,
            }

        outbound["tls"] = tls

    # --------------------------------------------------------
    # TRANSPORT: WS
    # --------------------------------------------------------

    if network == "ws":

        ws_headers = {}

        host_header = p.get(
            "host"
        )

        if host_header:

            ws_headers["Host"] = (
                host_header
            )

        outbound["transport"] = {
            "type": "ws",
            "path": p.get(
                "path",
                "/"
            ),
        }

        if ws_headers:

            outbound["transport"][
                "headers"
            ] = ws_headers

    # --------------------------------------------------------
    # TRANSPORT: GRPC
    # --------------------------------------------------------

    elif network == "grpc":

        service_name = p.get(
            "serviceName",
            p.get(
                "service_name",
                ""
            )
        )

        outbound["transport"] = {
            "type": "grpc",
            "service_name": service_name,
        }

    # --------------------------------------------------------
    # TRANSPORT: XHTTP
    # --------------------------------------------------------

    elif network in {
        "xhttp",
        "splithttp"
    }:

        transport = {
            "type": "http",
        }

        path = p.get(
            "path"
        )

        if path:
            transport["path"] = path

        host_header = p.get(
            "host"
        )

        if host_header:
            transport["host"] = host_header

        outbound["transport"] = transport

    # --------------------------------------------------------
    # SOCKS INBOUND
    # --------------------------------------------------------

    config = {
        "inbounds": [
            {
                "type": "socks",
                "tag": "socks-in",

                "listen": "127.0.0.1",

                "listen_port": local_port,

                "users": [],
            }
        ],

        "outbounds": [
            outbound
        ],
    }

    return config


# ============================================================
# SOCKS READINESS
# ============================================================

def wait_for_socks(
    local_port,
    timeout=SOCKS_START_TIMEOUT
):

    deadline = (
        time.monotonic()
        + timeout
    )

    while time.monotonic() < deadline:

        try:

            sock = socket_create_connection(
                "127.0.0.1",
                local_port,
                timeout=0.2
            )

            sock.close()

            return True

        except Exception:

            time.sleep(
                SOCKS_POLL_INTERVAL
            )

    return False


def socket_create_connection(
    host,
    port,
    timeout
):

    import socket

    return socket.create_connection(
        (host, port),
        timeout=timeout
    )


# ============================================================
# HTTP THROUGH SOCKS
# ============================================================

def request_through_socks(
    session,
    url
):

    try:

        response = session.get(
            url,
            timeout=HTTP_TIMEOUT,
            allow_redirects=False,
            headers={
                "User-Agent":
                    "Mozilla/5.0 "
                    "(Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 "
                    "(KHTML, like Gecko) "
                    "Chrome/140.0 Safari/537.36"
            },
        )

        return response.status_code

    except Exception:

        return None


# ============================================================
# QUICK TEST
# ============================================================

def quick_test(
    session
):

    status = request_through_socks(
        session,
        QUICK_TEST_URL
    )

    return status in {
        200,
        204,
        301,
        302,
    }


# ============================================================
# DEEP TEST
# ============================================================

def deep_test(
    session
):

    statuses = {}

    all_ok = True

    for target in VALIDATION_TARGETS:

        status = request_through_socks(
            session,
            target["url"]
        )

        statuses[
            target["name"]
        ] = status

        if status not in target[
            "expected_codes"
        ]:

            all_ok = False

    return all_ok, statuses


# ============================================================
# SINGLE NODE TEST
# ============================================================

def check_single_uri(
    vless_uri,
    local_port,
    deep=True
):

    config = parse_vless_to_json(
        vless_uri,
        local_port
    )

    if not config:
        return False, {}

    temp_path = None
    process = None

    try:

        # ----------------------------------------------------
        # Temporary config.
        # ----------------------------------------------------

        fd, temp_path = tempfile.mkstemp(
            prefix="sb_",
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

        # ----------------------------------------------------
        # Start sing-box.
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Wait until SOCKS is actually ready.
        # ----------------------------------------------------

        if not wait_for_socks(
            local_port
        ):

            return False, {
                "SOCKS": None
            }

        # ----------------------------------------------------
        # requests through SOCKS5H.
        #
        # socks5h = DNS through proxy.
        # ----------------------------------------------------

        proxy = (
            f"socks5h://127.0.0.1:"
            f"{local_port}"
        )

        session = requests.Session()

        session.proxies.update({
            "http": proxy,
            "https": proxy,
        })

        # ----------------------------------------------------
        # Quick test.
        # ----------------------------------------------------

        if not quick_test(
            session
        ):

            return False, {
                "QUICK": False
            }

        if not deep:

            return True, {
                "QUICK": True
            }

        # ----------------------------------------------------
        # Full mandatory test.
        # ----------------------------------------------------

        full_ok, statuses = deep_test(
            session
        )

        return full_ok, statuses

    except Exception as e:

        return False, {
            "ERROR": str(e)
        }

    finally:

        # ----------------------------------------------------
        # Stop sing-box.
        # ----------------------------------------------------

        if process is not None:

            try:

                process.terminate()

                try:

                    process.wait(
                        timeout=1.5
                    )

                except subprocess.TimeoutExpired:

                    process.kill()
                    process.wait(
                        timeout=1
                    )

            except Exception:
                pass

        # ----------------------------------------------------
        # Remove temp config.
        # ----------------------------------------------------

        if temp_path:

            try:

                if os.path.exists(
                    temp_path
                ):
                    os.remove(
                        temp_path
                    )

            except Exception:
                pass


# ============================================================
# REALITY CHECK
# ============================================================

def is_reality_node(
    vless_uri
):

    parsed = parse_vless_uri(
        vless_uri
    )

    if not parsed:
        return False

    params = parsed["params"]

    security = params.get(
        "security",
        ""
    ).lower()

    return (
        security == "reality"
        or bool(params.get("pbk"))
    )


# ============================================================
# WORKER
# ============================================================

def worker(
    vless_uri,
    local_port
):

    if stop_event.is_set():

        return None, None, {}

    # --------------------------------------------------------
    # 1. Original node.
    #
    # Сначала быстрая + полная проверка.
    # --------------------------------------------------------

    ok, statuses = check_single_uri(
        vless_uri,
        local_port,
        deep=True
    )

    if ok:

        return (
            vless_uri,
            None,
            statuses
        )

    # --------------------------------------------------------
    # 2. SNI mutation.
    #
    # Только Reality.
    # --------------------------------------------------------

    if not is_reality_node(
        vless_uri
    ):

        return None, None, statuses

    # Берём актуальный список SNI
    # на основании накопленной статистики.
    with sni_lock:

        sni_list = get_sni_list(
            SNI_STATS
        )

    for sni in sni_list:

        if stop_event.is_set():

            break

        mutated_uri = mutate_node_sni(
            vless_uri,
            sni
        )

        if not mutated_uri:
            continue

        # ----------------------------------------------------
        # Сначала быстрый тест.
        #
        # Важно: здесь deep=False.
        # Не тратим 3 запроса на каждый SNI.
        # ----------------------------------------------------

        quick_ok, quick_status = (
            check_single_uri(
                mutated_uri,
                local_port,
                deep=False
            )
        )

        with sni_lock:

            record_sni_attempt(
                SNI_STATS,
                sni,
                quick_ok
            )

        if not quick_ok:

            continue

        # ----------------------------------------------------
        # SNI технически ожил.
        #
        # Теперь обязательная глубокая проверка.
        # ----------------------------------------------------

        full_ok, full_statuses = (
            check_single_uri(
                mutated_uri,
                local_port,
                deep=True
            )
        )

        if full_ok:

            # Дополнительный success для SNI
            # здесь НЕ записываем.
            #
            # Он уже получил success за quick test.
            #
            # Иначе одна попытка считалась бы
            # дважды.

            return (
                mutated_uri,
                sni,
                full_statuses
            )

    return None, None, statuses


# ============================================================
# SOURCE FETCH
# ============================================================

def decode_base64_content(
    content
):

    content = content.strip()

    if not content:
        return []

    # Сначала проверяем обычный текст.
    lines = content.splitlines()

    if any(
        line.strip().lower().startswith(
            "vless://"
        )
        for line in lines
    ):

        return [
            line.strip()
            for line in lines
            if line.strip()
        ]

    # Затем Base64.
    try:

        normalized = "".join(
            content.split()
        )

        padding = (
            "="
            * (
                -len(normalized)
                % 4
            )
        )

        decoded = base64.b64decode(
            normalized + padding
        ).decode(
            "utf-8",
            errors="ignore"
        )

        return decoded.splitlines()

    except Exception:

        return lines


def fetch_source(
    source_url
):

    try:

        response = requests.get(
            source_url,
            timeout=15,
            headers={
                "User-Agent":
                    "Mozilla/5.0"
            },
        )

        response.raise_for_status()

        return decode_base64_content(
            response.text
        )

    except Exception as e:

        print(
            f"[SOURCE ERROR] "
            f"{source_url}: {e}"
        )

        return []


# ============================================================
# FILE HELPERS
# ============================================================

def read_node_file(
    filename
):

    if not os.path.exists(
        filename
    ):

        return []

    try:

        with open(
            filename,
            "r",
            encoding="utf-8",
            errors="ignore"
        ) as f:

            return [
                line.strip()
                for line in f
                if line.strip()
            ]

    except Exception:

        return []


def write_node_file(
    filename,
    nodes
):

    directory = os.path.dirname(
        filename
    )

    if directory:

        os.makedirs(
            directory,
            exist_ok=True
        )

    tmp_file = filename + ".tmp"

    with open(
        tmp_file,
        "w",
        encoding="utf-8"
    ) as f:

        if nodes:

            f.write(
                "\n".join(nodes)
            )

            f.write("\n")

    os.replace(
        tmp_file,
        filename
    )


# ============================================================
# ARCHIVE
# ============================================================

def update_archive(
    current_nodes
):

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

    combined = combined[
        :ARCHIVE_LIMIT
    ]

    write_node_file(
        ARCHIVE_FILE,
        combined
    )


def build_candidates(
    current_nodes
):

    current_nodes = deduplicate_nodes(
        current_nodes
    )

    alive_archive = deduplicate_nodes(
        read_node_file(
            ALIVE_ARCHIVE_FILE
        )
    )

    archive_nodes = deduplicate_nodes(
        read_node_file(
            ARCHIVE_FILE
        )
    )

    # Сначала ранее жившие.
    # Затем свежие.
    # Затем остальные архивные.
    #
    # НО ВСЕ ОНИ БУДУТ ЗАНОВО ПРОВЕРЕНЫ.
    #

    candidates = (
        alive_archive
        + current_nodes
        + archive_nodes
    )

    return deduplicate_nodes(
        candidates
    )


def update_alive_archive(
    validated_nodes
):

    old_alive = deduplicate_nodes(
        read_node_file(
            ALIVE_ARCHIVE_FILE
        )
    )

    validated_nodes = deduplicate_nodes(
        validated_nodes
    )

    # Свежие проверенные ноды ставим вперед.
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


# ============================================================
# MAIN
# ============================================================

def main():

    global SNI_STATS

    start_time = time.time()

    print()
    print(
        "=========================================="
    )
    print(
        "        SBORNIK VLESS CHECKER"
    )
    print(
        "=========================================="
    )
    print()

    # --------------------------------------------------------
    # SNI stats.
    # --------------------------------------------------------

    SNI_STATS = load_sni_stats()

    print(
        f"[SNI] Загружено SNI: "
        f"{len(SNI_STATS)}"
    )

    # --------------------------------------------------------
    # Check sing-box.
    # --------------------------------------------------------

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

        print(
            "[SING-BOX]",
            version.stdout.strip()
        )

    except Exception as e:

        print(
            f"[FATAL] sing-box недоступен: {e}"
        )

        return

    # --------------------------------------------------------
    # Load sources.
    # --------------------------------------------------------

    print()
    print(
        "[1/6] Загрузка источников..."
    )

    sources = read_node_file(
        SOURCES_FILE
    )

    all_nodes = []

    for source in sources:

        # Если строка уже VLESS —
        # это тоже допускаем.
        if source.lower().startswith(
            "vless://"
        ):

            all_nodes.append(
                source
            )

            continue

        nodes = fetch_source(
            source
        )

        all_nodes.extend(
            nodes
        )

        print(
            f"  {source} -> "
            f"{len(nodes)} строк"
        )

    current_nodes = deduplicate_nodes(
        all_nodes
    )

    print(
        f"[+] Уникальных VLESS: "
        f"{len(current_nodes)}"
    )

    # --------------------------------------------------------
    # Update general archive.
    # --------------------------------------------------------

    print()
    print(
        "[2/6] Обновление общего архива..."
    )

    update_archive(
        current_nodes
    )

    # --------------------------------------------------------
    # Build candidates.
    # --------------------------------------------------------

    print()
    print(
        "[3/6] Формирование очереди проверки..."
    )

    candidates = build_candidates(
        current_nodes
    )

    print(
        f"[+] К проверке: "
        f"{len(candidates)}"
    )

    if not candidates:

        print(
            "[ERROR] Нет VLESS нод."
        )

        return

    # --------------------------------------------------------
    # Prepare ports.
    # --------------------------------------------------------

    port_queue = queue.Queue()

    for i in range(
        MAX_THREADS
    ):

        port_queue.put(
            BASE_PORT + i
        )

    # --------------------------------------------------------
    # Check nodes.
    # --------------------------------------------------------

    print()
    print(
        "[4/6] Проверка нод:"
    )

    print(
        "      YouTube + Instagram + Telegram"
    )

    alive_results.clear()

    stop_event.clear()

    futures = {}

    with ThreadPoolExecutor(
        max_workers=MAX_THREADS
    ) as executor:

        for node in candidates:

            if stop_event.is_set():
                break

            port = port_queue.get()

            future = executor.submit(
                worker,
                node,
                port
            )

            futures[
                future
            ] = port

        for future in as_completed(
            futures
        ):

            port = futures[
                future
            ]

            try:

                result_uri, used_sni, statuses = (
                    future.result()
                )

                if result_uri:

                    # ----------------------------------------
                    # Строгое ограничение 100.
                    # ----------------------------------------

                    with results_lock:

                        if (
                            len(alive_results)
                            < MAX_NODES
                        ):

                            alive_results.append(
                                result_uri
                            )

                            count = len(
                                alive_results
                            )

                            if used_sni:

                                print(
                                    f"[+] "
                                    f"{count}/{MAX_NODES} "
                                    f"SNI={used_sni}"
                                )

                            else:

                                print(
                                    f"[+] "
                                    f"{count}/{MAX_NODES} "
                                    f"ORIGINAL"
                                )

                            if count >= MAX_NODES:

                                stop_event.set()

            except Exception as e:

                print(
                    f"[WORKER ERROR] {e}"
                )

            finally:

                # Порт обязательно возвращаем.
                port_queue.put(
                    port
                )

    # --------------------------------------------------------
    # Deduplicate final.
    # --------------------------------------------------------

    alive_results[:] = deduplicate_nodes(
        alive_results
    )

    alive_results[:] = alive_results[
        :MAX_NODES
    ]

    # --------------------------------------------------------
    # Statistics.
    # --------------------------------------------------------

    elapsed = (
        time.time()
        - start_time
    )

    print()
    print(
        "[5/6] Результат проверки"
    )

    print(
        f"  Рабочих нод: "
        f"{len(alive_results)}"
    )

    print(
        f"  Время: "
        f"{elapsed:.1f} сек."
    )

    # --------------------------------------------------------
    # Do NOT overwrite subscription if
    # absolutely nothing passed.
    # --------------------------------------------------------

    if not alive_results:

        print()
        print(
            "[WARNING] Ни одной ноды не прошло "
            "полную проверку."
        )

        print(
            "[WARNING] Существующая подписка "
            "НЕ перезаписывается."
        )

        # Статистику SNI всё равно сохраняем.
        save_sni_stats(
            SNI_STATS
        )

        return

    # --------------------------------------------------------
    # Save subscription.
    # --------------------------------------------------------

    print()
    print(
        "[6/6] Сохранение результатов..."
    )

    write_node_file(
        OUTPUT_FILE,
        alive_results
    )

    # --------------------------------------------------------
    # Alive archive.
    #
    # Только реально проверенные ноды.
    # Непроверенные старые ноды сюда НЕ добавляем.
    # --------------------------------------------------------

    update_alive_archive(
        alive_results
    )

    # --------------------------------------------------------
    # SNI stats.
    # --------------------------------------------------------

    save_sni_stats(
        SNI_STATS
    )

    # --------------------------------------------------------
    # Protocol statistics.
    # --------------------------------------------------------

    reality_count = 0
    tls_count = 0
    ws_count = 0
    grpc_count = 0
    xhttp_count = 0
    other_count = 0

    for node in alive_results:

        parsed = parse_vless_uri(
            node
        )

        if not parsed:
            other_count += 1
            continue

        params = parsed["params"]

        security = params.get(
            "security",
            ""
        ).lower()

        network = params.get(
            "type",
            "tcp"
        ).lower()

        if security == "reality":

            reality_count += 1

        elif security == "tls":

            tls_count += 1

        if network == "ws":

            ws_count += 1

        elif network == "grpc":

            grpc_count += 1

        elif network in {
            "xhttp",
            "splithttp"
        }:

            xhttp_count += 1

        elif network not in {
            "tcp",
            "ws",
            "grpc",
            "xhttp",
            "splithttp"
        }:

            other_count += 1

    print()
    print(
        "--- PROTOCOL STATS ---"
    )

    print(
        f"Reality: {reality_count}"
    )

    print(
        f"TLS:     {tls_count}"
    )

    print(
        f"WS:      {ws_count}"
    )

    print(
        f"gRPC:    {grpc_count}"
    )

    print(
        f"XHTTP:   {xhttp_count}"
    )

    print(
        f"Other:   {other_count}"
    )

    # --------------------------------------------------------
    # SNI statistics.
    # --------------------------------------------------------

    print()
    print(
        "--- TOP SNI STATS ---"
    )

    sorted_sni = sorted(
        SNI_STATS.items(),
        key=lambda item: (
            sni_success_rate(
                item[1]
            ),
            int(
                item[1].get(
                    "success",
                    0
                )
            ),
            int(
                item[1].get(
                    "attempts",
                    0
                )
            ),
        ),
        reverse=True
    )

    shown = 0

    for sni, record in sorted_sni:

        attempts = int(
            record.get(
                "attempts",
                0
            )
        )

        success = int(
            record.get(
                "success",
                0
            )
        )

        if attempts <= 0:
            continue

        rate = (
            success
            / attempts
            * 100
        )

        print(
            f"{sni}: "
            f"{success}/{attempts} "
            f"({rate:.1f}%)"
        )

        shown += 1

        if shown >= 15:
            break

    # --------------------------------------------------------
    # Final.
    # --------------------------------------------------------

    print()
    print(
        "=========================================="
    )

    print(
        f"Готово."
    )

    print(
        f"Подписка: {OUTPUT_FILE}"
    )

    print(
        f"Рабочих нод: "
        f"{len(alive_results)}/{MAX_NODES}"
    )

    print(
        f"Время: {elapsed:.1f} сек."
    )

    print(
        "=========================================="
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
