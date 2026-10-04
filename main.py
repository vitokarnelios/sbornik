import os
import re
import json
import time
import base64
import hashlib
import socket
import shutil
import subprocess
import threading
from urllib.parse import urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


# ============================================================
# CONFIG
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

SINGBOX_BIN = os.environ.get("SINGBOX_BIN", "sing-box")

MAX_NODES = 100
MAX_THREADS = 40

SOURCE_TIMEOUT = 15
SERVICE_TIMEOUT = 4
TCP_TIMEOUT = 2.0

SINGBOX_START_WAIT = 1.2

NEXT_PORT = 20000
MAX_PORT = 55000

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
# GLOBALS
# ============================================================

port_lock = threading.Lock()
next_port_value = NEXT_PORT

log_lock = threading.Lock()
stats_lock = threading.Lock()


# ============================================================
# LOGGING
# ============================================================

def log(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    line = f"[{timestamp}] {message}"

    with log_lock:
        print(line, flush=True)


def log_error(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    line = f"[{timestamp}] {message}"

    with log_lock:
        print(line, flush=True)

        try:
            with open(ERROR_LOG, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass


# ============================================================
# HELPERS
# ============================================================

def safe_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def get_free_port():
    global next_port_value

    with port_lock:
        start = next_port_value

        while True:
            port = next_port_value
            next_port_value += 1

            if next_port_value > MAX_PORT:
                next_port_value = NEXT_PORT

            if port == start:
                raise RuntimeError("No free tester ports available")

            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind(("127.0.0.1", port))
                    return port
            except OSError:
                continue


def node_id(uri):
    """
    IMPORTANT:
    SNI mutation is not used anymore.
    Therefore node identity is simply based on the original URI.
    """

    return hashlib.sha256(
        uri.strip().encode("utf-8", errors="ignore")
    ).hexdigest()[:32]


def clean_uri(uri):
    return uri.strip()


# ============================================================
# SOURCES
# ============================================================

def load_sources():
    """
    IMPORTANT:
    sources.txt is the ONLY source list.

    Add/remove source URLs there.
    Nothing is hardcoded in this script.
    """

    if not os.path.exists(SOURCES_FILE):
        raise FileNotFoundError(
            f"Sources file not found: {SOURCES_FILE}"
        )

    sources = []

    with open(SOURCES_FILE, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            if not line.lower().startswith(("http://", "https://")):
                continue

            if line not in sources:
                sources.append(line)

    return sources


def download_source(url):
    try:
        response = requests.get(
            url,
            timeout=SOURCE_TIMEOUT,
            headers={
                "User-Agent": "Mozilla/5.0 NodeTester/1.0"
            },
        )

        response.raise_for_status()

        return True, response.text, None

    except Exception as e:
        return False, "", str(e)


# ============================================================
# VLESS EXTRACTION
# ============================================================

def extract_vless(text):
    """
    Extract VLESS URIs from plain text.

    Also tries base64 if the source itself is encoded.
    """

    found = []

    if not text:
        return found

    # Normal VLESS URIs
    for match in re.findall(
        r"vless://[^\s<>'\"`]+",
        text,
        flags=re.IGNORECASE,
    ):
        uri = match.strip().rstrip(",;")

        if uri:
            found.append(uri)

    # Base64 fallback
    compact = re.sub(r"\s+", "", text)

    if len(compact) >= 16:
        try:
            padding = "=" * (-len(compact) % 4)

            decoded = base64.b64decode(
                compact + padding,
                validate=False,
            ).decode(
                "utf-8",
                errors="ignore",
            )

            for match in re.findall(
                r"vless://[^\s<>'\"`]+",
                decoded,
                flags=re.IGNORECASE,
            ):
                uri = match.strip().rstrip(",;")

                if uri:
                    found.append(uri)

        except Exception:
            pass

    # De-duplicate while preserving order
    result = []
    seen = set()

    for uri in found:
        uri = clean_uri(uri)

        if uri not in seen:
            seen.add(uri)
            result.append(uri)

    return result


# ============================================================
# VLESS PARSER
# ============================================================

def parse_vless(uri):
    try:
        parsed = urlparse(uri)

        if parsed.scheme.lower() != "vless":
            return None

        if not parsed.hostname:
            return None

        if not parsed.port:
            return None

        if not parsed.username:
            return None

        uuid = unquote(parsed.username)
        server = parsed.hostname
        port = parsed.port

        raw_params = parse_qs(
            parsed.query,
            keep_blank_values=True,
        )

        params = {}

        for key, values in raw_params.items():
            key_lower = key.lower()

            if values:
                params[key_lower] = unquote(values[-1])
            else:
                params[key_lower] = ""

        transport = params.get("type", "tcp").lower()

        security = params.get(
            "security",
            "none",
        ).lower()

        flow = params.get("flow", "")

        sni = params.get(
            "sni",
            server,
        )

        fp = params.get(
            "fp",
            "",
        )

        alpn_value = params.get(
            "alpn",
            "",
        )

        alpn = []

        if alpn_value:
            alpn = [
                x.strip()
                for x in alpn_value.split(",")
                if x.strip()
            ]

        # ----------------------------------------------------
        # OUTBOUND
        # ----------------------------------------------------

        outbound = {
            "type": "vless",

            # CRITICAL:
            # route.final points to this tag.
            "tag": "proxy",

            "server": server,
            "server_port": port,
            "uuid": uuid,
        }

        if flow:
            outbound["flow"] = flow

        # ----------------------------------------------------
        # TLS / REALITY
        # ----------------------------------------------------

        if security in ("tls", "reality"):

            tls = {
                "enabled": True,
                "server_name": sni,
            }

            if fp:
                tls["utls"] = {
                    "enabled": True,
                    "fingerprint": fp,
                }

            if alpn:
                tls["alpn"] = alpn

            if security == "reality":

                public_key = params.get(
                    "pbk",
                    "",
                )

                if not public_key:
                    public_key = params.get(
                        "publickey",
                        "",
                    )

                short_id = params.get(
                    "sid",
                    "",
                )

                if not short_id:
                    short_id = params.get(
                        "shortid",
                        "",
                    )

                if public_key:
                    tls["reality"] = {
                        "enabled": True,
                        "public_key": public_key,
                        "short_id": short_id,
                    }

            outbound["tls"] = tls

        elif security == "none":
            pass

        else:
            return None

        # ----------------------------------------------------
        # TRANSPORT
        # ----------------------------------------------------

        if transport in ("tcp", "raw"):
            pass

        elif transport == "ws":

            ws_path = params.get(
                "path",
                "/",
            )

            ws_headers = {}

            host = params.get(
                "host",
                "",
            )

            if host:
                ws_headers["Host"] = host

            transport_config = {
                "type": "ws",
                "path": ws_path,
            }

            if ws_headers:
                transport_config["headers"] = ws_headers

            outbound["transport"] = transport_config

        elif transport == "grpc":

            service_name = (
                params.get("servicename")
                or params.get("serviceName")
                or params.get("service_name")
                or ""
            )

            grpc_config = {
                "type": "grpc",
                "service_name": service_name,
            }

            mode = params.get(
                "mode",
                "",
            ).lower()

            if mode == "multi":
                grpc_config["multi_mode"] = True

            outbound["transport"] = grpc_config

        elif transport == "httpupgrade":

            path = params.get(
                "path",
                "/",
            )

            host = params.get(
                "host",
                "",
            )

            httpupgrade = {
                "type": "httpupgrade",
                "path": path,
            }

            if host:
                httpupgrade["host"] = host

            outbound["transport"] = httpupgrade

        elif transport == "xhttp":

            path = params.get(
                "path",
                "/",
            )

            host = params.get(
                "host",
                "",
            )

            xhttp = {
                "type": "httpupgrade",
                "path": path,
            }

            if host:
                xhttp["host"] = host

            outbound["transport"] = xhttp

        else:
            return None

        return {
            "uri": uri,
            "server": server,
            "port": port,
            "uuid": uuid,
            "transport": transport,
            "security": security,
            "sni": sni,
            "outbound": outbound,
        }

    except Exception:
        return None


# ============================================================
# SING-BOX CONFIG
# ============================================================

def build_config(node, local_port):
    """
    Creates a minimal sing-box configuration.

    IMPORTANT FIX:
    VLESS outbound has tag "proxy".
    route.final points to "proxy".

    This prevents the previous situation where route.final
    referenced an outbound tag that did not exist.
    """

    return {
        "log": {
            "level": "error",
        },

        "inbounds": [
            {
                "type": "mixed",
                "tag": "socks",
                "listen": "127.0.0.1",
                "listen_port": local_port,
            }
        ],

        "outbounds": [
            node["outbound"],

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


# ============================================================
# TCP PRECHECK
# ============================================================

def tcp_precheck(server, port):
    try:
        with socket.create_connection(
            (server, port),
            timeout=TCP_TIMEOUT,
        ):
            return True, None

    except Exception as e:
        return False, str(e)


# ============================================================
# SING-BOX PROCESS
# ============================================================

def write_json(path, data):
    with open(
        path,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2,
        )


def run_singbox_check(config_path):
    try:
        result = subprocess.run(
            [
                SINGBOX_BIN,
                "check",
                "-c",
                config_path,
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

        if result.returncode != 0:

            error_text = (
                result.stderr.strip()
                or result.stdout.strip()
                or f"exit code {result.returncode}"
            )

            return False, error_text

        return True, None

    except subprocess.TimeoutExpired:
        return False, "sing-box check timeout"

    except Exception as e:
        return False, str(e)


def start_singbox(config_path):
    try:

        process = subprocess.Popen(
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

        return process, None

    except Exception as e:
        return None, str(e)


# ============================================================
# SERVICE TEST
# ============================================================

def test_service(url, proxy_port):
    proxy = f"socks5h://127.0.0.1:{proxy_port}"

    proxies = {
        "http": proxy,
        "https": proxy,
    }

    started = time.time()

    try:
        response = requests.get(
            url,
            proxies=proxies,
            timeout=SERVICE_TIMEOUT,
            allow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 "
                    "(Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 "
                    "Chrome/154.0 Safari/537.36"
                )
            },
        )

        elapsed = time.time() - started

        ok = response.status_code in GOOD_CODES

        return {
            "url": url,
            "ok": ok,
            "status": response.status_code,
            "time": round(elapsed, 3),
            "error": None,
        }

    except Exception as e:

        elapsed = time.time() - started

        return {
            "url": url,
            "ok": False,
            "status": None,
            "time": round(elapsed, 3),
            "error": str(e),
        }


def test_services(proxy_port):
    results = []

    for url in TEST_URLS:
        result = test_service(
            url,
            proxy_port,
        )

        results.append(result)

    return results


# ============================================================
# SINGLE NODE TEST
# ============================================================

def check_single_uri(uri):
    node = parse_vless(uri)

    if not node:
        return {
            "uri": uri,
            "node_id": node_id(uri),
            "live": False,
            "tcp_ok": False,
            "success_count": 0,
            "services": [],
            "error": "invalid VLESS URI",
        }

    # --------------------------------------------------------
    # TCP PRECHECK
    # --------------------------------------------------------

    tcp_ok, tcp_error = tcp_precheck(
        node["server"],
        node["port"],
    )

    if not tcp_ok:

        return {
            "uri": uri,
            "node_id": node_id(uri),
            "server": node["server"],
            "port": node["port"],
            "transport": node["transport"],
            "security": node["security"],
            "sni": node["sni"],
            "live": False,
            "tcp_ok": False,
            "success_count": 0,
            "services": [],
            "error": f"TCP FAIL: {tcp_error}",
        }

    local_port = get_free_port()

    config_path = os.path.join(
        TEMP_DIR,
        f"node_{local_port}.json",
    )

    process = None

    try:

        config = build_config(
            node,
            local_port,
        )

        write_json(
            config_path,
            config,
        )

        # ----------------------------------------------------
        # SING-BOX CONFIG CHECK
        # ----------------------------------------------------

        check_ok, check_error = run_singbox_check(
            config_path,
        )

        if not check_ok:

            log_error(
                f"CONFIG FAIL | "
                f"{node['server']}:{node['port']} | "
                f"{node['transport']}/{node['security']} | "
                f"{check_error}"
            )

            return {
                "uri": uri,
                "node_id": node_id(uri),
                "server": node["server"],
                "port": node["port"],
                "transport": node["transport"],
                "security": node["security"],
                "sni": node["sni"],
                "live": False,
                "tcp_ok": True,
                "success_count": 0,
                "services": [],
                "error": f"CONFIG FAIL: {check_error}",
            }

        # ----------------------------------------------------
        # START SING-BOX
        # ----------------------------------------------------

        process, start_error = start_singbox(
            config_path,
        )

        if process is None:

            log_error(
                f"START FAIL | "
                f"{node['server']}:{node['port']} | "
                f"{start_error}"
            )

            return {
                "uri": uri,
                "node_id": node_id(uri),
                "server": node["server"],
                "port": node["port"],
                "transport": node["transport"],
                "security": node["security"],
                "sni": node["sni"],
                "live": False,
                "tcp_ok": True,
                "success_count": 0,
                "services": [],
                "error": f"START FAIL: {start_error}",
            }

        # ----------------------------------------------------
        # WAIT FOR LOCAL SOCKS
        # ----------------------------------------------------

        time.sleep(SINGBOX_START_WAIT)

        # ----------------------------------------------------
        # TEST SERVICES
        # ----------------------------------------------------

        services = test_services(
            local_port,
        )

        success_count = sum(
            1
            for item in services
            if item["ok"]
        )

        live = success_count >= 3

        return {
            "uri": uri,
            "node_id": node_id(uri),
            "server": node["server"],
            "port": node["port"],
            "transport": node["transport"],
            "security": node["security"],
            "sni": node["sni"],
            "live": live,
            "tcp_ok": True,
            "success_count": success_count,
            "services": services,
            "error": None,
        }

    except Exception as e:

        log_error(
            f"NODE ERROR | "
            f"{node['server']}:{node['port']} | "
            f"{repr(e)}"
        )

        return {
            "uri": uri,
            "node_id": node_id(uri),
            "server": node["server"],
            "port": node["port"],
            "transport": node["transport"],
            "security": node["security"],
            "sni": node["sni"],
            "live": False,
            "tcp_ok": True,
            "success_count": 0,
            "services": [],
            "error": repr(e),
        }

    finally:

        if process is not None:

            try:
                process.terminate()

                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)

            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass

        try:
            if os.path.exists(config_path):
                os.remove(config_path)
        except Exception:
            pass


# ============================================================
# ARCHIVE
# ============================================================

def load_archive():
    if not os.path.exists(ARCHIVE_FILE):
        return {}

    archive = {}

    try:
        with open(
            ARCHIVE_FILE,
            "r",
            encoding="utf-8",
            errors="ignore",
        ) as f:

            for line in f:

                uri = line.strip()

                if not uri:
                    continue

                if not uri.lower().startswith("vless://"):
                    continue

                archive[node_id(uri)] = uri

    except Exception as e:
        log_error(
            f"Archive read error: {e}"
        )

    return archive


def save_archive(archive):
    temp_file = ARCHIVE_FILE + ".tmp"

    values = list(archive.values())

    values.sort()

    with open(
        temp_file,
        "w",
        encoding="utf-8",
    ) as f:

        for uri in values:
            f.write(uri + "\n")

    os.replace(
        temp_file,
        ARCHIVE_FILE,
    )


# ============================================================
# STATS
# ============================================================

def load_stats():
    if not os.path.exists(STATS_FILE):
        return {
            "runs": 0,
            "nodes": {},
            "sources": {},
            "services": {},
        }

    try:

        with open(
            STATS_FILE,
            "r",
            encoding="utf-8",
        ) as f:

            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("stats root is not object")

        data.setdefault("runs", 0)
        data.setdefault("nodes", {})
        data.setdefault("sources", {})
        data.setdefault("services", {})

        return data

    except Exception as e:

        log_error(
            f"Stats read error: {e}"
        )

        return {
            "runs": 0,
            "nodes": {},
            "sources": {},
            "services": {},
        }


def save_stats(stats):
    temp_file = STATS_FILE + ".tmp"

    with open(
        temp_file,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            stats,
            f,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(
        temp_file,
        STATS_FILE,
    )


def ensure_source_bucket(stats, source):
    if source not in stats["sources"]:

        stats["sources"][source] = {
            "runs": 0,
            "downloads_ok": 0,
            "downloads_failed": 0,
            "found": 0,
            "tested": 0,
            "live": 0,
            "dead": 0,
        }

    bucket = stats["sources"][source]

    bucket.setdefault("runs", 0)
    bucket.setdefault("downloads_ok", 0)
    bucket.setdefault("downloads_failed", 0)
    bucket.setdefault("found", 0)
    bucket.setdefault("tested", 0)
    bucket.setdefault("live", 0)
    bucket.setdefault("dead", 0)

    return bucket


def update_stats_node(stats, result):
    nid = result["node_id"]

    if nid not in stats["nodes"]:

        stats["nodes"][nid] = {
            "uri": result["uri"],
            "tests": 0,
            "live": 0,
            "dead": 0,
            "tcp_fail": 0,
            "services": {
                url: {
                    "ok": 0,
                    "fail": 0,
                }
                for url in TEST_URLS
            },
        }

    bucket = stats["nodes"][nid]

    bucket["uri"] = result["uri"]
    bucket["tests"] += 1

    if result["live"]:
        bucket["live"] += 1
    else:
        bucket["dead"] += 1

    if not result["tcp_ok"]:
        bucket["tcp_fail"] += 1

    for service in result.get("services", []):

        url = service["url"]

        if url not in bucket["services"]:
            bucket["services"][url] = {
                "ok": 0,
                "fail": 0,
            }

        if service["ok"]:
            bucket["services"][url]["ok"] += 1
        else:
            bucket["services"][url]["fail"] += 1


def update_stats_services(stats, result):

    for service in result.get("services", []):

        url = service["url"]

        if url not in stats["services"]:

            stats["services"][url] = {
                "ok": 0,
                "fail": 0,
            }

        if service["ok"]:
            stats["services"][url]["ok"] += 1
        else:
            stats["services"][url]["fail"] += 1


# ============================================================
# NODE TEST WRAPPER
# ============================================================

def check_node(uri):
    result = check_single_uri(uri)

    status = "LIVE" if result["live"] else "DEAD"

    service_count = result.get(
        "success_count",
        0,
    )

    if result.get("error"):
        detail = f" | {result['error']}"
    else:
        detail = ""

    log(
        f"{status} | "
        f"{result.get('server', '?')}:"
        f"{result.get('port', '?')} | "
        f"{result.get('transport', '?')}/"
        f"{result.get('security', '?')} | "
        f"{service_count}/{len(TEST_URLS)}"
        f"{detail}"
    )

    return result


# ============================================================
# SOURCE MEMBERSHIP
# ============================================================

def build_source_membership(source_nodes):
    """
    Returns:
        node_id -> set(source URLs)
    """

    membership = {}

    for source, nodes in source_nodes.items():

        for uri in nodes:

            nid = node_id(uri)

            if nid not in membership:
                membership[nid] = set()

            membership[nid].add(source)

    return membership


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    log("=" * 70)
    log("=== VLESS BULK TESTER / SING-BOX ===")
    log("=== FULL TEST / NO EARLY EXIT ===")
    log("=" * 70)

    log(
        f"Services: {len(TEST_URLS)}"
    )

    log(
        "Services: "
        + ", ".join(TEST_URLS)
    )

    log(
        f"Live threshold: 3/{len(TEST_URLS)}"
    )

    log(
        f"Max subscription nodes: {MAX_NODES}"
    )

    log(
        f"Threads: {MAX_THREADS}"
    )

    log(
        f"Archive: {ARCHIVE_FILE}"
    )

    log(
        f"Subscription: {SUBSCRIPTION_FILE}"
    )

    # --------------------------------------------------------
    # SOURCES
    # --------------------------------------------------------

    try:
        sources = load_sources()
    except Exception as e:
        log_error(
            f"Cannot load sources.txt: {e}"
        )
        return

    log(
        f"Sources: {len(sources)}"
    )

    if not sources:
        log_error(
            "sources.txt contains no valid URLs"
        )
        return

    # --------------------------------------------------------
    # STATS
    # --------------------------------------------------------

    stats = load_stats()

    stats["runs"] += 1

    run_number = stats["runs"]

    log(
        f"Run number: {run_number}"
    )

    # --------------------------------------------------------
    # DOWNLOAD SOURCES
    # --------------------------------------------------------

    source_nodes = {}

    all_unique_nodes = {}

    source_results = {}

    total_found = 0

    successful_sources = 0
    failed_sources = 0

    for index, source in enumerate(
        sources,
        start=1,
    ):

        log(
            f"[SOURCE {index}/{len(sources)}] "
            f"Downloading: {source}"
        )

        ok, text, error = download_source(
            source
        )

        bucket = ensure_source_bucket(
            stats,
            source,
        )

        bucket["runs"] += 1

        if not ok:

            failed_sources += 1

            bucket["downloads_failed"] += 1

            source_results[source] = {
                "download_ok": False,
                "nodes": [],
                "error": error,
            }

            log(
                f"[SOURCE {index}] "
                f"FAILED: {error}"
            )

            continue

        successful_sources += 1

        bucket["downloads_ok"] += 1

        nodes = extract_vless(text)

        bucket["found"] += len(nodes)

        total_found += len(nodes)

        source_nodes[source] = nodes

        source_results[source] = {
            "download_ok": True,
            "nodes": nodes,
            "error": None,
        }

        for uri in nodes:

            nid = node_id(uri)

            if nid not in all_unique_nodes:
                all_unique_nodes[nid] = uri

        log(
            f"[SOURCE {index}] "
            f"OK | VLESS found: {len(nodes)}"
        )

    log("-" * 70)

    log(
        f"Sources successful: "
        f"{successful_sources}/{len(sources)}"
    )

    log(
        f"Sources failed: {failed_sources}"
    )

    log(
        f"VLESS found total: {total_found}"
    )

    log(
        f"Unique nodes: {len(all_unique_nodes)}"
    )

    # --------------------------------------------------------
    # SOURCE MEMBERSHIP
    # --------------------------------------------------------

    source_membership = build_source_membership(
        source_nodes
    )

    # --------------------------------------------------------
    # TEST ALL UNIQUE FRESH NODES
    # --------------------------------------------------------

    fresh_results = {}

    fresh_live = {}
    fresh_dead = {}

    log("-" * 70)

    log(
        f"START FRESH TEST: "
        f"{len(all_unique_nodes)} unique nodes"
    )

    with ThreadPoolExecutor(
        max_workers=MAX_THREADS
    ) as executor:

        future_map = {
            executor.submit(
                check_node,
                uri,
            ): nid
            for nid, uri in all_unique_nodes.items()
        }

        completed = 0

        for future in as_completed(
            future_map
        ):

            nid = future_map[future]

            try:
                result = future.result()

            except Exception as e:

                uri = all_unique_nodes[nid]

                result = {
                    "uri": uri,
                    "node_id": nid,
                    "live": False,
                    "tcp_ok": False,
                    "success_count": 0,
                    "services": [],
                    "error": repr(e),
                }

                log_error(
                    f"THREAD ERROR | {uri} | {repr(e)}"
                )

            fresh_results[nid] = result

            if result["live"]:
                fresh_live[nid] = result
            else:
                fresh_dead[nid] = result

            completed += 1

            if completed % 10 == 0 or completed == len(all_unique_nodes):

                log(
                    f"PROGRESS: "
                    f"{completed}/{len(all_unique_nodes)} | "
                    f"LIVE={len(fresh_live)} | "
                    f"DEAD={len(fresh_dead)}"
                )

    log("-" * 70)

    log(
        f"Fresh LIVE: {len(fresh_live)}"
    )

    log(
        f"Fresh DEAD: {len(fresh_dead)}"
    )

    # --------------------------------------------------------
    # UPDATE SOURCE TEST STATS
    # --------------------------------------------------------

    for source, nodes in source_nodes.items():

        bucket = ensure_source_bucket(
            stats,
            source,
        )

        bucket["tested"] += len(nodes)

        for uri in nodes:

            nid = node_id(uri)

            result = fresh_results.get(nid)

            if not result:
                continue

            if result["live"]:
                bucket["live"] += 1
            else:
                bucket["dead"] += 1

    # --------------------------------------------------------
    # UPDATE NODE / SERVICE STATS
    # --------------------------------------------------------

    for result in fresh_results.values():

        update_stats_node(
            stats,
            result,
        )

        update_stats_services(
            stats,
            result,
        )

    # --------------------------------------------------------
    # ARCHIVE
    # --------------------------------------------------------

    archive = load_archive()

    archive_before = len(archive)

    log("-" * 70)

    log(
        f"Archive before: {archive_before}"
    )

    # --------------------------------------------------------
    # ADD FRESH LIVE TO ARCHIVE
    # --------------------------------------------------------

    for nid, result in fresh_live.items():

        archive[nid] = result["uri"]

    # --------------------------------------------------------
    # REMOVE FRESH DEAD FROM ARCHIVE
    #
    # Only remove nodes that were actually tested in this run.
    # Other historical LIVE archive nodes remain untouched.
    # --------------------------------------------------------

    for nid in fresh_dead:

        if nid in archive:
            del archive[nid]

    log(
        f"Archive after fresh results: "
        f"{len(archive)}"
    )

    # --------------------------------------------------------
    # BUILD SUBSCRIPTION CANDIDATES
    #
    # Fresh LIVE first.
    # --------------------------------------------------------

    subscription = []

    used_ids = set()

    for nid, result in fresh_live.items():

        if len(subscription) >= MAX_NODES:
            break

        subscription.append(
            result["uri"]
        )

        used_ids.add(nid)

    # --------------------------------------------------------
    # ARCHIVE FILL
    #
    # If fresh LIVE < 100, test archive candidates.
    # Fresh nodes are not tested again.
    # --------------------------------------------------------

    archive_candidates = [
        (nid, uri)
        for nid, uri in archive.items()
        if nid not in fresh_results
    ]

    need_archive = (
        MAX_NODES - len(subscription)
    )

    archive_results = {}

    if need_archive > 0 and archive_candidates:

        log("-" * 70)

        log(
            f"Archive fill needed: "
            f"{need_archive}"
        )

        log(
            f"Archive candidates to test: "
            f"{len(archive_candidates)}"
        )

        with ThreadPoolExecutor(
            max_workers=MAX_THREADS
        ) as executor:

            future_map = {
                executor.submit(
                    check_node,
                    uri,
                ): (nid, uri)
                for nid, uri in archive_candidates
            }

            for future in as_completed(
                future_map
            ):

                nid, uri = future_map[future]

                try:
                    result = future.result()

                except Exception as e:

                    result = {
                        "uri": uri,
                        "node_id": nid,
                        "live": False,
                        "tcp_ok": False,
                        "success_count": 0,
                        "services": [],
                        "error": repr(e),
                    }

                    log_error(
                        f"ARCHIVE THREAD ERROR | "
                        f"{uri} | {repr(e)}"
                    )

                archive_results[nid] = result

                update_stats_node(
                    stats,
                    result,
                )

                update_stats_services(
                    stats,
                    result,
                )

        # ----------------------------------------------------
        # ARCHIVE RESULTS
        # ----------------------------------------------------

        for nid, result in archive_results.items():

            if result["live"]:

                archive[nid] = result["uri"]

            else:

                if nid in archive:
                    del archive[nid]

        # ----------------------------------------------------
        # FILL SUBSCRIPTION FROM LIVE ARCHIVE RESULTS
        # ----------------------------------------------------

        for nid, result in archive_results.items():

            if len(subscription) >= MAX_NODES:
                break

            if not result["live"]:
                continue

            if nid in used_ids:
                continue

            subscription.append(
                result["uri"]
            )

            used_ids.add(nid)

        log(
            f"Archive fill LIVE: "
            f"{sum(1 for x in archive_results.values() if x['live'])}"
        )

        log(
            f"Archive fill DEAD: "
            f"{sum(1 for x in archive_results.values() if not x['live'])}"
        )

    else:

        if need_archive > 0:
            log(
                "Archive fill skipped: "
                "no archive candidates"
            )

    # --------------------------------------------------------
    # IF STILL NOT FULL, USE EXISTING ARCHIVE
    #
    # This is only for archive entries that were not selected
    # as candidates in the current run.
    #
    # But we do NOT blindly trust them if they were tested
    # and failed.
    # --------------------------------------------------------

    if len(subscription) < MAX_NODES:

        for nid, uri in archive.items():

            if len(subscription) >= MAX_NODES:
                break

            if nid in used_ids:
                continue

            if nid in fresh_results:
                continue

            if nid in archive_results:
                continue

            subscription.append(uri)

            used_ids.add(nid)

    # --------------------------------------------------------
    # FINAL ARCHIVE SAVE
    # --------------------------------------------------------

    save_archive(archive)

    log(
        f"Archive final: {len(archive)}"
    )

    # --------------------------------------------------------
    # SUBSCRIPTION SAFETY
    # --------------------------------------------------------

    previous_subscription_exists = os.path.exists(
        SUBSCRIPTION_FILE
    )

    previous_subscription_size = 0

    if previous_subscription_exists:

        try:
            previous_subscription_size = os.path.getsize(
                SUBSCRIPTION_FILE
            )
        except Exception:
            previous_subscription_size = 0

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # NEVER replace a working subscription with an empty one.
    #
    # If this run gets 0 validated LIVE nodes, keep the previous
    # subscription intact.
    # --------------------------------------------------------

    if subscription:

        temp_subscription = (
            SUBSCRIPTION_FILE + ".tmp"
        )

        with open(
            temp_subscription,
            "w",
            encoding="utf-8",
        ) as f:

            for uri in subscription:
                f.write(uri + "\n")

        os.replace(
            temp_subscription,
            SUBSCRIPTION_FILE,
        )

        log(
            f"Subscription updated: "
            f"{len(subscription)} nodes"
        )

    else:

        if previous_subscription_exists:

            log(
                "Subscription update SKIPPED: "
                "0 validated LIVE nodes. "
                "Existing vless_001.txt preserved."
            )

            log(
                f"Existing subscription size: "
                f"{previous_subscription_size} bytes"
            )

        else:

            # No previous subscription exists.
            # Create an empty file only in this case.
            with open(
                SUBSCRIPTION_FILE,
                "w",
                encoding="utf-8",
            ):
                pass

            log(
                "Subscription remains empty: "
                "no validated LIVE nodes."
            )

    # --------------------------------------------------------
    # RUN STATS
    # --------------------------------------------------------

    stats["last_run"] = {
        "run": run_number,
        "timestamp": time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "sources": len(sources),
        "sources_ok": successful_sources,
        "sources_failed": failed_sources,
        "vless_found": total_found,
        "unique_nodes": len(all_unique_nodes),
        "fresh_live": len(fresh_live),
        "fresh_dead": len(fresh_dead),
        "archive_before": archive_before,
        "archive_after": len(archive),
        "subscription_nodes": len(subscription),
        "subscription_updated": bool(subscription),
        "services": len(TEST_URLS),
        "live_threshold": 3,
        "threads": MAX_THREADS,
        "duration_seconds": round(
            time.time() - start_time,
            2,
        ),
    }

    # --------------------------------------------------------
    # SAVE STATS
    # --------------------------------------------------------

    save_stats(stats)

    # --------------------------------------------------------
    # FINAL REPORT
    # --------------------------------------------------------

    elapsed = time.time() - start_time

    log("=" * 70)
    log("=== RUN FINISHED ===")
    log("=" * 70)

    log(
        f"Run: {run_number}"
    )

    log(
        f"Sources: "
        f"{successful_sources}/{len(sources)} OK"
    )

    log(
        f"VLESS found: "
        f"{total_found}"
    )

    log(
        f"Unique nodes: "
        f"{len(all_unique_nodes)}"
    )

    log(
        f"Fresh LIVE: "
        f"{len(fresh_live)}"
    )

    log(
        f"Fresh DEAD: "
        f"{len(fresh_dead)}"
    )

    log(
        f"Archive: "
        f"{archive_before} -> {len(archive)}"
    )

    log(
        f"Subscription candidates: "
        f"{len(subscription)}"
    )

    log(
        f"Subscription file: "
        f"{SUBSCRIPTION_FILE}"
    )

    log(
        f"Duration: "
        f"{elapsed:.1f} sec"
    )

    log("=" * 70)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
