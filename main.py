#!/usr/bin/env python3
"""
SNI MUTATOR + STATS + LOGS
- Проверяет ноды с оригинальным SNI
- Если нода мертва — мутирует SNI из рейтинга
- Ведёт статистику успешных SNI (только локально)
- TOP15 (shuffled) + RANDOM5 (shuffled)
- Сохраняет статистику один раз в конце
- Без старения
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
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote, urlencode, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
LOG_DIR = os.path.join(BASE_PATH, "logs")

os.makedirs(FINAL_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ========== ЛОГИРОВАНИЕ ==========
log_file = os.path.join(LOG_DIR, f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

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

if not os.path.exists(SOURCES_FILE):
    logger.error("Файл sources.txt не найден")
    exit(1)

with open(SOURCES_FILE, "r", encoding="utf-8") as f:
    SOURCES = [
        line.strip()
        for line in f
        if line.strip() and not line.strip().startswith("#")
    ]

# ========== СТАТИСТИКА SNI ==========
DEFAULT_SNI = {
    "web.max.ru": 0,
    "vk.com": 0,
    "rutube.ru": 0,
    "mail.ru": 0,
    "ok.ru": 0,
    "yandex.ru": 0,
    "dzen.ru": 0,
    "gosuslugi.ru": 0,
    "ozon.ru": 0,
    "wildberries.ru": 0,
    "kinopoisk.ru": 0,
    "yandex.by": 0,
    "yandex.kz": 0,
    "telegram.org": 0,
    "cdn.x5.ru": 0,
    "storage.yandex.net": 0,
    "api-maps.yandex.ru": 0,
    "avatars.mds.yandex.net": 0,
    "sberbank.ru": 0,
    "tbank.ru": 0,
    "avito.ru": 0,
    "hh.ru": 0,
    "rambler.ru": 0,
    "lenta.ru": 0,
    "ria.ru": 0,
    "tass.ru": 0
}

def load_sni_stats():
    """Загружает статистику SNI из файла"""
    if os.path.exists(SNI_STATS_FILE):
        try:
            with open(SNI_STATS_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                logger.info(f"📊 Загружена статистика SNI: {len(loaded)} доменов")
                return loaded
        except Exception as e:
            logger.warning(f"Ошибка загрузки статистики: {e}")
    logger.info("📊 Использую дефолтный список SNI")
    return DEFAULT_SNI.copy()

def save_sni_stats(stats):
    """Сохраняет статистику SNI в файл"""
    try:
        with open(SNI_STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        logger.info("💾 Статистика SNI сохранена")
    except Exception as e:
        logger.warning(f"Ошибка сохранения статистики: {e}")

def get_sni_list(stats, top_count=15, random_count=5):
    """
    Возвращает список SNI для проверки:
    - TOP15 (перемешанные)
    - RANDOM5 из оставшихся (перемешанные)
    """
    sorted_sni = sorted(stats.items(), key=lambda x: x[1], reverse=True)
    
    # ТОП-15
    top = [sni for sni, _ in sorted_sni[:top_count]]
    random.shuffle(top)  # перемешиваем только топ
    
    # Остальные
    remaining = [sni for sni, _ in sorted_sni[top_count:]]
    
    # Если в топе меньше top_count — добираем из остальных
    if len(top) < top_count:
        needed = top_count - len(top)
        if remaining:
            extra = random.sample(remaining, min(needed, len(remaining)))
            top.extend(extra)
    
    # Случайные 5 из оставшихся
    zero_remaining = [sni for sni in remaining if sni not in top]
    if zero_remaining and random_count > 0:
        random_ones = random.sample(zero_remaining, min(random_count, len(zero_remaining)))
        random.shuffle(random_ones)  # перемешиваем случайные
    else:
        random_ones = []
    
    result = top + random_ones
    logger.debug(f"🔍 Выбрано SNI: {result}")
    return result

# ========== ЗАГРУЗКА СТАТИСТИКИ ==========
SNI_STATS = load_sni_stats()

MAX_NODES = 100       
MAX_THREADS = 40      
TOP_SNI_COUNT = 15
RANDOM_SNI_COUNT = 5
SNI_SUCCESS_WEIGHT = 1  # +1 за успешную мутацию

stop_event = threading.Event()

port_queue = queue.Queue()
for i in range(MAX_THREADS):
    port_queue.put(11000 + i)

TEST_URLS = [
    "https://vk.com",
    "https://yandex.ru",
    "https://rutube.ru",
    "https://ok.ru",
    "https://www.microsoft.com",
]

def decode_base64_content(text):
    try:
        text = text.strip()
        if "://" in text:
            return [text]
        padding = len(text) % 4
        if padding:
            text += "=" * (4 - padding)
        decoded = base64.b64decode(text).decode("utf-8", errors="ignore")
        return decoded.splitlines()
    except:
        return []

def fetch_source(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        r = requests.get(url, timeout=15, headers=headers)
        if r.status_code == 200:
            final_lines = []
            for line in r.text.splitlines():
                line = line.strip()
                if not line:
                    continue
                if "://" not in line and len(line) > 50:
                    final_lines.extend(decode_base64_content(line))
                else:
                    final_lines.append(line)
            return final_lines
    except Exception as e:
        logger.debug(f"Ошибка загрузки {url}: {e}")
    return []

def is_valid_vless(line):
    return line.lower().startswith("vless://")

def mutate_node_sni(vless_uri, target_sni):
    """Генерирует новую строку vless:// с подмененным SNI"""
    try:
        parsed = urlparse(vless_uri)
        query_params = parse_qs(parsed.query)
        
        mutated_params = {k: v[:] for k, v in query_params.items()}
        mutated_params['sni'] = [target_sni]
        
        new_query = urlencode(mutated_params, doseq=True)
        
        orig_fragment = unquote(parsed.fragment) if parsed.fragment else "node"
        new_fragment = f"{orig_fragment}-fixed-{target_sni}"
        
        return urlunparse((
            parsed.scheme, parsed.netloc, parsed.path,
            parsed.params, new_query, new_fragment
        ))
    except:
        return vless_uri

def parse_vless_to_json(vless_uri, listen_port):
    try:
        parsed = urlparse(vless_uri)
        netloc = parsed.netloc
        if '@' not in netloc:
            return None
        
        uuid, server_part = netloc.split('@', 1)
        if ':' in server_part:
            server_address, server_port_str = server_part.split(':', 1)
            server_port = int(server_port_str)
        else:
            server_address = server_part
            server_port = 443
        
        query_params = parse_qs(parsed.query)
        params = {k.lower(): v[0] for k, v in query_params.items() if v}
        
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
        
        if "pbk" in params or params.get("security") == "reality" or security == "reality":
            outbound["tls"] = {
                "enabled": True,
                "server_name": params.get("sni", server_address),
                "utls": {
                    "enabled": True,
                    "fingerprint": params.get("fp", "chrome")
                },
                "reality": {
                    "enabled": True,
                    "public_key": params.get("pbk", ""),
                    "short_id": params.get("sid", "")
                }
            }
        
        elif security == "tls" or params.get("security") == "tls":
            outbound["tls"] = {
                "enabled": True,
                "server_name": params.get("sni", server_address),
                "utls": {
                    "enabled": True,
                    "fingerprint": params.get("fp", "chrome")
                }
            }
            if "alpn" in params:
                outbound["tls"]["alpn"] = params["alpn"].split(',')
        
        if transport_type == "grpc":
            outbound["transport"] = {
                "type": "grpc",
                "service_name": params.get("serviceName", "")
            }
        
        elif transport_type == "xhttp":
            outbound["transport"] = {
                "type": "xhttp",
                "path": params.get("path", "/"),
                "host": [params["host"]] if params.get("host") else [],
                "mode": params.get("mode", "auto")
            }
        
        elif transport_type == "ws":
            outbound["transport"] = {
                "type": "ws",
                "path": params.get("path", "/"),
                "headers": {
                    "Host": params.get("host", server_address)
                }
            }
        
        config = {
            "log": {"level": "error"},
            "inbounds": [
                {
                    "type": "socks",
                    "tag": "socks-in",
                    "listen": "127.0.0.1",
                    "listen_port": listen_port
                }
            ],
            "outbounds": [outbound]
        }
        return config
    except Exception as e:
        return None

def check_single_uri(vless_uri, local_port):
    """Проверяет одну ссылку через sing-box"""
    temp_config_path = os.path.join(BASE_PATH, f"temp_{local_port}.json")
    temp_log_path = os.path.join(BASE_PATH, f"singbox_{local_port}.log")
    
    config = parse_vless_to_json(vless_uri, local_port)
    if not config:
        return False

    with open(temp_config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)

    proc = None
    try:
        log_file = open(temp_log_path, "w", encoding="utf-8")
        proc = subprocess.Popen(
            ["sing-box", "run", "-c", temp_config_path],
            stdout=log_file,
            stderr=log_file
        )
        
        time.sleep(2.5)
        
        if proc.poll() is not None:
            return False

        proxies = {
            "http": f"socks5h://127.0.0.1:{local_port}",
            "https": f"socks5h://127.0.0.1:{local_port}"
        }

        for url in TEST_URLS:
            try:
                response = requests.get(
                    url,
                    proxies=proxies,
                    timeout=4,
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                )
                if response.status_code in [200, 204, 301, 302]:
                    return True
            except:
                continue
    except:
        pass
    finally:
        try:
            log_file.close()
        except:
            pass
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                proc.kill()
        for f in [temp_config_path, temp_log_path]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except:
                    pass
                
    return False

def check_node_worker(vless_uri):
    """Воркер: проверяет оригинал, потом мутирует SNI"""
    if stop_event.is_set():
        return None
    
    local_port = port_queue.get()
    result_uri = None
    
    try:
        # 1️⃣ Проверяем оригинал
        is_alive = check_single_uri(vless_uri, local_port)
        if is_alive:
            logger.debug(f"[LIVE] Исходная нода рабочая (порт {local_port})")
            result_uri = vless_uri
        
        # 2️⃣ Если оригинал мертв — мутируем SNI
        elif ("security=reality" in vless_uri.lower() or "pbk=" in vless_uri.lower()) and SNI_STATS:
            logger.debug(f"[DEAD] Исходная нода лежит. Запуск мутации SNI... (порт {local_port})")
            
            sni_list = get_sni_list(SNI_STATS, TOP_SNI_COUNT, RANDOM_SNI_COUNT)
            
            for sni in sni_list:
                mutated_uri = mutate_node_sni(vless_uri, sni)
                if check_single_uri(mutated_uri, local_port):
                    logger.info(f"[🎉 SNI WORKED] Нода ожила с SNI: {sni} (порт {local_port})")
                    
                    # +1 за успешную мутацию
                    SNI_STATS[sni] = SNI_STATS.get(sni, 0) + SNI_SUCCESS_WEIGHT
                    
                    result_uri = mutated_uri
                    break
        else:
            logger.debug(f"[SKIP] Не Reality, мутация не поддерживается")
    except Exception as e:
        logger.error(f"Ошибка в воркере: {e}")
    finally:
        port_queue.put(local_port)
        
    return result_uri

def main():
    logger.info("=== SING-BOX STATUS ===")
    try:
        sb_v = subprocess.run(["sing-box", "version"], capture_output=True, text=True)
        logger.info(sb_v.stdout.strip())
    except Exception as e:
        logger.error(f"sing-box не найден! {e}")
        return

    logger.info("\n--- STEP 1: FETCHING SOURCES ---")
    all_nodes = []
    for url in SOURCES:
        nodes = fetch_source(url)
        logger.info(f"Loaded {len(nodes)} lines from {url[:50]}...")
        all_nodes.extend(nodes)

    logger.info(f"\n--- STEP 2: VALIDATION ---")
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
    
    archive_path = os.path.join(BASE_PATH, "archive.txt")
    alive_archive_path = os.path.join(BASE_PATH, "alive_archive.txt")
    
    archive_list = []
    alive_archive_list = []
    
    if os.path.exists(archive_path):
        with open(archive_path, "r", encoding="utf-8") as f:
            archive_list = [x.strip() for x in f if x.strip()]
    
    if os.path.exists(alive_archive_path):
        with open(alive_archive_path, "r", encoding="utf-8") as f:
            alive_archive_list = [x.strip() for x in f if x.strip()]
    
    archive_seen = set(archive_list)
    for node in unique_nodes:
        if node not in archive_seen:
            archive_list.append(node)
            archive_seen.add(node)
    
    if len(archive_list) > 10000:
        archive_list = archive_list[-10000:]
    
    with open(archive_path, "w", encoding="utf-8") as f:
        f.write("\n".join(archive_list))
    
    logger.info(f"Archive updated: {len(archive_list)} nodes (limit 10000)")
    
    priority_order = []
    for node in reversed(alive_archive_list):
        if node in seen and node not in priority_order:
            priority_order.append(node)
    
    for node in unique_nodes:
        if node not in priority_order:
            priority_order.append(node)
    
    logger.info(f"\n--- STEP 3: LIVE CHECK ({len(priority_order)} nodes, threads: {MAX_THREADS}) ---")
    alive_nodes = []
    stop_event.clear()
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(check_node_worker, node) for node in priority_order]
        
        for future in as_completed(futures):
            try:
                res = future.result()
                if res:
                    alive_nodes.append(res)
                    if len(alive_nodes) >= MAX_NODES:
                        logger.info(f"✅ Собрано {MAX_NODES} нод, досрочно завершаем...")
                        stop_event.set()
                        break
            except Exception as e:
                logger.error(f"Future error: {e}")
    
    logger.info(f"\n--- RESULT: Found {len(alive_nodes)} live nodes ---")
    
    alive_nodes = list(dict.fromkeys(alive_nodes))
    logger.info(f"After dedup: {len(alive_nodes)}")
    
    if len(alive_nodes) < MAX_NODES:
        added = 0
        for node in reversed(alive_archive_list):
            if node not in alive_nodes:
                alive_nodes.append(node)
                added += 1
                if len(alive_nodes) >= MAX_NODES:
                    break
        logger.info(f"Filled from archive (+{added}), total: {len(alive_nodes)}")
    
    for node in alive_nodes:
        if node in alive_archive_list:
            alive_archive_list.remove(node)
        alive_archive_list.append(node)
    
    if len(alive_archive_list) > 5000:
        alive_archive_list = alive_archive_list[-5000:]
    
    with open(alive_archive_path, "w", encoding="utf-8") as f:
        f.write("\n".join(alive_archive_list))
    
    logger.info(f"alive_archive.txt updated: {len(alive_archive_list)} nodes (limit 5000)")
    
    if len(alive_nodes) == 0:
        logger.warning("0 live nodes, skipping update")
        save_sni_stats(SNI_STATS)
        return
    
    out_path = os.path.join(FINAL_DIR, "vless_001.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(alive_nodes[:MAX_NODES]))
    
    logger.info(f"Subscription updated: {out_path}")
    
    types_count = {"reality": 0, "tls": 0, "grpc": 0, "xhttp": 0, "ws": 0, "other": 0}
    for node in alive_nodes:
        if "security=reality" in node or "pbk=" in node:
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
    
    logger.info(f"\n--- STATS BY PROTOCOL ---")
    for proto, count in types_count.items():
        if count > 0:
            logger.info(f"{proto}: {count}")
    
    logger.info(f"\n--- TOP SNI STATS ---")
    sorted_sni = sorted(SNI_STATS.items(), key=lambda x: x[1], reverse=True)
    for sni, count in sorted_sni[:10]:
        if count > 0:
            logger.info(f"{sni}: {count} успешных мутаций")
    
    save_sni_stats(SNI_STATS)

if __name__ == "__main__":
    main()
