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
from urllib.parse import urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
os.makedirs(FINAL_DIR, exist_ok=True)

SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")

if not os.path.exists(SOURCES_FILE):
    print("Файл sources.txt не найден")
    exit(1)

with open(SOURCES_FILE, "r", encoding="utf-8") as f:
    SOURCES = [
        line.strip()
        for line in f
        if line.strip() and not line.strip().startswith("#")
    ]

MAX_NODES = 100       
MAX_THREADS = 70      

# Глобальный флаг для остановки запуска новых проверок/запросов
stop_event = threading.Event()

# Автоматически наполняем очередь портов на базе количества потоков
port_queue = queue.Queue()
for i in range(MAX_THREADS):
    port_queue.put(11000 + i)

TEST_URLS = [
    "https://vk.com",
    "https://yandex.ru",
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
        r = requests.get(url, timeout=15)
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
    except:
        pass
    return []

def is_valid_reality(line):
    if not line.lower().startswith("vless://"):
        return False
    line_lower = line.lower()
    if "pbk=" not in line_lower:
        return False
    return True

def parse_vless_reality_to_json(vless_uri, listen_port):
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
        
        pbk = params.get("pbk")
        sni = params.get("sni")
        sid = params.get("sid", "")
        flow = params.get("flow", "").lower()
        
        if not pbk or not sni:
            return None
            
        node_name = unquote(parsed.fragment) if parsed.fragment else "vless-reality"

        outbound = {
            "type": "vless",
            "tag": node_name,
            "server": server_address,
            "server_port": server_port,
            "uuid": uuid,
            "tls": {
                "enabled": True,
                "server_name": sni,
                "utls": {
                    "enabled": True,
                    "fingerprint": "chrome"
                },
                "reality": {
                    "enabled": True,
                    "public_key": pbk,
                    "short_id": sid
                }
            }
        }

        if flow and "vision" in flow:
            outbound["flow"] = flow

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
    except:
        return None

def check_node_worker(vless_uri):
    if stop_event.is_set():
        return None

    local_port = port_queue.get()
    temp_config_path = os.path.join(BASE_PATH, f"temp_{local_port}.json")
    temp_log_path = os.path.join(BASE_PATH, f"singbox_{local_port}.log")
    
    config = parse_vless_reality_to_json(vless_uri, local_port)
    if not config:
        port_queue.put(local_port)
        return None

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
        
        time.sleep(3.0)
        
        if stop_event.is_set():
            return None

        if proc.poll() is not None:
            return None

        proxies = {
            "http": f"socks5h://127.0.0.1:{local_port}",
            "https": f"socks5h://127.0.0.1:{local_port}"
        }

        for url in TEST_URLS:
            if stop_event.is_set():
                break
            try:
                response = requests.get(
                    url,
                    proxies=proxies,
                    timeout=5  
                )
                if response.status_code in [200, 204, 301, 302]:
                    if stop_event.is_set():
                        break
                    print(f"[УСПЕХ] Нода ответила через эндпоинт {url} (Порт {local_port})")
                    return vless_uri
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
        
        if os.path.exists(temp_config_path):
            os.remove(temp_config_path)
        if os.path.exists(temp_log_path):
            os.remove(temp_log_path)
            
        port_queue.put(local_port)
                
    return None

def main():
    try:
        sb_v = subprocess.run(["sing-box", "version"], capture_output=True, text=True)
        print(f"=== СИСТЕМНЫЙ СТАТУС ===\n{sb_v.stdout.strip()}\n========================")
    except Exception as e:
        print(f"Критическая ошибка: sing-box не найден! {e}")
        return

    print("--- Шаг 1: Сбор сырых данных ---")
    all_nodes = []
    all_sources = SOURCES

    for url in all_sources:
        nodes = fetch_source(url)
        print(f"Загружено {len(nodes)} строк из {url}")
        all_nodes.extend(nodes)

    print("\n--- Шаг 2: Валидация и дедупликация ---")
    unique_nodes = []
    seen = set()

    for line in all_nodes:
        line = line.strip()
        if not is_valid_reality(line):
            continue
        if line in seen:
            continue
        seen.add(line)
        unique_nodes.append(line)

    print(f"Уникальных Reality-конфигов после дедупликации: {len(unique_nodes)}")
    
    # --- Работа с Архивами ---
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

    # Обновление общего склада archive.txt
    archive_seen = set(archive_list)
    for node in unique_nodes:
        if node not in archive_seen:
            archive_list.append(node)
            archive_seen.add(node)

    if len(archive_list) > 10000:
        archive_list = archive_list[-10000:]

    with open(archive_path, "w", encoding="utf-8") as f:
        f.write("\n".join(archive_list))

    print(f"Общий архив archive.txt обновлен. Всего: {len(archive_list)} нод (Лимит 10000)")
    
    # Резерв подключается на первом этапе, если источников глобально не хватает
    if len(unique_nodes) < MAX_NODES:
        reserve_nodes = list(alive_archive_list)
        random.shuffle(reserve_nodes)

        for node in reserve_nodes:
            if node not in seen:
                unique_nodes.append(node)
                seen.add(node)

        print(f"Внимание! Предварительный резерв. Источников не хватает ({len(unique_nodes)} < {MAX_NODES}). Дозагрузили из архива.")
    
    # Чтение текущего кэша подписки для приоритезации проверки
    old_nodes = []
    out_path = os.path.join(FINAL_DIR, "vless_001.txt")
    if os.path.exists(out_path):
        with open(out_path, "r", encoding="utf-8") as f:
            old_nodes = [x.strip() for x in f if x.strip()]
    
    if not unique_nodes:
        print("Нет нод для проверки.")
        return

    random.shuffle(unique_nodes)

    priority_nodes = []
    other_nodes = []
    old_set = set(old_nodes)

    for node in unique_nodes:
        if node in old_set:
            priority_nodes.append(node)
        else:
            other_nodes.append(node)

    random.shuffle(priority_nodes)
    random.shuffle(other_nodes)
    
    unique_nodes = priority_nodes + other_nodes

    print(f"\n--- Шаг 3: Полный Live-Check ({len(unique_nodes)} нод, Потоков: {MAX_THREADS}) ---")
    alive_nodes = []
    
    stop_event.clear()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(check_node_worker, node) for node in unique_nodes]
        
        for future in as_completed(futures):
            try:
                res = future.result()
                if res:
                    alive_nodes.append(res)
                    
                    if len(alive_nodes) >= MAX_NODES:
                        print(f"Собрано {MAX_NODES} нод. Сигнализируем об экстренной остановке.")
                        stop_event.set()
                        
                        for f in futures:
                            f.cancel()
                        break
            except Exception as e:
                print(f"Ошибка фьючерса: {e}")

    print(f"\n--- Итог проверки: Найдено Реально Живых нод {len(alive_nodes)} ---")

    alive_nodes = list(dict.fromkeys(alive_nodes))
    print(f"После удаления полных дублей: {len(alive_nodes)}")

    # --- ТВОЙ ФИКС: Экстренное добивание подписки из LRU-топа, если живых нод всё ещё меньше лимита ---
    if len(alive_nodes) < MAX_NODES:
        added_count = 0
        # Читаем alive_archive с конца (самые свежие и проверенные временем ноды)
        for node in reversed(alive_archive_list):
            if node not in alive_nodes:
                alive_nodes.append(node)
                added_count += 1

            if len(alive_nodes) >= MAX_NODES:
                break
        print(f"Подписка добита свежими нодами из alive_archive.txt (+{added_count} шт.). Итоговый размер: {len(alive_nodes)}")

    # --- Честная LRU Ротация для alive_archive.txt (Лимит 5000) ---
    for node in alive_nodes:
        if node in alive_archive_list:
            alive_archive_list.remove(node)
        alive_archive_list.append(node)

    if len(alive_archive_list) > 5000:
        alive_archive_list = alive_archive_list[-5000:]

    with open(alive_archive_path, "w", encoding="utf-8") as f:
        f.write("\n".join(alive_archive_list))

    print(f"alive_archive.txt успешно обновлен. Актуальных нод в базе: {len(alive_archive_list)} (Лимит 5000)")

    if len(alive_nodes) == 0:
        print("Внимание! 0 живых нод. Перезапись отменена для защиты кэша подписок.")
        return

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(alive_nodes))
        
    print(f"Файл подписки успешно обновлен: {out_path}")

if __name__ == "__main__":
    main()
