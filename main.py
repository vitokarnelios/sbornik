#!/usr/bin/env python3
import os
import requests
import base64
import random
import json
import subprocess
import time
import queue
from urllib.parse import urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
os.makedirs(FINAL_DIR, exist_ok=True)

SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")

with open(SOURCES_FILE, "r", encoding="utf-8") as f:
    SOURCES = [
        line.strip()
        for line in f
        if line.strip() and not line.strip().startswith("#")
    ]

# Изменено: MAX_NODES выставлен на 30
MAX_NODES = 30       
MAX_THREADS = 30      

# Автоматически наполняем очередь портов на базе количества потоков
port_queue = queue.Queue()
for i in range(MAX_THREADS):
    port_queue.put(11000 + i)

# Изменено: Список эндпоинтов откорректирован (Apple убран)
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
        
        # Изменено: Время ожидания старта sing-box увеличено до 3.0 секунд
        time.sleep(3.0)
        
        if proc.poll() is not None:
            log_file.close()
            return None

        proxies = {
            "http": f"socks5h://127.0.0.1:{local_port}",
            "https": f"socks5h://127.0.0.1:{local_port}"
        }

        for url in TEST_URLS:
            try:
                response = requests.get(
                    url,
                    proxies=proxies,
                    timeout=12  
                )
                # Изменено: Строгая проверка кодов ответа, включая редиректы
                if response.status_code in [200, 204, 301, 302]:
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
    for url in SOURCES:
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
    
    if not unique_nodes:
        print("Нет нод для проверки.")
        return

    random.shuffle(unique_nodes)

    print(f"\n--- Шаг 3: Полный Live-Check ({len(unique_nodes)} нод, Потоков: {MAX_THREADS}) ---")
    alive_nodes = []
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(check_node_worker, node) for node in unique_nodes]
        
        for future in as_completed(futures):
            try:
                res = future.result()
                if res:
                    alive_nodes.append(res)
                    if len(alive_nodes) >= MAX_NODES:
                        print(f"Собрано достаточное количество стабильных прокси ({MAX_NODES}).")
                        break
            except Exception as e:
                print(f"Ошибка фьючерса: {e}")

    print(f"\n--- Итог проверки: Найдено Реально Живых нод {len(alive_nodes)} ---")

    # Добавлено: Удаление дубликатов по домену/IP-адресу сервера
    alive_nodes_unique = []
    seen_servers = set()

    for node in alive_nodes:
        try:
            parsed = urlparse(node)
            if '@' not in parsed.netloc:
                continue

            server = parsed.netloc.split('@')[1]
            if ':' in server:
                server = server.split(':')[0]

            if server in seen_servers:
                continue

            seen_servers.add(server)
            alive_nodes_unique.append(node)
        except:
            continue

    alive_nodes = alive_nodes_unique
    print(f"После удаления дублей серверов: {len(alive_nodes)}")

    if len(alive_nodes) == 0:
        print("Внимание! 0 живых нод. Перезапись отменена для защиты кэша подписок.")
        return

    out_path = os.path.join(FINAL_DIR, "vless_001.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(alive_nodes))
        
    print(f"Файл подписки успешно обновлен: {out_path}")

if __name__ == "__main__":
    main()
