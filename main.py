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
NODE_DB = os.path.join(BASE_PATH, "node_stats.json")
SOURCE_DB = os.path.join(BASE_PATH, "source_fails.json")

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
MAX_THREADS = 20      

stop_event = threading.Event()
port_queue = queue.Queue()
for i in range(MAX_THREADS):
    port_queue.put(11000 + i)

TEST_URLS = [
    "https://vk.com",
    "https://yandex.ru",
    "https://www.microsoft.com",
]

def load_json_db(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_json_db(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR DB] Не удалось сохранить базу {path}: {e}")

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
            return final_lines, True
    except:
        pass
    return [], False

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
        return None, False

    local_port = port_queue.get()
    temp_config_path = os.path.join(BASE_PATH, f"temp_{local_port}.json")
    temp_log_path = os.path.join(BASE_PATH, f"singbox_{local_port}.log")
    
    config = parse_vless_reality_to_json(vless_uri, local_port)
    if not config:
        port_queue.put(local_port)
        return None, False

    with open(temp_config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)

    proc = None
    is_alive = False
    log_file = None
    try:
        log_file = open(temp_log_path, "w", encoding="utf-8")
        proc = subprocess.Popen(
            ["sing-box", "run", "-c", temp_config_path],
            stdout=log_file,
            stderr=log_file
        )
        
        time.sleep(3.0)       
        
        if stop_event.is_set():
            return None, False

        if proc.poll() is not None:
            return None, False

        proxies = {
            "http": f"socks5h://127.0.0.1:{local_port}",
            "https": f"socks5h://127.0.0.1:{local_port}"
        }

        for url in TEST_URLS:
            if stop_event.is_set():
                break
            try:
                response = requests.get(url, proxies=proxies, timeout=6)
                if response.status_code in [200, 204, 301, 302]:
                    if stop_event.is_set():
                        break
                    print(f"[УСПЕХ] Нода ответила через эндпоинт {url} (Порт {local_port})")
                    is_alive = True
                    break  
            except Exception:
                continue 

    except:
        pass
    finally:
        if log_file:
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
                
    return vless_uri, is_alive

def main():
    try:
        sb_v = subprocess.run(["sing-box", "version"], capture_output=True, text=True)
        print(f"=== СИСТЕМНЫЙ СТАТУС ===\n{sb_v.stdout.strip()}\n========================")
    except Exception as e:
        print(f"Критическая ошибка: sing-box не найден! {e}")
        return

    node_db = load_json_db(NODE_DB)
    raw_source_db = load_json_db(SOURCE_DB)
    current_time = int(time.time())

    source_db = {}
    for url in SOURCES:
        if url in raw_source_db and isinstance(raw_source_db[url], dict):
            source_db[url] = raw_source_db[url]
            if "fails" not in source_db[url]: source_db[url]["fails"] = 0
            if "empty_responses" not in source_db[url]: source_db[url]["empty_responses"] = 0
            if "last_fail" not in source_db[url]: source_db[url]["last_fail"] = 0
        else:
            source_db[url] = {"fails": 0, "empty_responses": 0, "last_fail": 0}

    print("--- Шаг 1: Сбор сырых данных ---")
    all_nodes = []
    
    for url in SOURCES:
        if source_db[url]["fails"] >= 10 or source_db[url]["empty_responses"] >= 15:
            if current_time - source_db[url]["last_fail"] >= 604800:
                print(f"[РЕАНИМАЦИЯ] Прошел тайм-аут блокировки. Возвращаем источник: {url}")
                source_db[url]["fails"] = 5  
                source_db[url]["empty_responses"] = 5
            else:
                print(f"[МЁРТВЫЙ ИСТОЧНИК] Заблокирован: {url}")
                continue

        nodes, is_network_success = fetch_source(url)
        print(f"Загружено {len(nodes)} строк из {url}")
        
        if not is_network_success:
            source_db[url]["fails"] += 1
            source_db[url]["last_fail"] = current_time
        elif len(nodes) == 0:
            source_db[url]["empty_responses"] += 1
            source_db[url]["last_fail"] = current_time
        else:
            source_db[url]["fails"] = 0  
            source_db[url]["empty_responses"] = 0
            all_nodes.extend(nodes)

    save_json_db(SOURCE_DB, source_db)

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

    print(f"Уникальных Reality-конфигов из сети после дедупликации: {len(unique_nodes)}")
    
    if len(unique_nodes) < MAX_NODES:
        sorted_db_nodes = [
            k for k, v in sorted(
                node_db.items(), 
                key=lambda x: (x[1].get("score", 0), x[1].get("last_success", 0)), 
                reverse=True
            )
        ]
        
        added_from_reserve = 0
        for node in sorted_db_nodes:
            if node not in seen:
                unique_nodes.append(node)
                seen.add(node)
                added_from_reserve += 1
            if len(unique_nodes) >= MAX_NODES * 2:  
                break
        print(f"Дозагружено лучших нод из node_stats.json в пул проверки: +{added_from_reserve}")

    random.shuffle(unique_nodes)

    if not unique_nodes:
        print("Нет нод для проверки.")
        return

    print(f"\n--- Шаг 3: Полный Live-Check ({len(unique_nodes)} нод, Потоков: {MAX_THREADS}) ---")
    alive_nodes = []
    checked_results = {}

    stop_event.clear()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(check_node_worker, node) for node in unique_nodes]
        
        for future in as_completed(futures):
            try:
                node_uri, is_alive = future.result()
                if node_uri:
                    checked_results[node_uri] = is_alive
                    if is_alive:
                        alive_nodes.append(node_uri)
                        if len(alive_nodes) >= MAX_NODES:
                            print(f"Собрано лимитное количество живых нод ({MAX_NODES}). Стоп-сигнал.")
                            stop_event.set()
                            for f in futures:
                                f.cancel()
                            break
            except Exception as e:
                print(f"Ошибка фьючерса: {e}")

    print(f"\n--- Шаг 4: Обновление статистики node_stats.json ---")

    for node_uri, is_alive in checked_results.items():
        if node_uri not in node_db:
            node_db[node_uri] = {
                "success": 0, 
                "fail": 0, 
                "first_seen": current_time,
                "last_success": 0, 
                "last_checked": current_time,
                "score": 0
            }
        
        node_db[node_uri]["last_checked"] = current_time
        
        if is_alive:
            node_db[node_uri]["success"] += 1
            node_db[node_uri]["last_success"] = current_time
            if node_db[node_uri]["fail"] > 0:
                node_db[node_uri]["fail"] -= 1
        else:
            node_db[node_uri]["fail"] += 1

        last_suc = node_db[node_uri]["last_success"]
        days_old = (current_time - last_suc) / 86400.0 if last_suc > 0 else 30.0  
        
        node_db[node_uri]["score"] = node_db[node_uri]["success"] * 5 - node_db[node_uri]["fail"] * 3 - days_old

    dead_nodes = [k for k, v in node_db.items() if v.get("fail", 0) >= 10]
    for dead_node in dead_nodes:
        del node_db[dead_node]

    sorted_keys = sorted(
        node_db.keys(), 
        key=lambda x: (node_db[x].get("score", 0), node_db[x].get("last_success", 0)), 
        reverse=True
    )
    node_db = {k: node_db[k] for k in sorted_keys[:8000]}

    save_json_db(NODE_DB, node_db)
    print(f"База node_stats.json сохранена. Всего на хранении: {len(node_db)} нод.")

    print(f"\n--- Итог проверки: Найдено Реально Живых нод {len(alive_nodes)} ---")
    alive_nodes = list(dict.fromkeys(alive_nodes))

    if len(alive_nodes) < MAX_NODES:
        sorted_best_nodes = sorted(
            node_db.items(),
            key=lambda x: (x[1].get("score", 0), x[1].get("last_success", 0)),
            reverse=True
        )
        
        added_count = 0
        for node_uri, stats in sorted_best_nodes:
            if stats.get("fail", 0) >= 3:
                continue

            if node_uri not in alive_nodes:
                alive_nodes.append(node_uri)
                added_count += 1
            if len(alive_nodes) >= MAX_NODES:
                break
        print(f"Подписка добита проверенными нодами из node_stats.json (+{added_count} шт.). Итоговый размер: {len(alive_nodes)}")

    if len(alive_nodes) == 0:
        print("Внимание! 0 живых нод во всех пулах. Перезапись отменена.")
        return

    out_path = os.path.join(FINAL_DIR, "vless_001.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(alive_nodes))
        
    print(f"Файл подписки успешно обновлен: {out_path}")

if __name__ == "__main__":
    main()
