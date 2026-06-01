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

SOURCES = [
    # Igareck
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/BLACK_SS+All_RUS.txt",

    # Kort0881
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/clean/vless.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/ru-sni/vless_ru.txt",

    # Nikita29a
    "https://github.com/nikita29a/FreeProxyList/raw/refs/heads/main/mirror/1.txt",
    "https://github.com/nikita29a/FreeProxyList/raw/refs/heads/main/mirror/26.txt",

    # Hidashimora
    "https://raw.githubusercontent.com/Hidashimora/free-vpn-anti-rkn/main/configs/27.txt",
    "https://raw.githubusercontent.com/Hidashimora/free-vpn-anti-rkn/main/configs/34.txt"
]

MAX_NODES = 100       # Сколько живых нод оставить
MAX_THREADS = 10      # Количество одновременных потоков

# Инициализируем потокобезопасную очередь портов
port_queue = queue.Queue()
for i in range(MAX_THREADS):
    port_queue.put(11000 + i)

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
            "log": {"level": "info"}, # Ставим info, чтобы видеть логи подключения в файл
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

# --- ВОРКЕР С ТОТАЛЬНЫМ ДЕБАГОМ ---

def check_node_worker(vless_uri):
    # Берем свободный порт из очереди
    local_port = port_queue.get()
    
    temp_config_path = os.path.join(BASE_PATH, f"temp_{local_port}.json")
    temp_log_path = os.path.join(BASE_PATH, f"singbox_{local_port}.log")
    
    config = parse_vless_reality_to_json(vless_uri, local_port)
    if not config:
        port_queue.put(local_port)
        return None

    # Записываем конфиг
    with open(temp_config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)

    proc = None
    try:
        # Избегаем дедлока PIPE: пишем вывод sing-box в реальный файл лога
        log_file = open(temp_log_path, "w", encoding="utf-8")
        
        print(f"[START] Запуск sing-box на порту {local_port}...")
        proc = subprocess.Popen(
            ["sing-box", "run", "-c", temp_config_path],
            stdout=log_file,
            stderr=log_file
        )
        
        # Ждем инициализации
        time.sleep(2.0)
        
        # Проверяем статус процесса
        poll_status = proc.poll()
        print(f"[STATUS] Порт {local_port} -> poll={poll_status}")
        
        if poll_status is not None:
            # Процесс упал. Читаем файл лога и выводим ошибку конфигурации
            log_file.close()
            with open(temp_log_path, "r", encoding="utf-8") as f:
                print(f"[КРАШ КОНФИГА] Лог sing-box ({local_port}):\n{f.read().strip()}")
            return None

        # Процесс живой, пробуем слать запрос
        proxies = {
            "http": f"socks5h://127.0.0.1:{local_port}",
            "https": f"socks5h://127.0.0.1:{local_port}"
        }

        print(f"[HTTP-REQ] Отправка запроса через порт {local_port}...")
        response = requests.get(
            "https://cp.cloudflare.com/generate_204",
            proxies=proxies,
            timeout=5
        )
        
        print(f"[HTTP-RESP] Порт {local_port} вернул статус: {response.status_code}")
        
        if response.status_code in [200, 204]:
            print(f"[УСПЕХ] Нода на порту {local_port} РАБОТАЕТ!")
            return vless_uri
            
    except Exception as e:
        print(f"[FAIL] Ошибка запроса на порту {local_port}: {e}")
    finally:
        # Надежное закрытие лог-файла перед удалением
        try:
            log_file.close()
        except:
            pass

        # Тушим процесс
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                proc.kill()
        
        # Чистим файлы
        if os.path.exists(temp_config_path):
            os.remove(temp_config_path)
        if os.path.exists(temp_log_path):
            os.remove(temp_log_path)
            
        # Возвращаем порт обратно в очередь для следующего потока
        port_queue.put(local_port)
                
    return None

# --- ОСНОВНОЙ ПАЙПЛАЙН ---

def main():
    # Системный чек версии sing-box прямо в питоне для логов
    try:
        sb_v = subprocess.run(["sing-box", "version"], capture_output=True, text=True)
        print(f"=== СИСТЕМНЫЙ СТАТУС ===\n{sb_v.stdout.strip()}\n========================")
    except Exception as e:
        print(f"Критическая ошибка: sing-box не найден в системе! {e}")
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

    # Выводим тестовый JSON первой ноды для проверки структуры
    print("\n=== ТЕСТОВЫЙ СИНТАКСИС JSON (ПЕРВАЯ НОДА) ===")
    test_json = parse_vless_reality_to_json(unique_nodes[0], 11000)
    print(json.dumps(test_json, indent=2))
    print("=============================================\n")

    random.shuffle(unique_nodes)

    print(f"--- Шаг 3: Настоящий Live-Check через Queue (Потоков: {MAX_THREADS}) ---")
    alive_nodes = []
    
    # Берем первые 40 нод для чистого контролируемого теста логов
    test_pool = unique_nodes[:40]
    
    # Теперь итератор as_completed обходит фиксированный список фьючерсов, 
    # который мы НЕ меняем внутри цикла. Чистая архитектура.
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(check_node_worker, node) for node in test_pool]
        
        for future in as_completed(futures):
            try:
                res = future.result()
                if res:
                    alive_nodes.append(res)
            except Exception as e:
                print(f"Ошибка выполнения фьючерса: {e}")

    print(f"\n--- Итог проверки: Найдено Живых нод {len(alive_nodes)} из {len(test_pool)} проверенных ---")

    if len(alive_nodes) == 0:
        print("Внимание! 0 живых нод. Перезапись отменена, чтобы защитить старую базу подписок.")
        return

    out_path = os.path.join(FINAL_DIR, "vless_001.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(alive_nodes))
        
    print(f"Файл подписки успешно обновлен: {out_path}")

if __name__ == "__main__":
    main()
