#!/usr/bin/env python3
import os
import requests
import base64
import random
import json
import subprocess
import time
from urllib.parse import urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
os.makedirs(FINAL_DIR, exist_ok=True)

SOURCES = [
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/ru-sni/vless_ru.txt"
]

MAX_NODES = 100       # Лимит живых нод для сохранения
MAX_THREADS = 10      # Размер пула потоков

# Фиксированный пул портов, чтобы не плодить тысячи сокетов в системе
AVAILABLE_PORTS = [11000 + i for i in range(MAX_THREADS)]

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

# --- ПАРСИНГ И ДИНАМИЧЕСКАЯ СБОРКА ОБЪЕКТА ---

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

        # Базовый outbound без пустых полей
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

        # Правка 1: Не пишем пустую строку во flow, чтобы свежий sing-box не падал
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

# --- ВОРКЕР С ОТЛАДКОЙ И ПЕРЕХВАТОМ ОШИБОК ---

def check_node_worker(vless_uri, local_port):
    temp_config_path = os.path.join(BASE_PATH, f"temp_{local_port}.json")
    
    config = parse_vless_reality_to_json(vless_uri, local_port)
    if not config:
        return None

    with open(temp_config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)

    proc = None
    try:
        # Правка 3: Открываем PIPE для перехвата крашей конфигурации
        proc = subprocess.Popen(
            ["sing-box", "run", "-c", temp_config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Правка 2: 2 секунды стабильного ожидания
        time.sleep(2.0)

        # Правка 2.1: Проверяем, не сдох ли процесс сразу на старте
        if proc.poll() is not None:
            err_output = proc.stderr.read()
            print(f"[КРАШ СТАРТА] Порт {local_port} упал! Лог sing-box:\n{err_output.strip()}")
            return None

        proxies = {
            "http": f"socks5h://127.0.0.1:{local_port}",
            "https": f"socks5h://127.0.0.1:{local_port}"
        }

        # Правка 5: Настоящий сквозной запрос
        response = requests.get(
            "https://cp.cloudflare.com/generate_204",
            proxies=proxies,
            timeout=5
        )
        
        if response.status_code in [200, 204]:
            print(f"[УСПЕХ] Нода на порту {local_port} СТАБИЛЬНА.")
            return vless_uri
            
    except Exception as e:
        # Сюда падают таймауты requests, когда прокси жив, но туннель заблокирован
        pass
    finally:
        # Зачистка процессов
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                proc.kill()
        
        if os.path.exists(temp_config_path):
            try:
                os.remove(temp_config_path)
            except:
                pass
                
    return None

# --- УПРАВЛЕНИЕ ОЧЕРЕДЬЮ И СТАРТ ПАЙПЛАЙНА ---

def main():
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
    random.shuffle(unique_nodes)

    print(f"\n--- Шаг 3: Настоящий Live-Check через пул портов (Потоков: {MAX_THREADS}) ---")
    alive_nodes = []
    
    # Контролируем пул портов и динамически распределяем их по as_completed
    # Ограничиваем срез для первой отладки (проверим до 150 штук, чтобы не спамить лог)
    test_pool = unique_nodes[:150]
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Словарь для отслеживания фьючерсов: {future: (node_url, assigned_port)}
        futures_map = {}
        
        # Заполняем первоначальный пул задач на доступные порты
        for i, node in enumerate(test_pool):
            if i < MAX_THREADS:
                port = AVAILABLE_PORTS[i]
                # Передаем параметры в воркер напрямую
                fut = executor.submit(check_node_worker, node, port)
                futures_map[fut] = (node, port)
            else:
                break
                
        nodes_left_idx = MAX_THREADS
        
        # По мере завершения потоков освобождаем порты и закидываем новые ноды
        for future in as_completed(futures_map):
            node_uri, used_port = futures_map[future]
            try:
                result = future.result()
                if result:
                    alive_nodes.append(result)
                    # Правка 4: Убираем преждевременный брейк на этапе отладки, 
                    # чтобы прочекать весь тестовый срез и увидеть процент выживаемости
            except Exception as e:
                print(f"Исключение в потоке выполнения: {e}")
                
            # Если в списке тест-пула еще остались непроверенные ноды — пускаем их на освободившийся порт
            if nodes_left_idx < len(test_pool):
                next_node = test_pool[nodes_left_idx]
                new_fut = executor.submit(check_node_worker, next_node, used_port)
                futures_map[new_fut] = (next_node, used_port)
                nodes_left_idx += 1

    print(f"\n--- Итог проверки: Найдено Реально Живых нод {len(alive_nodes)} из {len(test_pool)} проверенных ---")

    if len(alive_nodes) == 0:
        print("Внимание! Ошибка сети или структуры JSON: 0 живых нод. Перезапись отменена.")
        return

    out_path = os.path.join(FINAL_DIR, "vless_001.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(alive_nodes))
        
    print(f"Файл подписки успешно обновлен: {out_path}")

if __name__ == "__main__":
    main()
