#!/usr/bin/env python3
import os
import subprocess
import socket
import time
import base64
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
GITHUBMIRROR_DIR = os.path.join(BASE_PATH, "githubmirror")
CLEAN_DIR = os.path.join(GITHUBMIRROR_DIR, "clean")
FINAL_DIR = os.path.join(BASE_PATH, "subs")

os.makedirs(FINAL_DIR, exist_ok=True)

# Увеличиваем таймаут, чтобы чекер не браковал нормальные сервера
CONNECT_TIMEOUT = 1.5
MAX_WORKERS = 80
MAX_VLESS_NODES = 200

def run_mirror():
    mirror_path = os.path.join(BASE_PATH, "mirror.py")
    if os.path.exists(mirror_path):
        print("Скачиваем свежие базы...")
        subprocess.run(["python3", mirror_path], cwd=BASE_PATH, capture_output=True, text=True)

def parse_vless_host_port(line: str):
    """Безопасный разбор хоста и порта без повреждения строки"""
    try:
        if not line.startswith("vless://"):
            return None, None
        # Убираем протокол
        parts = line.split("://", 1)[1]
        # Отрезаем UUID и находим то, что после знака @
        if "@" in parts:
            parts = parts.split("@", 1)[1]
        # Отрезаем параметры после знака ? или #
        main_part = parts.split("?")[0].split("#")[0]
        if ":" in main_part:
            host_parts = main_part.rsplit(":", 1)
            host = host_parts[0].strip("[]")
            port = int(host_parts[1].split("/")[0])
            return host, port
    except:
        pass
    return None, None

def check_vless_node(config: str):
    """Проверка доступности VLESS ноды по сети"""
    host, port = parse_vless_host_port(config)
    if not host or not port:
        return None
    try:
        start = time.time()
        # Разрешаем DNS имя в IP адрес
        addr = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)[0][4]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(CONNECT_TIMEOUT)
        res = s.connect_ex(addr)
        latency = (time.time() - start) * 1000
        s.close()
        if res == 0:
            return {'config': config, 'latency': latency}
    except:
        pass
    return None

def main():
    run_mirror()
    
    raw_configs = []
    seen = set()
    search_dir = CLEAN_DIR if os.path.isdir(CLEAN_DIR) else GITHUBMIRROR_DIR
    
    # Собираем исключительно VLESS строки из всех скачанных файлов
    if os.path.isdir(search_dir):
        for root, _, files in os.walk(search_dir):
            for fn in files:
                if not fn.endswith(".txt"): continue
                with open(os.path.join(root, fn), "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("vless://") and line not in seen:
                            raw_configs.append(line)
                            seen.add(line)

    print(f"Найдено {len(raw_configs)} сырых VLESS серверов. Фильтруем...")
    
    valid_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_vless_node, cfg): cfg for cfg in raw_configs}
        for future in as_completed(futures):
            res = future.result()
            if res:
                valid_results.append(res)

    # Сортируем по скорости (наименьший пинг в начале)
    valid_results.sort(key=lambda x: x['latency'])
    
    # Обрезаем строго до лимита
    final_nodes = valid_results[:MAX_VLESS_NODES]
    
    # Записываем готовый чистый файл
    out_path = os.path.join(FINAL_DIR, "vless_001.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        for node in final_nodes:
            f.write(f"{node['config']}\n")
            
    print(f"Успех! Файл vless_001.txt сохранен. Найдено живых: {len(final_nodes)}")

if __name__ == "__main__":
    main()
