#!/usr/bin/env python3
import os
import subprocess
import socket
import time
import base64
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
GITHUBMIRROR_DIR = os.path.join(BASE_PATH, "githubmirror")
CLEAN_DIR = os.path.join(GITHUBMIRROR_DIR, "clean")
FINAL_DIR = os.path.join(BASE_PATH, "subs")

os.makedirs(FINAL_DIR, exist_ok=True)

PROTOCOLS = ["vless", "vmess", "trojan", "ss", "hysteria", "hysteria2", "hy2", "tuic"]

CONNECT_TIMEOUT = 0.5
DNS_TIMEOUT = 1        
MAX_WORKERS = 100       

# ТВОИ ЖЕСТКИЕ ЛИМИТЫ НА КОЛИЧЕСТВО СЕРВЕРОВ
MAX_NODES_PER_PROTO = {
    "vless": 200,        # Ссылка 1 — строго до 200 нод
    "vmess": 100,        # Ссылка 2 — строго до 100 нод
    "trojan": 200,       # Ссылка 3 — строго до 200 нод
    "ss": 150,           # Ссылка 4 — строго до 150 нод
    "hysteria2": 150,
    "hy2": 100
}

def run_mirror():
    mirror_path = os.path.join(BASE_PATH, "mirror.py")
    if os.path.exists(mirror_path):
        subprocess.run(["python3", mirror_path], cwd=BASE_PATH, capture_output=True, text=True)

def protocol_of(line: str):
    for p in PROTOCOLS:
        if line.startswith(f"{p}://"): return p
    return None

def extract_host_port(line: str):
    try:
        after_proto = line.split("://", 1)[1]
        if line.startswith("vmess://"):
            try:
                encoded = after_proto.strip()
                missing_padding = len(encoded) % 4
                if missing_padding: encoded += '=' * (4 - missing_padding)
                data = json.loads(base64.urlsafe_b64decode(encoded).decode('utf-8', errors='ignore'))
                return data.get('add'), int(data.get('port', 443))
            except: return None, None
        if '@' in after_proto: after_proto = after_proto.split('@', 1)[1]
        hp = after_proto.split('?')[0].split('#')[0]
        if ':' in hp:
            parts = hp.rsplit(':', 1)
            return parts[0].strip('[]'), int(parts[1].split('/')[0])
        return hp.strip('[]'), 443
    except: return None, None

def check_node(config: str, protocol: str):
    if len(config) < 20: return None
    host, port = extract_host_port(config)
    if not host or not port: return None
    try:
        start = time.time()
        addr = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)[0][4]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(CONNECT_TIMEOUT)
        res = s.connect_ex(addr)
        latency = (time.time() - start) * 1000
        s.close()
        if res == 0:
            return {'config': config, 'protocol': protocol, 'latency': latency}
    except: pass
    return None

def main():
    print("🚀 Запуск сбора серверов через mirror.py...")
    run_mirror()
    
    raw_configs = []
    seen = set()
    search_dir = CLEAN_DIR if os.path.isdir(CLEAN_DIR) else GITHUBMIRROR_DIR
    
    if os.path.isdir(search_dir):
        for root, _, files in os.walk(search_dir):
            for fn in files:
                if not fn.endswith(".txt"): continue
                with open(os.path.join(root, fn), "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if "://" in line and line not in seen:
                            proto = protocol_of(line)
                            if proto:
                                raw_configs.append((line, proto))
                                seen.add(line)

    print(f"🔍 Найдено сырых серверов: {len(raw_configs)}. Начинаем чекать...")
    valid_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_node, c, p): p for c, p in raw_configs}
        for future in as_completed(futures):
            r = future.result()
            if r: valid_results.append(r)

    buckets = defaultdict(list)
    for r in valid_results:
        buckets[r['protocol']].append(r)

    for proto in PROTOCOLS:
        items = buckets[proto]
        items_sorted = sorted(items, key=lambda x: x['latency'])
        limit = MAX_NODES_PER_PROTO.get(proto, 100)
        items_final = items_sorted[:limit]

        filename = "vless_001.txt" if proto == "vless" else f"{proto}_001.txt"
        with open(os.path.join(FINAL_DIR, filename), "w", encoding="utf-8") as f:
            for item in items_final:
                f.write(f"{item['config']}\n")
        print(f"💾 Файл {filename} сохранен. Строго оставлено: {len(items_final)}")

if __name__ == "__main__":
    main()
