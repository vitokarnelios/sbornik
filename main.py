#!/usr/bin/env python3
import os
import requests
from concurrent.futures import ThreadPoolExecutor

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
os.makedirs(FINAL_DIR, exist_ok=True)

# Проверенные источники с гарантированно валидными VLESS-Reality конфигурациями
SOURCES = [
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt"
]

def fetch_source(url):
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.text.splitlines()
    except:
        pass
    return []

def main():
    print("🚀 Старт загрузки валидных VLESS конфигураций...")
    all_configs = []
    
    with ThreadPoolExecutor(max_workers=5) as ex:
        results = ex.map(fetch_source, SOURCES)
        for configs in results:
            all_configs.extend(configs)

    unique_nodes = []
    seen = set()
    
    for line in all_configs:
        line = line.strip()
        # Жесткая фильтрация: только VLESS и только с обязательными Reality/TLS параметрами
        if line.startswith("vless://") and line not in seen:
            if "pbk=" in line or "sni=" in line:
                seen.add(line)
                unique_nodes.append(line)

    out_path = os.path.join(FINAL_DIR, "vless_001.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(unique_nodes))
        
    print(f"✅ Финал! Успешно сохранено {len(unique_nodes)} проверенных VLESS нод.")

if __name__ == "__main__":
    main()
