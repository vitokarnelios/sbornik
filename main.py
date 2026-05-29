#!/usr/bin/env python3
import os
import requests
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
os.makedirs(FINAL_DIR, exist_ok=True)

# ПОЛНЫЙ СПИСОК ТВОИХ ИСТОЧНИКОВ
SOURCES = [
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/clean/vless.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/ru-sni/vless_ru.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/BLACK_SS+All_RUS.txt",
    "https://github.com/nikita29a/FreeProxyList/raw/refs/heads/main/mirror/1.txt",
    "https://github.com/nikita29a/FreeProxyList/raw/refs/heads/main/mirror/26.txt",
    "https://raw.githubusercontent.com/Hidashimora/free-vpn-anti-rkn/main/configs/27.txt",
    "https://raw.githubusercontent.com/Hidashimora/free-vpn-anti-rkn/main/configs/34.txt"
]

ALLOWED_PROTOCOLS = ["vless", "ss", "trojan", "vmess"]

# ЖЕСТКИЙ ЛИМИТ: сколько максимум серверов оставлять в каждом файле
MAX_LINES = 250

def fetch_source(url):
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.text.splitlines()
    except:
        pass
    return []

def main():
    print("🚀 Старт сортировки источников с жестким лимитом строк...")
    all_configs = []
    
    with ThreadPoolExecutor(max_workers=5) as ex:
        results = ex.map(fetch_source, SOURCES)
        for configs in results:
            all_configs.extend(configs)

    buckets = defaultdict(list)
    seen = set()
    
    for line in all_configs:
        line = line.strip()
        if not line or "://" not in line or line in seen:
            continue
            
        proto = line.split("://")[0].lower()
        
        if proto in ALLOWED_PROTOCOLS:
            # Валидация VLESS, чтобы не пускать битые строки без Reality/TLS параметров
            if proto == "vless":
                if "pbk=" not in line and "sni=" not in line and "security=reality" not in line:
                    continue
            
            seen.add(line)
            buckets[proto].append(line)

    # Записываем файлы с жестким ограничением размера
    for proto in ALLOWED_PROTOCOLS:
        nodes = buckets[proto]
        
        # ОБРЕЗАЕМ СПИСОК СТРОГО ДО 250 ШТУК
        nodes_limited = nodes[:MAX_LINES]
        
        if proto == "vless":
            filename = "vless_001.txt"
        else:
            filename = f"{proto}_001.txt"
            
        out_path = os.path.join(FINAL_DIR, filename)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(nodes_limited))
            
        print(f"💾 Файл {filename} сохранен. Жестко оставлено нод: {len(nodes_limited)}")

if __name__ == "__main__":
    main()
