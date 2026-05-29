#!/usr/bin/env python3
import os
import requests
import base64
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

# 🔥 ТВОИ ЖЕСТКИЕ ЛИМИТЫ ДЛЯ КАЖДОГО ПРОТОКОЛА
MAX_LINES_BY_PROTO = {
    "vless": 250,      # Строго 250 серверов VLESS (можешь поменять на 200)
    "ss": 150,         # Shadowsocks — 150
    "trojan": 150,     # Trojan — 150
    "vmess": 100       # VMess — 100
}

def decode_base64_content(text):
    """Декодирует скрытые пачки серверов из Base64"""
    text = text.strip()
    if "://" in text and not text.startswith("ey"):
        return text.splitlines()
    try:
        missing_padding = len(text) % 4
        if missing_padding:
            text += '=' * (4 - missing_padding)
        decoded = base64.b64decode(text).decode('utf-8', errors='ignore')
        return decoded.splitlines()
    except:
        return text.splitlines()

def fetch_source(url):
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            lines = r.text.splitlines()
            final_lines = []
            for line in lines:
                # Если строка огромная и без протокола — это зашифрованный пак нод
                if len(line) > 100 and "://" not in line:
                    final_lines.extend(decode_base64_content(line))
                else:
                    final_lines.append(line)
            return final_lines
    except:
        pass
    return []

def main():
    print("🚀 Старт полной распаковки и сортировки по протоколам...")
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
            # Валидация VLESS параметров, чтобы убрать пустой хлам
            if proto == "vless":
                if "pbk=" not in line and "sni=" not in line and "security=reality" not in line:
                    continue
            
            seen.add(line)
            buckets[proto].append(line)

    # Записываем файлы с ограничением по твоему словарю лимитов
    for proto in ALLOWED_PROTOCOLS:
        nodes = buckets[proto]
        
        # Берем лимит именно для этого протокола
        limit = MAX_LINES_BY_PROTO.get(proto, 250)
        nodes_limited = nodes[:limit]
        
        if proto == "vless":
            filename = "vless_001.txt"
        else:
            filename = f"{proto}_001.txt"
            
        out_path = os.path.join(FINAL_DIR, filename)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(nodes_limited))
            
        print(f"💾 {filename} — Успешно сохранено {len(nodes_limited)} нод (лимит был {limit})")

if __name__ == "__main__":
    main()
