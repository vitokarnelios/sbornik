#!/usr/bin/env python3
import os
import requests
import base64
import socket
import time
import random
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
os.makedirs(FINAL_DIR, exist_ok=True)

# ТВОИ 9 ИСТОЧНИКОВ (содержит и Игоряка, и kort0881, и паблик листы)
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

ALLOWED_PROTOCOLS = ["vless", "trojan", "ss", "vmess"]

# ЖЕСТКИЕ ЛИМИТЫ СТРОК ДЛЯ HIDDIFY
MAX_LINES_BY_PROTO = {
    "vless": 250,
    "trojan": 150,
    "ss": 150,
    "vmess": 100
}

CONNECT_TIMEOUT = 1.5
MAX_WORKERS = 80

def decode_base64_content(text):
    try:
        text = text.strip()
        if "://" in text:
            return [text]
        if len(text) < 100:
            return []
        padding = len(text) % 4
        if padding:
            text += "=" * (4 - padding)
        decoded = base64.b64decode(text).decode("utf-8", errors="ignore")
        if "://" not in decoded:
            return []
        return decoded.splitlines()
    except:
        return []

def fetch_source(url):
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return []
        final = []
        for line in r.text.splitlines():
            line = line.strip()
            if not line:
                continue
            if "://" in line:
                final.append(line)
            else:
                final.extend(decode_base64_content(line))
        return final
    except:
        return []

def protocol_of(line):
    for p in ALLOWED_PROTOCOLS:
        if line.startswith(f"{p}://"):
            return p
    return None

def extract_host_port(line):
    try:
        if line.startswith("vmess://"):
            encoded = line[8:]
            padding = len(encoded) % 4
            if padding:
                encoded += "=" * (4 - padding)
            data = base64.b64decode(encoded).decode("utf-8", errors="ignore")
            # Безопасный разбор JSON вместо опасного eval
            j = json.loads(data)
            return j.get("add"), int(j.get("port", 443))

        after = line.split("://", 1)[1]
        if "@" in after:
            after = after.split("@", 1)[1]
        hostpart = after.split("?")[0].split("#")[0]
        if ":" in hostpart:
            h, p = hostpart.rsplit(":", 1)
            return h.strip("[]"), int(p.split("/")[0])
        return hostpart.strip("[]"), 443
    except:
        return None, None

def check_node(config):
    proto = protocol_of(config)
    if not proto:
        return None

    if proto == "vless":
        if "pbk=" not in config and "security=reality" not in config:
            return None

    host, port = extract_host_port(config)
    if not host or not port:
        return None

    try:
        start = time.time()
        # Универсальный резолв IPv4 / IPv6
        res_info = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)[0]
        family, socktype, proto_type, _, sockaddr = res_info
        
        s = socket.socket(family, socktype, proto_type)
        s.settimeout(CONNECT_TIMEOUT)
        res = s.connect_ex(sockaddr)
        latency = (time.time() - start) * 1000
        s.close()

        if res == 0:
            return {
                "config": config,
                "protocol": proto,
                "latency": latency
            }
    except:
        pass
    return None

def main():
    print("🚀 Скачиваем и распаковываем подписки...")
    all_configs = []

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = [ex.submit(fetch_source, u) for u in SOURCES]
        for future in as_completed(futures):
            try:
                all_configs.extend(future.result())
            except:
                pass

    print(f"📦 Всего загружено строк: {len(all_configs)}")

    unique = []
    seen = set()
    for line in all_configs:
        line = line.strip()
        if not line or line in seen:
            continue
        seen.add(line)
        unique.append(line)

    print(f"🔍 Уникальных строк после очистки дублей: {len(unique)}")
    print("⚡ Запуск сетевого чекера (TCP/DNS) в 80 потоков...")
    
    valid = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(check_node, c) for c in unique]
        for future in as_completed(futures):
            try:
                r = future.result()
                if r:
                    valid.append(r)
            except:
                pass

    print(f"✅ Проверку прошли (живые порты): {len(valid)}")

    buckets = defaultdict(list)
    for item in valid:
        buckets[item["protocol"]].append(item)

    for proto in ALLOWED_PROTOCOLS:
        items = buckets[proto]
        
        # 1. Сортируем всю пачку по пингу (быстрые в начало)
        items.sort(key=lambda x: x["latency"])

        # 2. Правильное перемешивание ТОП-50 быстрых нод, чтобы ссылки были живыми
        if len(items) > 1:
            top_slice = items[:50]
            random.shuffle(top_slice)
            items = top_slice + items[50:]

        # 3. Нарезаем строго по лимиту протокола
        limit = MAX_LINES_BY_PROTO[proto]
        final = items[:limit]

        filename = "vless_001.txt" if proto == "vless" else f"{proto}_001.txt"
        out = os.path.join(FINAL_DIR, filename)

        with open(out, "w", encoding="utf-8") as f:
            for node in final:
                f.write(node["config"] + "\n")

        print(f"💾 {filename} успешно сохранен. Записано нод: {len(final)}")

    print("🔥 ВСЕ ПРОЦЕССЫ ЗАВЕРШЕНЫ УСПЕШНО")

if __name__ == "__main__":
    main()
