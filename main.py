#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import base64
import random
import socket
import time
import json

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")

os.makedirs(FINAL_DIR, exist_ok=True)

# =========================================================
# ИСТОЧНИКИ
# =========================================================

SOURCES = [

    # IGARECK (САМЫЕ СТАБИЛЬНЫЕ ДЛЯ БЕЛЫХ СПИСКОВ)
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/BLACK_SS+All_RUS.txt",

    # HIDDIFY / ANTI-RKN
    "https://raw.githubusercontent.com/Hidashimora/free-vpn-anti-rkn/main/configs/27.txt",
    "https://raw.githubusercontent.com/Hidashimora/free-vpn-anti-rkn/main/configs/34.txt",

    # KORT0881
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/clean/vless.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/ru-sni/vless_ru.txt",

    # ТВОИ ГОТОВЫЕ СБОРКИ
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/vless_001.txt",
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/hysteria2_001.txt",
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/ss_001.txt",
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/trojan_001.txt",

    # NIKITA29A
    "https://github.com/nikita29a/FreeProxyList/raw/refs/heads/main/mirror/1.txt",
    "https://github.com/nikita29a/FreeProxyList/raw/refs/heads/main/mirror/26.txt",
]

# =========================================================
# НАСТРОЙКИ
# =========================================================

MAX_WORKERS = 80
CONNECT_TIMEOUT = 1.5

# СКОЛЬКО МАКСИМУМ НОД ОСТАВЛЯТЬ
LIMITS = {
    "vless": 200,
    "trojan": 120,
    "ss": 120,
    "vmess": 80,
    "hysteria2": 80,
}

ALLOWED_PROTOCOLS = [
    "vless",
    "trojan",
    "ss",
    "vmess",
    "hysteria2",
    "hy2"
]

# =========================================================
# BASE64
# =========================================================

def decode_base64_content(text):
    try:
        text = text.strip()

        if "://" in text:
            return [text]

        if len(text) < 60:
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

# =========================================================
# ЗАГРУЗКА ИСТОЧНИКОВ
# =========================================================

def fetch_source(url):

    try:
        r = requests.get(
            url,
            timeout=20,
            headers={
                "User-Agent": "Mozilla/5.0"
            }
        )

        if r.status_code != 200:
            return []

        final_lines = []

        for line in r.text.splitlines():

            line = line.strip()

            if not line:
                continue

            # base64 пачки
            if "://" not in line:
                final_lines.extend(
                    decode_base64_content(line)
                )
            else:
                final_lines.append(line)

        return final_lines

    except:
        return []

# =========================================================
# ПРОТОКОЛ
# =========================================================

def protocol_of(line):

    line = line.lower()

    for p in ALLOWED_PROTOCOLS:
        if line.startswith(f"{p}://"):
            return p

    return None

# =========================================================
# ВАЛИДАЦИЯ VLESS
# =========================================================

def is_good_vless(line):

    if not line.startswith("vless://"):
        return False

    # ОБЯЗАТЕЛЬНО REALITY
    if "pbk=" not in line:
        return False

    if "security=reality" not in line:
        return False

    return True

# =========================================================
# HOST PORT
# =========================================================

def extract_host_port(line):

    try:

        # VMESS
        if line.startswith("vmess://"):

            encoded = line[8:]

            padding = len(encoded) % 4

            if padding:
                encoded += "=" * (4 - padding)

            decoded = base64.b64decode(encoded).decode(
                "utf-8",
                errors="ignore"
            )

            data = json.loads(decoded)

            host = data.get("add")
            port = int(data.get("port", 443))

            return host, port

        # остальные
        after = line.split("://", 1)[1]

        if "@" in after:
            after = after.split("@", 1)[1]

        hostpart = after.split("?")[0].split("#")[0]

        if ":" in hostpart:

            h, p = hostpart.rsplit(":", 1)

            return h.strip("[]"), int(
                p.split("/")[0]
            )

        return hostpart.strip("[]"), 443

    except:
        return None, None

# =========================================================
# TCP CHECK
# =========================================================

def check_node(config):

    proto = protocol_of(config)

    if not proto:
        return None

    # VLESS ONLY REALITY
    if proto == "vless":

        if not is_good_vless(config):
            return None

    host, port = extract_host_port(config)

    if not host or not port:
        return None

    try:

        start = time.time()

        info = socket.getaddrinfo(
            host,
            port,
            socket.AF_UNSPEC,
            socket.SOCK_STREAM
        )[0]

        family, socktype, proto_type, _, sockaddr = info

        s = socket.socket(
            family,
            socktype,
            proto_type
        )

        s.settimeout(CONNECT_TIMEOUT)

        result = s.connect_ex(sockaddr)

        latency = (time.time() - start) * 1000

        s.close()

        if result == 0:

            return {
                "config": config,
                "protocol": proto,
                "latency": latency
            }

    except:
        pass

    return None

# =========================================================
# MAIN
# =========================================================

def main():

    print("🚀 Загрузка подписок...")

    all_configs = []

    with ThreadPoolExecutor(max_workers=10) as executor:

        futures = [
            executor.submit(fetch_source, url)
            for url in SOURCES
        ]

        for future in as_completed(futures):

            try:
                all_configs.extend(
                    future.result()
                )
            except:
                pass

    print(f"📦 Загружено строк: {len(all_configs)}")

    # =====================================================
    # УДАЛЕНИЕ ДУБЛЕЙ
    # =====================================================

    unique = []
    seen = set()

    for line in all_configs:

        line = line.strip()

        if not line:
            continue

        if "://" not in line:
            continue

        if line in seen:
            continue

        proto = protocol_of(line)

        if not proto:
            continue

        seen.add(line)
        unique.append(line)

    print(f"🔍 После удаления дублей: {len(unique)}")

    # =====================================================
    # TCP CHECK
    # =====================================================

    print("⚡ Проверка серверов...")

    valid = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [
            executor.submit(check_node, cfg)
            for cfg in unique
        ]

        for future in as_completed(futures):

            try:

                result = future.result()

                if result:
                    valid.append(result)

            except:
                pass

    print(f"✅ Живых серверов: {len(valid)}")

    # =====================================================
    # СОРТИРОВКА
    # =====================================================

    buckets = defaultdict(list)

    for item in valid:
        buckets[item["protocol"]].append(item)

    # =====================================================
    # СОХРАНЕНИЕ
    # =====================================================

    for proto in ALLOWED_PROTOCOLS:

        items = buckets[proto]

        # СОРТ ПО ПИНГУ
        items.sort(
            key=lambda x: x["latency"]
        )

        # БЕРЕМ ТОП БЫСТРЫХ
        top_fast = items[:300]

        # ПЕРЕМЕШИВАЕМ
        random.shuffle(top_fast)

        limit = LIMITS.get(proto, 100)

        final_nodes = top_fast[:limit]

        if proto == "hy2":
            filename = "hysteria2_001.txt"
        else:
            filename = f"{proto}_001.txt"

        out_path = os.path.join(
            FINAL_DIR,
            filename
        )

        with open(
            out_path,
            "w",
            encoding="utf-8"
        ) as f:

            for node in final_nodes:
                f.write(
                    node["config"] + "\n"
                )

        print(
            f"💾 {filename} -> "
            f"{len(final_nodes)} серверов"
        )

    print("🔥 ГОТОВО")

# =========================================================

if __name__ == "__main__":
    main()
