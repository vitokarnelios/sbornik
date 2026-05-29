#!/usr/bin/env python3
import os
import requests
import base64
import random
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
os.makedirs(FINAL_DIR, exist_ok=True)

SOURCES = [
    # IGARECK
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/BLACK_SS+All_RUS.txt",

    # HIDASHIMORA
    "https://raw.githubusercontent.com/Hidashimora/free-vpn-anti-rkn/main/configs/27.txt",
    "https://raw.githubusercontent.com/Hidashimora/free-vpn-anti-rkn/main/configs/34.txt",

    # KORT0881
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/clean/vless.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/ru-sni/vless_ru.txt",

    # ТВОИ
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/vless_001.txt",
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/hysteria2_001.txt",
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/ss_001.txt",
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/trojan_001.txt"
]

LIMITS = {
    "vless": 200,
    "trojan": 150,
    "ss": 150,
    "vmess": 100,
    "hysteria2": 100,
    "hy2": 100
}

def decode_base64_line(text):
    try:
        text = text.strip()

        if "://" in text:
            return [text]

        if len(text) < 40:
            return []

        padding = len(text) % 4
        if padding:
            text += "=" * (4 - padding)

        decoded = base64.b64decode(text).decode(
            "utf-8",
            errors="ignore"
        )

        return decoded.splitlines()

    except:
        return []

def fetch_source(url):
    try:
        r = requests.get(url, timeout=20)

        if r.status_code != 200:
            return []

        result = []

        for line in r.text.splitlines():
            line = line.strip()

            if not line:
                continue

            if "://" in line:
                result.append(line)
            else:
                result.extend(decode_base64_line(line))

        return result

    except:
        return []

def protocol_of(line):
    try:
        return line.split("://")[0].lower()
    except:
        return None

def valid_vless(line):
    if not line.startswith("vless://"):
        return False

    # Только Reality/TLS
    if (
        "pbk=" not in line
        and "security=reality" not in line
        and "type=grpc" not in line
        and "type=ws" not in line
    ):
        return False

    return True

def main():
    print("🚀 Загружаем подписки...")

    all_lines = []

    with ThreadPoolExecutor(max_workers=10) as ex:
        results = ex.map(fetch_source, SOURCES)

        for data in results:
            all_lines.extend(data)

    print(f"📦 Загружено строк: {len(all_lines)}")

    buckets = defaultdict(list)
    seen = set()

    for line in all_lines:
        line = line.strip()

        if not line:
            continue

        if line in seen:
            continue

        if "://" not in line:
            continue

        proto = protocol_of(line)

        if not proto:
            continue

        # ФИЛЬТР VLESS
        if proto == "vless":
            if not valid_vless(line):
                continue

        seen.add(line)
        buckets[proto].append(line)

    # Перемешиваем
    for proto in buckets:
        random.shuffle(buckets[proto])

    # Сохраняем
    for proto, nodes in buckets.items():

        limit = LIMITS.get(proto, 100)

        final_nodes = nodes[:limit]

        filename = (
            "vless_001.txt"
            if proto == "vless"
            else f"{proto}_001.txt"
        )

        out = os.path.join(FINAL_DIR, filename)

        with open(out, "w", encoding="utf-8") as f:
            f.write("\n".join(final_nodes))

        print(
            f"💾 {filename} сохранен | "
            f"{len(final_nodes)} нод"
        )

    print("✅ Готово")

if __name__ == "__main__":
    main()
