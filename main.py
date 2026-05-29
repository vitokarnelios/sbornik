#!/usr/bin/env python3

import os
import requests
import base64
import random

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")

os.makedirs(FINAL_DIR, exist_ok=True)

# =========================================================
# ИСТОЧНИКИ
# =========================================================

SOURCES = [

    # IGARECK
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/BLACK_SS+All_RUS.txt",

    # HIDDIFY
    "https://raw.githubusercontent.com/Hidashimora/free-vpn-anti-rkn/main/configs/27.txt",
    "https://raw.githubusercontent.com/Hidashimora/free-vpn-anti-rkn/main/configs/34.txt",

    # KORT0881
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/clean/vless.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/ru-sni/vless_ru.txt",

    # ТВОИ
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/vless_001.txt",
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/hysteria2_001.txt",
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/ss_001.txt",
    "https://raw.githubusercontent.com/vitokarnelios/sbornik-vless/refs/heads/main/subs/trojan_001.txt",
]

# =========================================================
# ЛИМИТЫ
# =========================================================

LIMITS = {
    "vless": 200,
    "trojan": 120,
    "ss": 120,
    "vmess": 80,
    "hysteria2": 80
}

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

        decoded = base64.b64decode(
            text
        ).decode(
            "utf-8",
            errors="ignore"
        )

        return decoded.splitlines()

    except:
        return []

# =========================================================
# ЗАГРУЗКА
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

        final = []

        for line in r.text.splitlines():

            line = line.strip()

            if not line:
                continue

            if "://" not in line:
                final.extend(
                    decode_base64_content(line)
                )
            else:
                final.append(line)

        return final

    except:
        return []

# =========================================================
# ПРОТОКОЛ
# =========================================================

def protocol_of(line):

    line = line.lower()

    for proto in [
        "vless",
        "trojan",
        "ss",
        "vmess",
        "hysteria2",
        "hy2"
    ]:

        if line.startswith(f"{proto}://"):
            return proto

    return None

# =========================================================
# VLESS FILTER
# =========================================================

def valid_vless(line):

    if not line.startswith("vless://"):
        return False

    if "pbk=" not in line:
        return False

    if "security=reality" not in line:
        return False

    return True

# =========================================================
# MAIN
# =========================================================

def main():

    print("🚀 Загрузка подписок...")

    all_lines = []

    for url in SOURCES:

        print(f"📥 {url}")

        all_lines.extend(
            fetch_source(url)
        )

    print(f"\n📦 Загружено строк: {len(all_lines)}")

    buckets = {
        "vless": [],
        "trojan": [],
        "ss": [],
        "vmess": [],
        "hysteria2": []
    }

    seen = set()

    for line in all_lines:

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

        # ФИЛЬТР REALITY
        if proto == "vless":

            if not valid_vless(line):
                continue

        seen.add(line)

        if proto == "hy2":
            proto = "hysteria2"

        if proto in buckets:
            buckets[proto].append(line)

    # =====================================================
    # СОХРАНЕНИЕ
    # =====================================================

    for proto, nodes in buckets.items():

        random.shuffle(nodes)

        limit = LIMITS.get(proto, 100)

        final_nodes = nodes[:limit]

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

            f.write(
                "\n".join(final_nodes)
            )

        print(
            f"💾 {filename}: "
            f"{len(final_nodes)} нод"
        )

    print("\n🔥 ГОТОВО")

# =========================================================

if __name__ == "__main__":
    main()
