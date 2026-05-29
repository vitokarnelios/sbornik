#!/usr/bin/env python3
import os
import requests
import base64
import random

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
os.makedirs(FINAL_DIR, exist_ok=True)

SOURCES = [
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
    "https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/ru-sni/vless_ru.txt"
]

MAX_NODES = 250

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

def main():
    all_nodes = []

    for url in SOURCES:
        all_nodes.extend(fetch_source(url))

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

    random.shuffle(unique_nodes)

    final_pack = unique_nodes[:MAX_NODES]

    out_path = os.path.join(FINAL_DIR, "vless_001.txt")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(final_pack))

if __name__ == "__main__":
    main()
