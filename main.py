#!/usr/bin/env python3

import os
import json
import time
import random
import requests
import subprocess
import signal

BASE_DIR = os.path.dirname(os.path.abspath(**file**))

SUBS_DIR = os.path.join(BASE_DIR, "subs")
os.makedirs(SUBS_DIR, exist_ok=True)

SINGBOX_PATH = os.path.join(
BASE_DIR,
"sing-box-1.14.0-alpha.26-linux-amd64",
"sing-box"
)

SOURCES = [
"https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
"https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
"https://raw.githubusercontent.com/kort0881/vpn-vless-configs-russia/main/githubmirror/ru-sni/vless_ru.txt"
]

TEST_URLS = [
"https://cp.cloudflare.com",
"https://www.google.com/generate_204"
]

MAX_NODES = 70

def fetch_nodes():
all_nodes = []

```
for url in SOURCES:
    try:
        r = requests.get(url, timeout=15)

        if r.status_code == 200:
            for line in r.text.splitlines():
                line = line.strip()

                if line.startswith("vless://"):
                    if "pbk=" in line.lower():
                        all_nodes.append(line)

    except Exception as e:
        print("SOURCE ERROR:", e)

return list(set(all_nodes))
```

def make_config(node, socks_port):
return {
"log": {
"level": "error"
},
"inbounds": [
{
"type": "socks",
"tag": "socks-in",
"listen": "127.0.0.1",
"listen_port": socks_port
}
],
"outbounds": [
{
"type": "selector",
"tag": "select",
"outbounds": [
"proxy"
],
"default": "proxy"
},
{
"type": "vless",
"tag": "proxy",
"server": "PLACEHOLDER"
}
]
}

def build_temp_config(node, port):
config = {
"log": {
"level": "error"
},
"inbounds": [
{
"type": "socks",
"listen": "127.0.0.1",
"listen_port": port
}
],
"outbounds": [
{
"type": "vless",
"tag": "proxy",
"server": "127.0.0.1"
}
]
}

```
config_path = os.path.join(BASE_DIR, f"temp_{port}.json")

with open(config_path, "w", encoding="utf-8") as f:
    json.dump(config, f)

return config_path
```

def test_node(node):
port = random.randint(20000, 50000)

```
config_path = build_temp_config(node, port)

proc = None

try:
    proc = subprocess.Popen(
        [SINGBOX_PATH, "run", "-c", config_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    time.sleep(3)

    proxies = {
        "http": f"socks5h://127.0.0.1:{port}",
        "https": f"socks5h://127.0.0.1:{port}"
    }

    for url in TEST_URLS:
        r = requests.get(
            url,
            proxies=proxies,
            timeout=8
        )

        if r.status_code not in [200, 204]:
            raise Exception("Bad status")

    return True

except Exception:
    return False

finally:
    try:
        if proc:
            proc.kill()
    except:
        pass

    try:
        os.remove(config_path)
    except:
        pass
```

def main():
print("Loading Reality nodes...")

```
nodes = fetch_nodes()

print(f"Loaded: {len(nodes)}")

alive = []

for i, node in enumerate(nodes, 1):
    print(f"[{i}/{len(nodes)}] checking...")

    ok = test_node(node)

    if ok:
        print("ALIVE")
        alive.append(node)
    else:
        print("DEAD")

    if len(alive) >= MAX_NODES:
        break

random.shuffle(alive)

out_path = os.path.join(SUBS_DIR, "vless_001.txt")

with open(out_path, "w", encoding="utf-8") as f:
    f.write("\n".join(alive))

print(f"SAVED {len(alive)} LIVE NODES")
```

if **name** == "**main**":
main()
